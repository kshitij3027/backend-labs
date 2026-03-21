"""Kafka consumer that rebuilds and maintains user profile state from a compacted topic."""

import logging
import threading
import time
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, TopicPartition

from src.config import Settings
from src.models import StateUpdate, UpdateType, UserProfile

logger = logging.getLogger(__name__)


class StateConsumer:
    """Consumes user profile events and maintains an in-memory state store.

    Supports two modes:
    - **rebuild_state**: assigns to a partition at the beginning and reads all
      existing messages to reconstruct the full state table.
    - **consume_loop**: subscribes to the topic and processes new events in
      real time.
    """

    def __init__(self, config: Settings) -> None:
        self.config = config
        self.consumer = Consumer(
            {
                "bootstrap.servers": config.active_bootstrap_servers,
                "group.id": config.consumer_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        # In-memory state store keyed by user_id
        self.state: dict[str, UserProfile] = {}

        # Metrics
        self.total_messages_consumed: int = 0
        self.tombstones_processed: int = 0
        self.updates_by_type: dict[str, int] = {"CREATE": 0, "UPDATE": 0, "DELETE": 0}

        # Control
        self._running: bool = False
        self._lock = threading.Lock()
        self.on_state_change: Optional[Callable] = None

        logger.info(
            "StateConsumer initialized (broker=%s, group=%s)",
            config.active_bootstrap_servers,
            config.consumer_group_id,
        )

    # ------------------------------------------------------------------
    # Message processing
    # ------------------------------------------------------------------

    def _process_message(self, key_bytes: bytes, value_bytes: Optional[bytes]) -> None:
        """Process a single Kafka message, updating the in-memory state.

        Args:
            key_bytes: The raw message key (expected format ``profile:<user_id>``).
            value_bytes: The raw message value, or *None* for a Kafka tombstone.
        """
        user_id = key_bytes.decode("utf-8").replace("profile:", "", 1)

        with self._lock:
            if value_bytes is None:
                # Kafka tombstone — remove the profile entirely
                if user_id in self.state:
                    del self.state[user_id]
                self.tombstones_processed += 1
                self.updates_by_type["DELETE"] += 1
            else:
                state_update = StateUpdate.from_kafka_value(value_bytes)

                if state_update.update_type == UpdateType.DELETE:
                    # Explicit DELETE event (non-tombstone)
                    if user_id in self.state:
                        del self.state[user_id]
                    self.tombstones_processed += 1
                    self.updates_by_type["DELETE"] += 1
                else:
                    # CREATE or UPDATE — upsert the profile
                    if state_update.profile is not None:
                        self.state[user_id] = state_update.profile
                    self.updates_by_type[state_update.update_type.value] += 1

            self.total_messages_consumed += 1

            if self.on_state_change is not None:
                self.on_state_change()

    # ------------------------------------------------------------------
    # State rebuild (from beginning of topic)
    # ------------------------------------------------------------------

    def rebuild_state(self) -> None:
        """Read the entire topic from the beginning to rebuild the state table.

        Uses manual partition assignment (``assign``) rather than group-based
        subscription so that every message is read regardless of committed
        offsets.  After the rebuild the assignment is cleared so the consumer
        can later be used with ``subscribe()`` for live consumption.
        """
        tp = TopicPartition(self.config.topic_name, 0, 0)
        self.consumer.assign([tp])
        self.consumer.seek(tp)

        consecutive_none = 0
        logger.info("Rebuilding state from topic '%s' …", self.config.topic_name)

        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                consecutive_none += 1
                if consecutive_none >= 2:
                    break
                continue

            consecutive_none = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition during rebuild.")
                    break
                logger.error("Consumer error during rebuild: %s", msg.error())
                continue

            self._process_message(msg.key(), msg.value())

        # Unassign so the consumer can be reused with subscribe()
        self.consumer.unsubscribe()

        logger.info(
            "State rebuild complete — consumed=%d, active_profiles=%d, tombstones=%d",
            self.total_messages_consumed,
            len(self.get_active_profiles()),
            self.tombstones_processed,
        )

    # ------------------------------------------------------------------
    # Live consumption loop
    # ------------------------------------------------------------------

    def consume_loop(self) -> None:
        """Subscribe to the topic and process new messages until ``stop()`` is called."""
        self._running = True
        self.consumer.subscribe([self.config.topic_name])
        logger.info("Live consumption started on topic '%s'.", self.config.topic_name)

        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error: %s", msg.error())
                    continue

                self._process_message(msg.key(), msg.value())
        finally:
            self.consumer.close()
            logger.info("Consumer closed.")

    def stop(self) -> None:
        """Signal the consume loop to exit."""
        self._running = False

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_active_profiles(self) -> dict[str, UserProfile]:
        """Return a copy of all non-deleted profiles in the state store."""
        with self._lock:
            return {
                uid: profile
                for uid, profile in self.state.items()
                if not profile.deleted
            }

    def get_stats(self) -> dict:
        """Return a snapshot of consumer metrics."""
        with self._lock:
            return {
                "total_consumed": self.total_messages_consumed,
                "active_profiles": sum(
                    1 for p in self.state.values() if not p.deleted
                ),
                "total_in_state": len(self.state),
                "tombstones_processed": self.tombstones_processed,
                "updates_by_type": dict(self.updates_by_type),
            }
