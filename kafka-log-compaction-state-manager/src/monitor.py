"""Monitors topic offsets, compaction metrics, and topic configuration."""

import logging
import time
import threading

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource

from src.config import Settings

logger = logging.getLogger(__name__)


class CompactionMonitor:
    """Collects compaction-related metrics from Kafka and the state consumer.

    Uses a dedicated lightweight consumer for watermark queries and the
    ``AdminClient`` for topic configuration introspection.
    """

    def __init__(self, config: "Settings", consumer: "StateConsumer") -> None:  # noqa: F821
        self.config = config
        self.consumer = consumer
        self.admin = AdminClient(
            {"bootstrap.servers": config.active_bootstrap_servers}
        )
        self._consumer_for_watermarks = Consumer(
            {
                "bootstrap.servers": config.active_bootstrap_servers,
                "group.id": f"{config.consumer_group_id}-monitor",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        self.messages_per_second: float = 0.0
        self._last_count: int = 0
        self._last_time: float = time.time()
        self._lock = threading.Lock()

        logger.info("CompactionMonitor initialized.")

    # ------------------------------------------------------------------
    # Topic offsets
    # ------------------------------------------------------------------

    def get_topic_offsets(self) -> dict:
        """Query low/high watermark offsets for partition 0 of the topic.

        Returns:
            dict with keys ``low``, ``high``, and ``total_messages``.
        """
        tp = TopicPartition(self.config.topic_name, 0)
        low, high = self._consumer_for_watermarks.get_watermark_offsets(tp)
        return {"low": low, "high": high, "total_messages": high - low}

    # ------------------------------------------------------------------
    # Compaction metrics
    # ------------------------------------------------------------------

    def get_compaction_metrics(self) -> dict:
        """Build a combined metrics dictionary.

        Includes watermark offsets, consumer state statistics, a compaction
        ratio estimate, and throughput measurement.
        """
        offsets = self.get_topic_offsets()
        stats = self.consumer.get_stats()

        total_messages = offsets["total_messages"]
        unique_keys = stats["total_in_state"]
        compaction_ratio = unique_keys / max(total_messages, 1)
        estimated_storage_saved_bytes = (
            self.consumer.total_messages_consumed - unique_keys
        ) * 200  # average message size estimate

        # Throughput calculation
        with self._lock:
            now = time.time()
            elapsed = now - self._last_time
            if elapsed > 0:
                self.messages_per_second = (
                    stats["total_consumed"] - self._last_count
                ) / elapsed
            self._last_count = stats["total_consumed"]
            self._last_time = now

        return {
            "offsets": offsets,
            "consumer_stats": stats,
            "total_messages": total_messages,
            "unique_keys": unique_keys,
            "compaction_ratio": compaction_ratio,
            "estimated_storage_saved_bytes": estimated_storage_saved_bytes,
            "messages_per_second": self.messages_per_second,
        }

    # ------------------------------------------------------------------
    # Topic configuration
    # ------------------------------------------------------------------

    def get_topic_config(self) -> dict:
        """Fetch compaction-relevant topic configuration entries.

        Returns:
            dict with keys ``cleanup.policy``, ``segment.bytes``,
            ``delete.retention.ms``, and ``min.cleanable.dirty.ratio``.
        """
        resource = ConfigResource("TOPIC", self.config.topic_name)
        futures = self.admin.describe_configs([resource])

        result: dict = {}
        for res, future in futures.items():
            configs = future.result()
            for key in (
                "cleanup.policy",
                "segment.bytes",
                "delete.retention.ms",
                "min.cleanable.dirty.ratio",
            ):
                if key in configs:
                    result[key] = configs[key].value

        return result
