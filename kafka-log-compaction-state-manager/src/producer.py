"""Kafka producer that generates user profile state updates for log compaction demos."""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from faker import Faker

from src.config import Settings
from src.models import StateUpdate, UpdateType, UserProfile

logger = logging.getLogger(__name__)


class ProfileProducer:
    """Produces user profile CREATE/UPDATE/DELETE events to a compacted Kafka topic."""

    def __init__(self, config: Settings) -> None:
        self.config = config
        self.producer = Producer(
            {
                "bootstrap.servers": config.active_bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "max.in.flight.requests.per.connection": 5,
            }
        )
        self.faker = Faker()
        self.profiles: dict[str, UserProfile] = {}
        self.deleted_ids: set[str] = set()
        self.stats = {"created": 0, "updated": 0, "deleted": 0, "total_produced": 0}
        logger.info("ProfileProducer initialized (broker=%s)", config.active_bootstrap_servers)

    # ------------------------------------------------------------------
    # Delivery callback
    # ------------------------------------------------------------------

    def _delivery_callback(self, err, msg) -> None:
        """Log delivery success or failure."""
        if err is not None:
            logger.error("Message delivery failed: %s (topic=%s)", err, msg.topic())
        else:
            logger.debug(
                "Message delivered: topic=%s partition=%d offset=%d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    # ------------------------------------------------------------------
    # Profile generation helpers
    # ------------------------------------------------------------------

    def _generate_profile(self, user_id: str) -> UserProfile:
        """Create a new UserProfile with random faker data."""
        return UserProfile(
            user_id=user_id,
            email=self.faker.email(),
            first_name=self.faker.first_name(),
            last_name=self.faker.last_name(),
            age=random.randint(18, 80),
            version=1,
            last_updated=datetime.now(timezone.utc).isoformat(),
        )

    # ------------------------------------------------------------------
    # Produce
    # ------------------------------------------------------------------

    def produce_event(self, state_update: StateUpdate) -> None:
        """Produce a state update event to Kafka.

        DELETE events are produced as tombstones (value=None) so that log
        compaction eventually removes the key entirely.
        """
        key = f"profile:{state_update.user_id}".encode("utf-8")

        if state_update.update_type == UpdateType.DELETE:
            value = None  # tombstone
        else:
            value = state_update.to_kafka_value()

        self.producer.produce(
            topic=self.config.topic_name,
            key=key,
            value=value,
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

        self.stats["total_produced"] += 1
        if state_update.update_type == UpdateType.CREATE:
            self.stats["created"] += 1
        elif state_update.update_type == UpdateType.UPDATE:
            self.stats["updated"] += 1
        elif state_update.update_type == UpdateType.DELETE:
            self.stats["deleted"] += 1

        logger.info(
            "[%s] user_id=%s  (total=%d)",
            state_update.update_type.value,
            state_update.user_id,
            self.stats["total_produced"],
        )

    # ------------------------------------------------------------------
    # Event builders
    # ------------------------------------------------------------------

    def _create_user(self) -> StateUpdate:
        """Generate a CREATE event for a brand-new user."""
        user_id = f"user_{uuid.uuid4().hex[:8]}"
        profile = self._generate_profile(user_id)
        self.profiles[user_id] = profile
        return StateUpdate(
            user_id=user_id,
            update_type=UpdateType.CREATE,
            profile=profile,
        )

    def _update_user(self) -> Optional[StateUpdate]:
        """Generate an UPDATE event for a random active profile.

        Returns None when there are no active profiles to update.
        """
        if not self.profiles:
            return None

        user_id = random.choice(list(self.profiles.keys()))
        profile = self.profiles[user_id]

        # Pick 1-2 fields to modify
        fields_to_change = random.sample(
            ["email", "first_name", "last_name", "age"],
            k=random.randint(1, 2),
        )
        updates: dict = {}
        for field in fields_to_change:
            if field == "email":
                updates["email"] = self.faker.email()
            elif field == "first_name":
                updates["first_name"] = self.faker.first_name()
            elif field == "last_name":
                updates["last_name"] = self.faker.last_name()
            elif field == "age":
                updates["age"] = random.randint(18, 80)

        updated_profile = profile.model_copy(
            update={
                **updates,
                "version": profile.version + 1,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
        )
        self.profiles[user_id] = updated_profile

        return StateUpdate(
            user_id=user_id,
            update_type=UpdateType.UPDATE,
            profile=updated_profile,
        )

    def _delete_user(self) -> Optional[StateUpdate]:
        """Generate a DELETE (tombstone) event for a random active profile.

        Returns None when there are no active profiles to delete.
        """
        if not self.profiles:
            return None

        user_id = random.choice(list(self.profiles.keys()))
        del self.profiles[user_id]
        self.deleted_ids.add(user_id)

        return StateUpdate(
            user_id=user_id,
            update_type=UpdateType.DELETE,
            profile=None,
        )

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(self, duration_seconds: int = 60) -> None:
        """Run the producer for the given duration.

        Phase 1: Seed the topic with ``config.num_users`` initial profiles.
        Phase 2: Randomly create/update/delete profiles until the duration
        elapses, with a 70/20/10 weighting for update/create/delete.
        """
        logger.info("Phase 1: creating %d initial users …", self.config.num_users)
        for _ in range(self.config.num_users):
            event = self._create_user()
            self.produce_event(event)

        self.producer.flush()
        logger.info("Phase 1 complete — %d users seeded.", len(self.profiles))

        logger.info("Phase 2: random mutations for %ds …", duration_seconds)
        start = time.time()
        event_count = 0

        while time.time() - start < duration_seconds:
            roll = random.random()
            event: Optional[StateUpdate] = None

            if roll < 0.7:
                event = self._update_user()
            elif roll < 0.9:
                event = self._create_user()
            else:
                event = self._delete_user()

            if event is not None:
                self.produce_event(event)
                event_count += 1

            if event_count % 10 == 0:
                self.producer.flush()

            time.sleep(self.config.update_interval_seconds)

        self.producer.flush()
        logger.info(
            "Producer finished — stats: %s  active_profiles=%d  deleted=%d",
            self.stats,
            len(self.profiles),
            len(self.deleted_ids),
        )


if __name__ == "__main__":
    from src.config import load_config

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    config = load_config()
    producer = ProfileProducer(config)
    producer.run(60)
