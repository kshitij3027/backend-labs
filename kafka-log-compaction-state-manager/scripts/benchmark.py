"""Performance benchmarks for the Kafka log compaction state manager."""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# Add project root to path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_config, Settings
from src.models import UserProfile, StateUpdate, UpdateType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Benchmark:
    """Performance benchmarks for Kafka log compaction."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bootstrap = settings.active_bootstrap_servers
        self.topic = settings.topic_name

    def _make_producer(self) -> Producer:
        return Producer({
            "bootstrap.servers": self.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "linger.ms": 5,
            "batch.num.messages": 1000,
        })

    def _make_consumer(self, group_id: str | None = None) -> Consumer:
        gid = group_id or f"bench-{uuid.uuid4().hex[:12]}"
        return Consumer({
            "bootstrap.servers": self.bootstrap,
            "group.id": gid,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def _make_profile_bytes(self, user_id: str, version: int = 1) -> bytes:
        """Build a serialized StateUpdate for a CREATE/UPDATE event."""
        profile = UserProfile(
            user_id=user_id,
            email=f"{user_id}@bench.test",
            first_name="Bench",
            last_name=f"User{version}",
            age=25,
            version=version,
            last_updated=datetime.now(timezone.utc).isoformat(),
        )
        update = StateUpdate(
            user_id=user_id,
            update_type=UpdateType.CREATE if version == 1 else UpdateType.UPDATE,
            profile=profile,
        )
        return update.to_kafka_value()

    # ------------------------------------------------------------------
    # Benchmark 1: Throughput
    # ------------------------------------------------------------------

    def test_throughput(self) -> dict:
        """Measure raw message production throughput."""
        logger.info("=== Benchmark 1: Throughput Test ===")
        total = 2100
        producer = self._make_producer()

        # Pre-build all messages
        messages = []
        for i in range(total):
            uid = f"user_bench_{i:04d}"
            key = f"profile:{uid}".encode("utf-8")
            value = self._make_profile_bytes(uid, version=1)
            messages.append((key, value))

        start = time.monotonic()
        for idx, (key, value) in enumerate(messages):
            producer.produce(self.topic, key=key, value=value)
            if idx % 500 == 0:
                producer.poll(0)

        producer.flush(timeout=30)
        elapsed = time.monotonic() - start

        msg_per_second = total / elapsed if elapsed > 0 else 0
        passed = msg_per_second >= 350

        result = {
            "total": total,
            "elapsed_seconds": round(elapsed, 3),
            "msg_per_second": round(msg_per_second, 1),
            "passed": passed,
        }
        logger.info("Throughput: %d msgs in %.3fs = %.1f msg/s (target: 350+) %s",
                     total, elapsed, msg_per_second, "PASS" if passed else "FAIL")
        return result

    # ------------------------------------------------------------------
    # Benchmark 2: State Rebuild
    # ------------------------------------------------------------------

    def test_state_rebuild(self, num_profiles: int = 1000) -> dict:
        """Measure time to produce profiles and rebuild state from scratch."""
        logger.info("=== Benchmark 2: State Rebuild Test ===")

        # Produce unique profiles
        producer = self._make_producer()
        for i in range(num_profiles):
            uid = f"user_rebuild_{i:04d}"
            key = f"profile:{uid}".encode("utf-8")
            value = self._make_profile_bytes(uid, version=1)
            producer.produce(self.topic, key=key, value=value)
            if i % 500 == 0:
                producer.poll(0)
        producer.flush(timeout=30)
        logger.info("Produced %d unique profiles for rebuild test.", num_profiles)

        # Give Kafka a moment to settle
        time.sleep(2)

        # Create a fresh consumer and rebuild state
        consumer = self._make_consumer()
        tp = TopicPartition(self.topic, 0, 0)
        consumer.assign([tp])
        consumer.seek(tp)

        state: dict[str, dict] = {}
        start = time.monotonic()
        consecutive_none = 0

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                consecutive_none += 1
                if consecutive_none >= 3:
                    break
                continue

            consecutive_none = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.warning("Consumer error: %s", msg.error())
                continue

            key = msg.key()
            value = msg.value()
            if key is not None:
                key_str = key.decode("utf-8")
                if value is None:
                    state.pop(key_str, None)
                else:
                    state[key_str] = True  # Just track keys for speed

        elapsed = time.monotonic() - start
        consumer.close()

        profiles_rebuilt = len(state)
        passed = elapsed < 10.0

        result = {
            "num_profiles": num_profiles,
            "rebuild_seconds": round(elapsed, 3),
            "profiles_rebuilt": profiles_rebuilt,
            "passed": passed,
        }
        logger.info("Rebuild: %d profiles in %.3fs (target: <10s) %s",
                     profiles_rebuilt, elapsed, "PASS" if passed else "FAIL")
        return result

    # ------------------------------------------------------------------
    # Benchmark 3: Compaction Effectiveness
    # ------------------------------------------------------------------

    def test_compaction_effectiveness(self, num_users: int = 50, updates_per_user: int = 100) -> dict:
        """Measure how well compaction reduces duplicate keys.

        Uses a dedicated topic to isolate compaction measurement from
        messages produced by other benchmarks. The metric is message
        reduction: (1 - total_consumed / total_produced) * 100, which
        shows how many messages compaction actually removed.
        """
        logger.info("=== Benchmark 3: Compaction Effectiveness Test ===")

        # Create a dedicated topic for the compaction test so results
        # are not polluted by messages from benchmarks 1 & 2.
        compact_topic = "benchmark-compaction-test"
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        new_topic = NewTopic(
            compact_topic,
            num_partitions=1,
            replication_factor=1,
            config={
                "cleanup.policy": "compact",
                "segment.bytes": "524288",          # 512KB — smaller segments roll faster
                "min.cleanable.dirty.ratio": "0.01", # very aggressive cleaning
                "delete.retention.ms": "1000",
                "max.compaction.lag.ms": "5000",
                "segment.ms": "5000",               # force segment roll every 5s
                "min.compaction.lag.ms": "0",
            },
        )
        fs = admin.create_topics([new_topic])
        for topic, future in fs.items():
            try:
                future.result()
                logger.info("Created dedicated compaction topic: %s", topic)
            except Exception as exc:
                # Topic may already exist from a previous run
                logger.info("Topic %s already exists or error: %s", topic, exc)

        # Give the topic a moment to become available
        time.sleep(2)

        total_produced = num_users * updates_per_user
        producer = self._make_producer()

        # Produce many updates per user to the dedicated topic
        for i in range(num_users):
            uid = f"user_compact_{i:04d}"
            key = f"profile:{uid}".encode("utf-8")
            for v in range(1, updates_per_user + 1):
                value = self._make_profile_bytes(uid, version=v)
                producer.produce(compact_topic, key=key, value=value)
            if i % 10 == 0:
                producer.poll(0)

        producer.flush(timeout=30)
        logger.info("Produced %d messages (%d users x %d updates). Waiting for compaction...",
                     total_produced, num_users, updates_per_user)

        # Wait for compaction to kick in.
        # With segment.bytes=512KB, segment.ms=5s, and aggressive dirty ratio,
        # compaction should trigger well within this window.
        time.sleep(60)

        # Consume everything and count total messages remaining after compaction
        consumer = self._make_consumer()
        tp = TopicPartition(compact_topic, 0, 0)
        consumer.assign([tp])
        consumer.seek(tp)

        all_keys: set[str] = set()
        total_consumed = 0
        consecutive_none = 0

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                consecutive_none += 1
                if consecutive_none >= 3:
                    break
                continue

            consecutive_none = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                logger.warning("Consumer error: %s", msg.error())
                continue

            total_consumed += 1
            if msg.key() is not None:
                all_keys.add(msg.key().decode("utf-8"))

        consumer.close()

        unique_keys = len(all_keys)
        # Measure actual compaction: how many messages were removed compared
        # to total produced. E.g. produced 5000, consumed 100 → 98% reduction.
        reduction_percent = (1 - total_consumed / total_produced) * 100 if total_produced > 0 else 0
        passed = reduction_percent > 40  # 40% threshold is realistic for Docker

        result = {
            "total_produced": total_produced,
            "total_in_topic": total_consumed,
            "unique_keys": unique_keys,
            "reduction_percent": round(reduction_percent, 1),
            "passed": passed,
        }
        logger.info(
            "Compaction: produced %d, consumed %d, %d unique keys, %.1f%% reduction (target: >40%%) %s",
            total_produced, total_consumed, unique_keys, reduction_percent, "PASS" if passed else "FAIL",
        )
        return result


def main() -> None:
    print("=" * 60)
    print("  Kafka Log Compaction - Performance Benchmarks")
    print("=" * 60)
    print()

    settings = load_config()
    bench = Benchmark(settings)

    results = {}

    results["throughput"] = bench.test_throughput()
    print()

    results["state_rebuild"] = bench.test_state_rebuild(num_profiles=1000)
    print()

    results["compaction_effectiveness"] = bench.test_compaction_effectiveness(
        num_users=50, updates_per_user=100,
    )
    print()

    # Summary
    print("=" * 60)
    print("  BENCHMARK SUMMARY")
    print("=" * 60)

    all_passed = True
    for name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        all_passed = all_passed and result["passed"]
        print(f"  [{status}] {name}")
        for k, v in result.items():
            if k != "passed":
                print(f"         {k}: {v}")
        print()

    total = len(results)
    passed = sum(1 for r in results.values() if r["passed"])
    print(f"  Result: {passed}/{total} benchmarks passed")
    print("=" * 60)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
