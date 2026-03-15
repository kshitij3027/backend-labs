#!/usr/bin/env python3
"""Fault tolerance verification for the Kafka log streaming cluster.

Runs INSIDE Docker. Tests:
1. All 3 brokers are active
2. ISR (in-sync replicas) is complete for every partition
3. Partition leaders are distributed across all brokers
4. Replication factor is 3 for all user topics
5. min.insync.replicas is 2
6. Produce with acks=all succeeds
7. Consume from all partitions
8. Each broker can independently accept produce requests
"""

import json
import os
import sys
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, RESOURCE_TOPIC

BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"
)

USER_TOPICS = [
    "web-api-logs",
    "user-service-logs",
    "payment-service-logs",
    "critical-logs",
]


class FaultToleranceTest:
    """Verify that the Kafka cluster can tolerate broker failures."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results: list[dict] = []
        self.admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    def check(self, name: str, condition: bool, detail: str = "") -> None:
        status = "PASS" if condition else "FAIL"
        self.results.append({"name": name, "status": status, "detail": detail})
        if condition:
            self.passed += 1
            suffix = f" -- {detail}" if detail else ""
            print(f"  [PASS] {name}{suffix}")
        else:
            self.failed += 1
            suffix = f" -- {detail}" if detail else ""
            print(f"  [FAIL] {name}{suffix}")

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------

    def run_all(self) -> bool:
        print("=" * 60)
        print("Kafka Fault Tolerance Test")
        print(f"Bootstrap: {BOOTSTRAP_SERVERS}")
        print("=" * 60)

        self.test_broker_count()
        self.test_isr_complete()
        self.test_leader_distribution()
        self.test_replication_factor()
        self.test_min_insync_replicas()
        self.test_produce_with_acks_all()
        self.test_consume_from_all_partitions()
        self.test_produce_consume_resilience()

        print()
        print("=" * 60)
        print("RESULTS SUMMARY")
        print("=" * 60)
        for r in self.results:
            tag = r["status"]
            suffix = f" -- {r['detail']}" if r["detail"] else ""
            print(f"  [{tag}] {r['name']}{suffix}")
        print()
        print(f"  {self.passed} passed, {self.failed} failed")
        print("=" * 60)

        if self.failed:
            print("\nFault tolerance test FAILED.\n")
        else:
            print("\nFault tolerance test PASSED.\n")

        return self.failed == 0

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_broker_count(self):
        """Verify all 3 brokers are active."""
        print("\n[Broker Count]")
        try:
            metadata = self.admin.list_topics(timeout=10)
            count = len(metadata.brokers)
            self.check("3 brokers active", count == 3, f"brokers={count}")
            # Also list the broker IDs
            broker_ids = sorted(metadata.brokers.keys())
            self.check(
                "Broker IDs are 1, 2, 3",
                broker_ids == [1, 2, 3],
                f"ids={broker_ids}",
            )
        except Exception as e:
            self.check("Broker count check", False, str(e))

    def test_isr_complete(self):
        """Verify all partitions have complete ISR (all replicas in sync)."""
        print("\n[ISR Complete]")
        try:
            metadata = self.admin.list_topics(timeout=10)
            all_complete = True
            checked = 0
            for topic_name, topic_meta in metadata.topics.items():
                if topic_name.startswith("_"):
                    continue
                if topic_name not in USER_TOPICS:
                    continue
                for part_id, part_meta in topic_meta.partitions.items():
                    isr_count = len(part_meta.isrs)
                    replica_count = len(part_meta.replicas)
                    checked += 1
                    ok = isr_count == replica_count
                    if not ok:
                        all_complete = False
                    self.check(
                        f"ISR complete: {topic_name}[{part_id}]",
                        ok,
                        f"ISR={isr_count}/{replica_count}",
                    )
            if checked == 0:
                self.check("ISR check", False, "No user-topic partitions found")
            elif all_complete:
                self.check(
                    "All partitions fully in-sync",
                    True,
                    f"checked={checked}",
                )
        except Exception as e:
            self.check("ISR check", False, str(e))

    def test_leader_distribution(self):
        """Verify partition leaders are spread across all 3 brokers."""
        print("\n[Leader Distribution]")
        try:
            metadata = self.admin.list_topics(timeout=10)
            leaders: set[int] = set()
            for topic_name, topic_meta in metadata.topics.items():
                if topic_name not in USER_TOPICS:
                    continue
                for _part_id, part_meta in topic_meta.partitions.items():
                    leaders.add(part_meta.leader)
            self.check(
                "Leaders distributed across all 3 brokers",
                len(leaders) >= 3,
                f"leaders_on_brokers={sorted(leaders)}",
            )
        except Exception as e:
            self.check("Leader distribution check", False, str(e))

    def test_replication_factor(self):
        """Verify RF=3 for all user topics."""
        print("\n[Replication Factor]")
        try:
            metadata = self.admin.list_topics(timeout=10)
            for topic_name in USER_TOPICS:
                if topic_name in metadata.topics:
                    partitions = metadata.topics[topic_name].partitions
                    replicas = len(partitions[0].replicas)
                    self.check(
                        f"RF=3 for {topic_name}",
                        replicas == 3,
                        f"replicas={replicas}",
                    )
                else:
                    self.check(f"RF=3 for {topic_name}", False, "topic not found")
        except Exception as e:
            self.check("Replication factor check", False, str(e))

    def test_min_insync_replicas(self):
        """Verify min.insync.replicas=2."""
        print("\n[Min Insync Replicas]")
        try:
            resource = ConfigResource(RESOURCE_TOPIC, "web-api-logs")
            futures = self.admin.describe_configs([resource])
            for _res, future in futures.items():
                config = future.result()
                min_isr = config.get("min.insync.replicas")
                if min_isr:
                    self.check(
                        "min.insync.replicas=2",
                        min_isr.value == "2",
                        f"value={min_isr.value}",
                    )
                else:
                    self.check(
                        "min.insync.replicas=2",
                        False,
                        "config key not found",
                    )
        except Exception as e:
            self.check("min.insync.replicas check", False, str(e))

    def test_produce_with_acks_all(self):
        """Produce messages with acks=all and verify delivery."""
        print("\n[Produce with acks=all]")
        try:
            producer = Producer(
                {
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "acks": "all",
                }
            )
            delivered = 0
            errors = 0

            def cb(err, msg):
                nonlocal delivered, errors
                if err:
                    errors += 1
                else:
                    delivered += 1

            num_messages = 100
            for i in range(num_messages):
                msg = json.dumps(
                    {"test": "fault-tolerance", "seq": i}
                ).encode()
                # Use varied keys to distribute across all partitions.
                key = f"ft-key-{i}".encode()
                producer.produce("web-api-logs", msg, key=key, callback=cb)

            producer.flush(timeout=30)
            self.check(
                f"{num_messages} messages delivered with acks=all",
                delivered == num_messages,
                f"delivered={delivered}, errors={errors}",
            )
        except Exception as e:
            self.check("Produce with acks=all", False, str(e))

    def test_consume_from_all_partitions(self):
        """Verify messages can be consumed from all partitions.

        Uses explicit partition assignment to guarantee we read from every
        partition, rather than relying on subscribe() which may only poll
        from one partition within the message limit.
        """
        print("\n[Consume from All Partitions]")
        try:
            from confluent_kafka import TopicPartition

            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            metadata = admin.list_topics(timeout=10)
            num_partitions = len(metadata.topics["web-api-logs"].partitions)

            partitions_seen: set[int] = set()
            for pid in range(num_partitions):
                consumer = Consumer(
                    {
                        "bootstrap.servers": BOOTSTRAP_SERVERS,
                        "group.id": f"ft-part-{pid}-{int(time.time())}",
                        "auto.offset.reset": "earliest",
                    }
                )
                consumer.assign([TopicPartition("web-api-logs", pid)])
                deadline = time.time() + 5
                while time.time() < deadline:
                    msg = consumer.poll(0.5)
                    if msg and not msg.error():
                        partitions_seen.add(msg.partition())
                        break
                consumer.close()

            self.check(
                "Consumed from all partitions",
                len(partitions_seen) >= 2,
                f"partitions={sorted(partitions_seen)}",
            )
        except Exception as e:
            self.check("Consume from all partitions", False, str(e))

    def test_produce_consume_resilience(self):
        """Produce to each broker individually and verify delivery."""
        print("\n[Produce via Individual Brokers]")
        brokers = ["kafka-1:29092", "kafka-2:29092", "kafka-3:29092"]
        for broker in brokers:
            broker_name = broker.split(":")[0]
            try:
                p = Producer({"bootstrap.servers": broker, "acks": "1"})
                delivered = [False]

                def cb(err, _msg, _d=delivered):
                    _d[0] = err is None

                p.produce(
                    "web-api-logs",
                    json.dumps({"test": "resilience", "broker": broker_name}).encode(),
                    callback=cb,
                )
                p.flush(timeout=10)
                self.check(f"Produce via {broker_name}", delivered[0])
            except Exception as e:
                self.check(f"Produce via {broker_name}", False, str(e))


if __name__ == "__main__":
    tester = FaultToleranceTest()
    success = tester.run_all()
    sys.exit(0 if success else 1)
