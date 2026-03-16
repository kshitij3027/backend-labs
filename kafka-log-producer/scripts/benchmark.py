"""Throughput benchmark for the Kafka Log Producer."""

import os
import sys
import time

from confluent_kafka import Producer

# Allow running from project root with PYTHONPATH=/app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.log_generator import LogGenerator
from src.models import LogEntry


class Benchmark:
    """Produce messages directly to Kafka and measure throughput."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "1",
                "batch.size": 65536,
                "linger.ms": 10,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 100000,
            }
        )
        self.generator = LogGenerator()

    # ------------------------------------------------------------------
    # Throughput test
    # ------------------------------------------------------------------

    def run_throughput_test(self, count: int = 10000) -> dict:
        """Produce *count* messages as fast as possible and measure throughput."""
        sent = 0
        failed = 0

        def _on_delivery(err, msg):
            nonlocal sent, failed
            if err:
                failed += 1
            else:
                sent += 1

        entries = self.generator.generate_batch(count)

        start = time.time()
        for entry in entries:
            topic = entry.route_topic()
            key = entry.to_kafka_key()
            value = entry.to_kafka_value()
            try:
                self.producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=value.encode("utf-8"),
                    callback=_on_delivery,
                )
            except BufferError:
                self.producer.poll(1.0)
                self.producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=value.encode("utf-8"),
                    callback=_on_delivery,
                )
            # Service callbacks without blocking
            self.producer.poll(0)

        self.producer.flush(30)
        elapsed = time.time() - start

        return {
            "count": count,
            "sent": sent,
            "failed": failed,
            "duration": round(elapsed, 3),
            "throughput": round(count / elapsed, 1) if elapsed > 0 else 0,
        }

    # ------------------------------------------------------------------
    # Sustained rate test
    # ------------------------------------------------------------------

    def run_sustained_test(
        self, duration: int = 60, target_rate: int = 1000
    ) -> dict:
        """Send at *target_rate* msg/s for *duration* seconds."""
        total_sent = 0
        total_failed = 0

        def _on_delivery(err, msg):
            nonlocal total_sent, total_failed
            if err:
                total_failed += 1
            else:
                total_sent += 1

        start = time.time()
        while time.time() - start < duration:
            batch_start = time.time()
            entries = self.generator.generate_batch(target_rate)

            for entry in entries:
                topic = entry.route_topic()
                key = entry.to_kafka_key()
                value = entry.to_kafka_value()
                try:
                    self.producer.produce(
                        topic=topic,
                        key=key.encode("utf-8"),
                        value=value.encode("utf-8"),
                        callback=_on_delivery,
                    )
                except BufferError:
                    self.producer.poll(1.0)
                    self.producer.produce(
                        topic=topic,
                        key=key.encode("utf-8"),
                        value=value.encode("utf-8"),
                        callback=_on_delivery,
                    )
                self.producer.poll(0)

            # Pace to ~1-second batches
            batch_elapsed = time.time() - batch_start
            if batch_elapsed < 1.0:
                time.sleep(1.0 - batch_elapsed)

            elapsed = time.time() - start
            rate = (total_sent + total_failed) / elapsed if elapsed > 0 else 0
            print(
                f"  [{elapsed:.0f}s] produced={total_sent + total_failed} "
                f"rate={rate:.0f}/s",
                end="\r",
            )

        self.producer.flush(30)
        actual_duration = time.time() - start

        return {
            "duration": round(actual_duration, 1),
            "total_sent": total_sent,
            "total_failed": total_failed,
            "throughput": round(total_sent / actual_duration, 1) if actual_duration > 0 else 0,
            "target_rate": target_rate,
            "zero_failures": total_failed == 0,
        }

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    @staticmethod
    def report(label: str, results: dict) -> int:
        """Print a formatted results table and return exit code."""
        print(f"\n{'=' * 60}")
        print(f"  {label}")
        print(f"{'=' * 60}")
        for key, value in results.items():
            print(f"  {key:>20s}: {value}")
        print(f"{'=' * 60}")

        throughput = results.get("throughput", 0)
        zero_failures = results.get("zero_failures", results.get("failed", 1) == 0)
        passed = throughput >= 1000 and zero_failures
        status = "PASS" if passed else "FAIL"
        print(f"  Result: {status}")
        return 0 if passed else 1


if __name__ == "__main__":
    bench = Benchmark()

    print("\n--- Throughput Test (10,000 messages) ---")
    throughput_results = bench.run_throughput_test(10000)
    code1 = Benchmark.report("Throughput Test", throughput_results)

    print("\n--- Sustained Rate Test (60s @ 1000 msg/s) ---")
    sustained_results = bench.run_sustained_test(duration=60, target_rate=1000)
    code2 = Benchmark.report("Sustained Rate Test", sustained_results)

    sys.exit(max(code1, code2))
