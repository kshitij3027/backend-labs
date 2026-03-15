#!/usr/bin/env python3
"""Throughput benchmark for the Kafka Log Streaming Cluster.

Runs INSIDE Docker. Targets:
  - Producer: >= 1 000 msg/sec sustained
  - Latency:  P50 < 10 ms (produce-to-consume)
"""

import json
import os
import sys
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"
)
BENCHMARK_TOPIC = "web-api-logs"
NUM_MESSAGES = 10_000
MESSAGE_SIZE = 256  # approximate payload bytes


class Benchmark:
    """Run throughput and latency benchmarks and print a report."""

    def __init__(self):
        self.results: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------

    def run_all(self) -> bool:
        print("=" * 60)
        print("Kafka Throughput Benchmark")
        print(f"Bootstrap: {BOOTSTRAP_SERVERS}")
        print(f"Messages:  {NUM_MESSAGES}  |  ~Size: {MESSAGE_SIZE} bytes")
        print("=" * 60)

        self.benchmark_producer()
        self.benchmark_consumer()
        self.benchmark_latency()
        self.print_summary()

        return self.results.get("producer_mps", 0) >= 1000

    # ------------------------------------------------------------------
    # Producer benchmark
    # ------------------------------------------------------------------

    def benchmark_producer(self):
        """Produce NUM_MESSAGES and measure throughput."""
        print("\n--- Producer Benchmark ---")

        producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "batch.size": 200000,
                "linger.ms": 50,
                "compression.type": "lz4",
                "acks": "1",
            }
        )

        # Build a realistic JSON payload close to MESSAGE_SIZE bytes.
        padding_len = max(0, MESSAGE_SIZE - 150)
        payload = json.dumps(
            {
                "timestamp": "2026-03-15T12:00:00+00:00",
                "service": "web-api",
                "level": "INFO",
                "endpoint": "/benchmark",
                "status_code": 200,
                "user_id": "benchmark-user",
                "message": "x" * padding_len,
                "sequence_number": 0,
            }
        ).encode()

        delivered = 0
        failed = 0

        def cb(err, _msg):
            nonlocal delivered, failed
            if err:
                failed += 1
            else:
                delivered += 1

        start = time.perf_counter()
        for i in range(NUM_MESSAGES):
            producer.produce(
                BENCHMARK_TOPIC,
                payload,
                f"bench-{i % 50}".encode(),
                callback=cb,
            )
            # Drain delivery reports periodically to avoid buffer-full errors.
            if i % 1000 == 0:
                producer.poll(0)

        producer.flush(timeout=60)
        elapsed = time.perf_counter() - start

        mps = delivered / elapsed if elapsed > 0 else 0
        mbps = (delivered * len(payload)) / (elapsed * 1024 * 1024) if elapsed > 0 else 0

        print(f"  Produced:   {NUM_MESSAGES}")
        print(f"  Delivered:  {delivered}")
        print(f"  Failed:     {failed}")
        print(f"  Elapsed:    {elapsed:.2f}s")
        print(f"  Throughput: {mps:.0f} msg/sec")
        print(f"  Bandwidth:  {mbps:.2f} MB/sec")

        self.results["producer_mps"] = mps
        self.results["producer_mbps"] = mbps

        target_met = mps >= 1000
        print(f"  Target (>= 1000 msg/sec): {'PASS' if target_met else 'FAIL'}")

    # ------------------------------------------------------------------
    # Consumer benchmark
    # ------------------------------------------------------------------

    def benchmark_consumer(self):
        """Consume messages and measure throughput."""
        print("\n--- Consumer Benchmark ---")

        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "group.id": f"benchmark-consumer-{int(time.time())}",
                "auto.offset.reset": "earliest",
                "fetch.min.bytes": 1024,
                "fetch.wait.max.ms": 100,
            }
        )
        consumer.subscribe([BENCHMARK_TOPIC])

        count = 0
        target = min(NUM_MESSAGES, 5000)
        start = time.perf_counter()
        deadline = start + 30  # Hard cap at 30 seconds.

        while count < target and time.perf_counter() < deadline:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                count += 1

        elapsed = time.perf_counter() - start
        consumer.close()

        mps = count / elapsed if elapsed > 0 else 0
        print(f"  Consumed:   {count}")
        print(f"  Elapsed:    {elapsed:.2f}s")
        print(f"  Throughput: {mps:.0f} msg/sec")

        self.results["consumer_mps"] = mps

    # ------------------------------------------------------------------
    # Latency benchmark
    # ------------------------------------------------------------------

    def benchmark_latency(self):
        """Measure produce-to-consume round-trip latency."""
        print("\n--- Latency Benchmark ---")

        latency_topic = "critical-logs"  # Single partition for clean measurement.
        num_samples = 100

        producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "acks": "1",
                "linger.ms": 0,  # No batching -- minimise send delay.
            }
        )

        consumer = Consumer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "group.id": f"latency-bench-{int(time.time())}",
                "auto.offset.reset": "latest",
            }
        )
        consumer.subscribe([latency_topic])
        # Warm up the consumer (trigger rebalance / partition assignment).
        consumer.poll(3.0)

        latencies: list[float] = []
        for i in range(num_samples):
            send_time = time.perf_counter()
            msg = json.dumps(
                {"bench": True, "send_time": send_time, "seq": i}
            ).encode()
            producer.produce(latency_topic, msg)
            producer.flush(timeout=5)

            recv_msg = consumer.poll(5.0)
            if recv_msg and not recv_msg.error():
                recv_time = time.perf_counter()
                latency_ms = (recv_time - send_time) * 1000
                latencies.append(latency_ms)

        consumer.close()

        if latencies:
            latencies.sort()
            n = len(latencies)
            p50 = latencies[int(n * 0.5)]
            p95 = latencies[int(n * 0.95)]
            p99 = latencies[min(int(n * 0.99), n - 1)]

            print(f"  Samples: {n}")
            print(f"  P50:     {p50:.1f}ms")
            print(f"  P95:     {p95:.1f}ms")
            print(f"  P99:     {p99:.1f}ms")
            print(f"  Sub-10ms P50: {'PASS' if p50 < 10 else 'FAIL'}")

            self.results["latency_p50"] = p50
            self.results["latency_p95"] = p95
            self.results["latency_p99"] = p99
        else:
            print("  No latency samples collected")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def print_summary(self):
        print()
        print("=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)
        for key, value in self.results.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")

        producer_pass = self.results.get("producer_mps", 0) >= 1000
        print(
            f"\n  Producer >= 1000 msg/sec: {'PASS' if producer_pass else 'FAIL'}"
        )
        print("=" * 60)


if __name__ == "__main__":
    bench = Benchmark()
    success = bench.run_all()
    sys.exit(0 if success else 1)
