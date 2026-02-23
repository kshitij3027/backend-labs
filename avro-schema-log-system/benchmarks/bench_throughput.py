"""Throughput benchmark for Avro serialization and deserialization."""

import sys
import time

from src.log_event import LogEvent
from src.schema_registry import SchemaRegistry
from src.serializer import AvroSerializer
from src.deserializer import AvroDeserializer

NUM_EVENTS = 10_000
MIN_THROUGHPUT = 3_000  # events/sec
MAX_LATENCY_MS = 1.0  # milliseconds


def run_benchmark():
    """Run serialize/deserialize benchmarks for all schema versions."""
    registry = SchemaRegistry()
    serializer = AvroSerializer(registry)
    deserializer = AvroDeserializer(registry)

    versions = registry.list_versions()
    results: dict[str, dict] = {}

    print("=== Avro Serialization Benchmark ===")

    for version in versions:
        # Generate sample events
        events = [LogEvent.generate_sample(version) for _ in range(NUM_EVENTS)]
        event_dicts = [e.to_dict(version) for e in events]

        # --- Serialize ---
        start = time.perf_counter()
        serialized = [serializer.serialize(d, version) for d in event_dicts]
        ser_elapsed = time.perf_counter() - start

        ser_throughput = NUM_EVENTS / ser_elapsed
        ser_latency_ms = (ser_elapsed / NUM_EVENTS) * 1000

        # --- Deserialize ---
        start = time.perf_counter()
        for blob in serialized:
            deserializer.deserialize(blob, version)
        deser_elapsed = time.perf_counter() - start

        deser_throughput = NUM_EVENTS / deser_elapsed
        deser_latency_ms = (deser_elapsed / NUM_EVENTS) * 1000

        # --- Size ---
        avg_size = sum(len(b) for b in serialized) / len(serialized)

        results[version] = {
            "ser_throughput": ser_throughput,
            "ser_latency_ms": ser_latency_ms,
            "deser_throughput": deser_throughput,
            "deser_latency_ms": deser_latency_ms,
            "avg_size": avg_size,
        }

        print(f"Schema Version: {version}")
        print(f"  Serialize:   {ser_throughput:,.0f} events/sec ({ser_latency_ms:.3f}ms avg latency)")
        print(f"  Deserialize: {deser_throughput:,.0f} events/sec ({deser_latency_ms:.3f}ms avg latency)")
        print(f"  Avg size:    {avg_size:.0f} bytes")
        print()

    # --- Size ordering check ---
    sizes = {v: r["avg_size"] for v, r in results.items()}
    size_order_correct = sizes["v1"] < sizes["v2"] < sizes["v3"]

    print("=== Size Ordering ===")
    size_parts = [f"{v} ({sizes[v]:.0f} bytes)" for v in versions]
    symbol = "\u2713" if size_order_correct else "\u2717"
    print(f"{' < '.join(size_parts)} {symbol}")
    print()

    # --- Threshold checks ---
    all_throughput_ok = all(
        r["ser_throughput"] > MIN_THROUGHPUT and r["deser_throughput"] > MIN_THROUGHPUT
        for r in results.values()
    )
    all_latency_ok = all(
        r["ser_latency_ms"] < MAX_LATENCY_MS and r["deser_latency_ms"] < MAX_LATENCY_MS
        for r in results.values()
    )

    print("=== Threshold Check ===")
    print(f"All versions > {MIN_THROUGHPUT:,} events/sec: {'PASS' if all_throughput_ok else 'FAIL'}")
    print(f"All latencies < {MAX_LATENCY_MS}ms: {'PASS' if all_latency_ok else 'FAIL'}")
    print(f"Size ordering correct: {'PASS' if size_order_correct else 'FAIL'}")

    if all_throughput_ok and all_latency_ok and size_order_correct:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    run_benchmark()
