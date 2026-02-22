"""Benchmark serialization and deserialization for JSON vs Protocol Buffers."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import Any, Callable

from src.serializer import (
    deserialize_json,
    deserialize_protobuf,
    serialize_json,
    serialize_protobuf,
)


@dataclass
class BenchmarkResult:
    """Stores timing and size results for a single benchmark run."""

    format_name: str
    operation: str
    iterations: int
    mean_ms: float
    stddev_ms: float
    min_ms: float
    max_ms: float
    total_bytes: int


def run_benchmark(
    func: Callable[[Any], Any],
    data: Any,
    iterations: int,
    format_name: str,
    operation: str,
    size_bytes: int = 0,
) -> BenchmarkResult:
    """Run *func(data)* for *iterations* times and collect timing statistics.

    Args:
        func: The callable to benchmark (e.g., ``serialize_json``).
        data: The argument passed to *func* on every iteration.
        iterations: How many times to call *func*.
        format_name: Human-readable format label (e.g., ``"JSON"``).
        operation: Human-readable operation label (e.g., ``"serialize"``).
        size_bytes: Pre-computed serialized size in bytes (used for reporting).

    Returns:
        A :class:`BenchmarkResult` with timing statistics in milliseconds.
    """
    timings_ms: list[float] = []

    for _ in range(iterations):
        start = time.perf_counter()
        func(data)
        end = time.perf_counter()
        timings_ms.append((end - start) * 1000.0)

    mean = statistics.mean(timings_ms)
    stddev = statistics.stdev(timings_ms) if iterations > 1 else 0.0
    min_val = min(timings_ms)
    max_val = max(timings_ms)

    return BenchmarkResult(
        format_name=format_name,
        operation=operation,
        iterations=iterations,
        mean_ms=mean,
        stddev_ms=stddev,
        min_ms=min_val,
        max_ms=max_val,
        total_bytes=size_bytes,
    )


def run_all_benchmarks(entries: list[dict], iterations: int) -> dict[str, BenchmarkResult]:
    """Run the full suite of JSON and Protobuf benchmarks.

    Benchmarks performed:
    1. JSON serialize
    2. JSON deserialize
    3. Protobuf serialize
    4. Protobuf deserialize

    For deserialization benchmarks the entries are first serialized to
    produce the bytes payload that will be deserialized repeatedly.

    Args:
        entries: List of log entry dicts to benchmark against.
        iterations: Number of iterations per benchmark.

    Returns:
        A dict mapping benchmark keys to their :class:`BenchmarkResult`.
        Keys: ``json_serialize``, ``json_deserialize``,
        ``protobuf_serialize``, ``protobuf_deserialize``.
    """
    # Pre-serialize to get byte payloads and sizes
    json_bytes = serialize_json(entries)
    proto_bytes = serialize_protobuf(entries)

    json_size = len(json_bytes)
    proto_size = len(proto_bytes)

    results: dict[str, BenchmarkResult] = {}

    # JSON serialize
    results["json_serialize"] = run_benchmark(
        func=serialize_json,
        data=entries,
        iterations=iterations,
        format_name="JSON",
        operation="serialize",
        size_bytes=json_size,
    )

    # JSON deserialize
    results["json_deserialize"] = run_benchmark(
        func=deserialize_json,
        data=json_bytes,
        iterations=iterations,
        format_name="JSON",
        operation="deserialize",
        size_bytes=json_size,
    )

    # Protobuf serialize
    results["protobuf_serialize"] = run_benchmark(
        func=serialize_protobuf,
        data=entries,
        iterations=iterations,
        format_name="Protobuf",
        operation="serialize",
        size_bytes=proto_size,
    )

    # Protobuf deserialize
    results["protobuf_deserialize"] = run_benchmark(
        func=deserialize_protobuf,
        data=proto_bytes,
        iterations=iterations,
        format_name="Protobuf",
        operation="deserialize",
        size_bytes=proto_size,
    )

    return results
