"""Tests for src.benchmark."""

from __future__ import annotations

from src.benchmark import BenchmarkResult, run_all_benchmarks, run_benchmark
from src.log_generator import generate_log_batch
from src.serializer import serialize_json

# Use a small batch and few iterations so tests stay fast.
_SMALL_BATCH = 10
_FEW_ITERATIONS = 5


class TestBenchmarkResult:
    """Verify that BenchmarkResult stores the expected fields."""

    def test_fields_present(self) -> None:
        result = BenchmarkResult(
            format_name="JSON",
            operation="serialize",
            iterations=10,
            mean_ms=1.23,
            stddev_ms=0.45,
            min_ms=0.80,
            max_ms=2.00,
            total_bytes=512,
        )
        assert result.format_name == "JSON"
        assert result.operation == "serialize"
        assert result.iterations == 10
        assert result.mean_ms == 1.23
        assert result.stddev_ms == 0.45
        assert result.min_ms == 0.80
        assert result.max_ms == 2.00
        assert result.total_bytes == 512


class TestRunBenchmark:
    """Tests for the run_benchmark helper."""

    def test_returns_benchmark_result(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        result = run_benchmark(
            func=serialize_json,
            data=entries,
            iterations=_FEW_ITERATIONS,
            format_name="JSON",
            operation="serialize",
            size_bytes=100,
        )
        assert isinstance(result, BenchmarkResult)

    def test_positive_mean(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        result = run_benchmark(
            func=serialize_json,
            data=entries,
            iterations=_FEW_ITERATIONS,
            format_name="JSON",
            operation="serialize",
        )
        assert result.mean_ms > 0

    def test_mean_between_min_and_max(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        result = run_benchmark(
            func=serialize_json,
            data=entries,
            iterations=_FEW_ITERATIONS,
            format_name="JSON",
            operation="serialize",
        )
        assert result.min_ms <= result.mean_ms <= result.max_ms

    def test_iterations_count_correct(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        result = run_benchmark(
            func=serialize_json,
            data=entries,
            iterations=_FEW_ITERATIONS,
            format_name="JSON",
            operation="serialize",
        )
        assert result.iterations == _FEW_ITERATIONS

    def test_size_bytes_stored(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        result = run_benchmark(
            func=serialize_json,
            data=entries,
            iterations=_FEW_ITERATIONS,
            format_name="JSON",
            operation="serialize",
            size_bytes=999,
        )
        assert result.total_bytes == 999


class TestRunAllBenchmarks:
    """Tests for the full benchmark suite."""

    def test_returns_all_four_keys(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        results = run_all_benchmarks(entries, _FEW_ITERATIONS)
        expected_keys = {
            "json_serialize",
            "json_deserialize",
            "protobuf_serialize",
            "protobuf_deserialize",
        }
        assert set(results.keys()) == expected_keys

    def test_all_results_are_benchmark_result(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        results = run_all_benchmarks(entries, _FEW_ITERATIONS)
        for key, result in results.items():
            assert isinstance(result, BenchmarkResult), f"{key} is not a BenchmarkResult"

    def test_all_results_have_positive_mean(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        results = run_all_benchmarks(entries, _FEW_ITERATIONS)
        for key, result in results.items():
            assert result.mean_ms > 0, f"{key} has non-positive mean_ms"

    def test_size_bytes_populated(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        results = run_all_benchmarks(entries, _FEW_ITERATIONS)
        for key, result in results.items():
            assert result.total_bytes > 0, f"{key} has no size info"

    def test_protobuf_smaller_than_json(self) -> None:
        entries = generate_log_batch(_SMALL_BATCH)
        results = run_all_benchmarks(entries, _FEW_ITERATIONS)
        json_size = results["json_serialize"].total_bytes
        proto_size = results["protobuf_serialize"].total_bytes
        assert proto_size < json_size
