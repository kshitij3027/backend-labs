"""Unit tests for src.metrics — counters, bounded windows, thread safety.

Everything here is deterministic, including the threaded test: durations are
binary-exact floats (quarters and halves are powers of two, so their sums
come out identical in every interleaving), and the nearest-rank percentile
convention has exactly one right answer per input. No sleeps, no network,
no tolerance bands.
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

from src.metrics import (
    LATENCY_WINDOW_SIZE,
    FilterMetrics,
    MetricsRegistry,
    percentile_nearest_rank,
)

#: The exact key set every snapshot must expose (C8's /stats relies on it).
SNAPSHOT_KEYS = {
    "adds_total",
    "queries_total",
    "positives",
    "negatives",
    "observed_false_positives",
    "observed_fp_rate",
    "avg_add_ms",
    "p50_add_ms",
    "p99_add_ms",
    "avg_query_ms",
    "p50_query_ms",
    "p99_query_ms",
}


class TestPercentileConvention:
    """Pin the nearest-rank definition: ``sorted[ceil(P/100 · N) − 1]``."""

    def test_empty_window_reads_zero(self) -> None:
        assert percentile_nearest_rank([], 50.0) == 0.0
        assert percentile_nearest_rank([], 99.0) == 0.0

    def test_single_sample_is_every_percentile(self) -> None:
        assert percentile_nearest_rank([7.5], 50.0) == 7.5
        assert percentile_nearest_rank([7.5], 99.0) == 7.5

    def test_p50_of_1_to_100_is_50(self) -> None:
        """ceil(0.50 · 100) = rank 50 → the 50th smallest: 50.0, not 50.5.

        Interpolated conventions would say 50.5 here; nearest-rank always
        returns an actually-observed sample.
        """
        values = [float(i) for i in range(1, 101)]
        assert percentile_nearest_rank(values, 50.0) == 50.0

    def test_p99_of_1_to_100_is_99(self) -> None:
        """ceil(0.99 · 100) = rank 99 → 99.0; only p100 reads the max."""
        values = [float(i) for i in range(1, 101)]
        assert percentile_nearest_rank(values, 99.0) == 99.0
        assert percentile_nearest_rank(values, 100.0) == 100.0

    def test_small_window_ranks(self) -> None:
        """N=4: p50 → rank ceil(2.0)=2 → 2.0; p99 → rank ceil(3.96)=4 → 4.0."""
        values = [1.0, 2.0, 3.0, 4.0]
        assert percentile_nearest_rank(values, 50.0) == 2.0
        assert percentile_nearest_rank(values, 99.0) == 4.0


class TestCounters:
    """Operation counters and the observed-FP ratio."""

    def test_mixed_adds_and_queries_count_exactly(self) -> None:
        metrics = FilterMetrics()
        for _ in range(7):
            metrics.record_add(0.25)
        for _ in range(5):
            metrics.record_query(0.5, positive=True)
        for _ in range(8):
            metrics.record_query(0.5, positive=False)
        metrics.record_false_positive()
        metrics.record_false_positive()

        snap = metrics.snapshot()
        assert snap["adds_total"] == 7
        assert snap["queries_total"] == 13
        assert snap["positives"] == 5
        assert snap["negatives"] == 8
        assert snap["positives"] + snap["negatives"] == snap["queries_total"]
        assert snap["observed_false_positives"] == 2
        # 2 disproved out of 5 bloom positives.
        assert snap["observed_fp_rate"] == 2 / 5

    def test_observed_fp_rate_zero_when_no_positives(self) -> None:
        """Negatives only → denominator clamps to 1 → 0.0, never a crash."""
        metrics = FilterMetrics()
        for _ in range(4):
            metrics.record_query(0.5, positive=False)
        snap = metrics.snapshot()
        assert snap["positives"] == 0
        assert snap["observed_fp_rate"] == 0.0


class TestLatency:
    """avg / p50 / p99 per operation kind, tracked independently."""

    def test_add_durations_1_to_100(self) -> None:
        """Known series: avg is the mean, percentiles follow nearest-rank."""
        metrics = FilterMetrics()
        for ms in range(1, 101):
            metrics.record_add(float(ms))
        snap = metrics.snapshot()
        assert snap["avg_add_ms"] == 50.5
        assert snap["p50_add_ms"] == 50.0  # nearest-rank, not interpolated 50.5
        assert snap["p99_add_ms"] == 99.0  # rank 99 of 100, not the max
        # The query side is untouched by add traffic.
        assert snap["avg_query_ms"] == 0.0
        assert snap["p50_query_ms"] == 0.0
        assert snap["p99_query_ms"] == 0.0

    def test_query_durations_independent_of_adds(self) -> None:
        metrics = FilterMetrics()
        for ms in range(101, 201):
            metrics.record_query(float(ms), positive=(ms % 2 == 0))
        for _ in range(10):
            metrics.record_add(1.0)
        snap = metrics.snapshot()
        assert snap["avg_query_ms"] == 150.5
        assert snap["p50_query_ms"] == 150.0
        assert snap["p99_query_ms"] == 199.0
        assert snap["avg_add_ms"] == 1.0
        assert snap["p50_add_ms"] == 1.0
        assert snap["p99_add_ms"] == 1.0

    def test_ms_values_round_to_4_decimals(self) -> None:
        metrics = FilterMetrics()
        metrics.record_add(0.123456789)
        snap = metrics.snapshot()
        assert snap["avg_add_ms"] == 0.1235
        assert snap["p50_add_ms"] == 0.1235
        assert snap["p99_add_ms"] == 0.1235


class TestWindowBound:
    """The deque cap: fixed memory, percentiles see only recent samples."""

    def test_window_capped_at_1000_after_5000_samples(self) -> None:
        metrics = FilterMetrics()
        for _ in range(4_000):
            metrics.record_add(1000.0)  # ancient, slow samples
        for _ in range(1_000):
            metrics.record_add(1.0)  # the most recent 1000, all fast
        # The bound itself: 5000 appends, exactly 1000 retained.
        assert len(metrics._add_window) == LATENCY_WINDOW_SIZE == 1_000

        snap = metrics.snapshot()
        # Percentiles describe only the surviving window — every 1000.0
        # sample has been evicted, so even p99 reads the fast value.
        assert snap["p50_add_ms"] == 1.0
        assert snap["p99_add_ms"] == 1.0
        # The average is lifetime (running sum), so it still remembers all
        # 5000 samples: (4000·1000 + 1000·1) / 5000 = 800.2.
        assert snap["avg_add_ms"] == 800.2
        assert snap["adds_total"] == 5_000


class TestEmptySnapshot:
    """A fresh ledger reads all-zero with no division blowups."""

    def test_fresh_metrics_snapshot_is_all_zero(self) -> None:
        snap = FilterMetrics().snapshot()
        assert set(snap) == SNAPSHOT_KEYS
        assert all(value == 0 for value in snap.values())

    def test_fresh_snapshot_value_types(self) -> None:
        """Counters stay ints; every derived/ms value is a float."""
        snap = FilterMetrics().snapshot()
        for key in (
            "adds_total",
            "queries_total",
            "positives",
            "negatives",
            "observed_false_positives",
        ):
            assert isinstance(snap[key], int), key
        for key in (
            "observed_fp_rate",
            "avg_add_ms",
            "p50_add_ms",
            "p99_add_ms",
            "avg_query_ms",
            "p50_query_ms",
            "p99_query_ms",
        ):
            assert isinstance(snap[key], float), key


class TestRegistry:
    """Auto-creating, instance-stable name → metrics map."""

    def test_get_auto_creates(self) -> None:
        registry = MetricsRegistry()
        assert isinstance(registry.get("error_logs"), FilterMetrics)

    def test_same_name_returns_same_instance(self) -> None:
        registry = MetricsRegistry()
        assert registry.get("error_logs") is registry.get("error_logs")

    def test_different_names_return_different_instances(self) -> None:
        registry = MetricsRegistry()
        assert registry.get("error_logs") is not registry.get("access_logs")

    def test_snapshot_covers_every_known_name(self) -> None:
        registry = MetricsRegistry()
        registry.get("error_logs").record_add(0.25)
        registry.get("access_logs").record_query(0.5, positive=True)
        registry.get("security_logs")  # touched but never recorded into
        snap = registry.snapshot()
        assert set(snap) == {"error_logs", "access_logs", "security_logs"}
        assert snap["error_logs"]["adds_total"] == 1
        assert snap["access_logs"]["positives"] == 1
        assert set(snap["security_logs"]) == SNAPSHOT_KEYS

    def test_empty_registry_snapshot_is_empty_dict(self) -> None:
        assert MetricsRegistry().snapshot() == {}


class TestThreadSafety:
    """Writers from many threads, zero lost updates.

    This mirrors production: the event loop and AnyIO threadpool workers
    both record into the same ledger. Durations are powers of two so float
    accumulation is exact in any interleaving — the averages come out
    exactly right if and only if no update was dropped.
    """

    def test_8_threads_x_1000_mixed_records_land_exactly(self) -> None:
        metrics = FilterMetrics()

        def hammer() -> None:
            for i in range(1_000):
                if i % 2 == 0:
                    metrics.record_add(0.25)
                else:
                    metrics.record_query(0.5, positive=(i % 4 == 1))
                    if i % 100 == 1:
                        metrics.record_false_positive()

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(hammer) for _ in range(8)]
            for future in futures:
                future.result()  # re-raises any worker exception

        snap = metrics.snapshot()
        # Per thread: 500 adds + 500 queries (250 positive / 250 negative)
        # and 10 observed FPs (i ∈ {1, 101, ..., 901}), times 8 threads.
        assert snap["adds_total"] == 4_000
        assert snap["queries_total"] == 4_000
        assert snap["adds_total"] + snap["queries_total"] == 8_000
        assert snap["positives"] == 2_000
        assert snap["negatives"] == 2_000
        assert snap["observed_false_positives"] == 80
        assert snap["observed_fp_rate"] == 80 / 2_000
        # Exact averages prove the float accumulators lost nothing either.
        assert snap["avg_add_ms"] == 0.25
        assert snap["avg_query_ms"] == 0.5

    def test_concurrent_get_yields_a_single_instance(self) -> None:
        """64 racing get() calls on one name must all share one ledger."""
        registry = MetricsRegistry()
        with ThreadPoolExecutor(max_workers=8) as pool:
            instances = list(pool.map(lambda _: registry.get("hot"), range(64)))
        assert all(instance is instances[0] for instance in instances)


class TestSerialization:
    """Snapshots are plain JSON-ready dicts — exactly what /stats returns."""

    def test_filter_snapshot_json_roundtrip(self) -> None:
        metrics = FilterMetrics()
        metrics.record_add(0.25)
        metrics.record_query(1.5, positive=True)
        metrics.record_false_positive()
        snap = metrics.snapshot()
        assert json.loads(json.dumps(snap)) == snap

    def test_registry_snapshot_json_roundtrip(self) -> None:
        registry = MetricsRegistry()
        registry.get("error_logs").record_add(0.25)
        registry.get("sessions").record_query(0.5, positive=False)
        snap = registry.snapshot()
        assert json.loads(json.dumps(snap)) == snap
        assert all(
            type(value) in (int, float)
            for filter_snap in snap.values()
            for value in filter_snap.values()
        )
