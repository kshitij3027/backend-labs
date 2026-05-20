"""Unit tests for the C6 stats subsystem (throughput, latency, counters).

Three classes mirroring the three stats components:

* :class:`TestThroughput` — sliding-window ops-per-second counter,
  including the sliding behavior under a monkeypatched clock.
* :class:`TestLatency` — reservoir-sampled latency histogram, including
  percentile correctness on a known distribution and the reservoir
  bound on a high-volume stream.
* :class:`TestCounters` — pattern hit counters, including thread
  safety under 10×100 concurrent increments.

Each test uses small, deterministic inputs so failures are easy to
attribute. The throughput tests monkeypatch ``ThroughputCounter._now_sec``
to advance time without touching ``time.time`` globally.
"""
from __future__ import annotations

import threading

import pytest

from src.stats.counters import PatternCounters
from src.stats.latency import LatencyHistogram
from src.stats.throughput import ThroughputCounter


# ---------------------------------------------------------------------------
# TestThroughput — sliding-window ops-per-second counter
# ---------------------------------------------------------------------------


class TestThroughput:
    """Behavior tests for :class:`ThroughputCounter`."""

    def test_record_increments_total_count(self) -> None:
        """``total_count`` reflects every ``record()`` call."""
        # A 60-second window holds at most 60 buckets, so 50 increments
        # at the same second collapse into a single bucket with
        # count=50 — total_count still returns 50.
        counter = ThroughputCounter(window_seconds=60)
        for _ in range(50):
            counter.record()
        assert counter.total_count() == 50

    def test_ops_per_second_within_tolerance(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recording N ops in 1 second yields rate ≈ N / window_seconds.

        We freeze the clock at t=1000 and record 60 ops, then evaluate
        the rate. With a 60-second window, that's 60/60 = 1.0 ops/sec.
        Tolerance is ±0.1 to absorb any rounding.
        """
        counter = ThroughputCounter(window_seconds=60)
        # Freeze the clock so every record() lands in the same bucket.
        monkeypatch.setattr(counter, "_now_sec", lambda: 1000)
        for _ in range(60):
            counter.record()
        # 60 ops in a 60-second window -> 1.0 ops/sec.
        rate = counter.ops_per_second()
        assert rate == pytest.approx(1.0, abs=0.1)

    def test_sliding_window_drops_old_buckets(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Advancing time past the window drops stale buckets from the rate.

        Record 100 ops at t=1000 (giving a healthy positive rate), then
        advance the clock by 120 seconds and re-query. The 100 ops
        are now older than the 60-second window, so ``ops_per_second``
        must return 0.0 (or near-zero — there's also no new data).
        """
        counter = ThroughputCounter(window_seconds=60)
        # First batch — at t=1000, 100 records.
        current_time = [1000]
        monkeypatch.setattr(counter, "_now_sec", lambda: current_time[0])
        for _ in range(100):
            counter.record()
        # Sanity: rate is now > 0.
        assert counter.ops_per_second() > 0
        # Advance the clock beyond the window. The deque still holds
        # the t=1000 bucket (only 1 bucket, < maxlen=60), but the
        # rate calculation must apply the time-based cutoff.
        current_time[0] = 1200  # 200 seconds later — way past 60s
        # No new ops -> rate must drop to 0.0.
        assert counter.ops_per_second() == 0.0


# ---------------------------------------------------------------------------
# TestLatency — reservoir-sampled latency histogram
# ---------------------------------------------------------------------------


class TestLatency:
    """Behavior tests for :class:`LatencyHistogram`."""

    def test_empty_histogram_returns_zeros(self) -> None:
        """An untouched histogram returns count=0 and zeroed percentiles."""
        hist = LatencyHistogram()
        snap = hist.snapshot()
        # Stable shape so the dashboard can render a cold start.
        assert snap == {
            "count": 0,
            "mean_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
        }

    def test_percentiles_within_tolerance_for_known_distribution(self) -> None:
        """Recording 1..100 should give p50≈50, p95≈95, p99≈99 (±5)."""
        hist = LatencyHistogram(reservoir_size=1024)
        # 100 values, all distinct, all in the reservoir (1024 capacity).
        for i in range(1, 101):
            hist.record(float(i))
        snap = hist.snapshot()
        # Sanity: count reflects all 100 observations.
        assert snap["count"] == 100
        # Mean should be ~50.5 (sum 1..100 / 100).
        assert snap["mean_ms"] == pytest.approx(50.5, abs=1.0)
        # Nearest-rank percentiles on [1..100] sorted:
        # p50 -> index 50 -> value 51 (close enough); tolerance ±5.
        assert snap["p50_ms"] == pytest.approx(50.0, abs=5.0)
        assert snap["p95_ms"] == pytest.approx(95.0, abs=5.0)
        assert snap["p99_ms"] == pytest.approx(99.0, abs=5.0)

    def test_reservoir_caps_at_size(self) -> None:
        """Recording N >> reservoir_size keeps reservoir at reservoir_size."""
        hist = LatencyHistogram(reservoir_size=1024)
        # 5000 records — well past the 1024 cap.
        for i in range(5000):
            hist.record(float(i))
        snap = hist.snapshot()
        # ``count`` continues to grow with every observation — it's the
        # total seen, not the sample size.
        assert snap["count"] == 5000
        # The reservoir itself is bounded — assert via the private
        # attribute (test code is allowed to peek; production callers
        # use snapshot()).
        assert len(hist._reservoir) <= 1024

    def test_single_value_percentiles(self) -> None:
        """One observation: every percentile returns that single value."""
        hist = LatencyHistogram()
        hist.record(7.5)
        snap = hist.snapshot()
        # count=1, every percentile points to the lone value.
        assert snap["count"] == 1
        assert snap["mean_ms"] == 7.5
        assert snap["p50_ms"] == 7.5
        assert snap["p95_ms"] == 7.5
        assert snap["p99_ms"] == 7.5


# ---------------------------------------------------------------------------
# TestCounters — per-pattern hit counters
# ---------------------------------------------------------------------------


class TestCounters:
    """Behavior tests for :class:`PatternCounters`."""

    def test_incr_creates_and_increments(self) -> None:
        """``incr`` lazily creates new keys at zero before adding."""
        counters = PatternCounters()
        counters.incr("ssn")
        counters.incr("ssn")
        counters.incr("credit_card")
        snap = counters.snapshot()
        assert snap == {"ssn": 2, "credit_card": 1}

    def test_total_sums_all_counters(self) -> None:
        """``total()`` returns the sum across every pattern."""
        counters = PatternCounters()
        counters.incr("ssn", 3)
        counters.incr("email", 5)
        counters.incr("us_phone", 2)
        # 3 + 5 + 2 = 10.
        assert counters.total() == 10

    def test_snapshot_returns_copy(self) -> None:
        """``snapshot()`` returns a copy — mutation doesn't affect state."""
        counters = PatternCounters()
        counters.incr("ssn")
        snap = counters.snapshot()
        # Mutate the snapshot...
        snap["ssn"] = 9999
        snap["something_else"] = 42
        # ...the live state is unaffected.
        assert counters.snapshot() == {"ssn": 1}

    def test_thread_safe_under_concurrent_increments(self) -> None:
        """10 threads × 100 incr → snapshot returns exactly 1000.

        This is the canonical thread-safety regression test the spec
        requires. The lock around ``incr`` ensures the read-modify-write
        is atomic and no increments are lost under contention.
        """
        counters = PatternCounters()

        def worker() -> None:
            # 100 increments per worker against the same key. The
            # lock makes the read-modify-write atomic so no
            # increments are lost.
            for _ in range(100):
                counters.incr("ssn")

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 10 workers × 100 increments = 1000 — exact, not approximate.
        snap = counters.snapshot()
        assert snap["ssn"] == 1000
        assert counters.total() == 1000

    def test_incr_with_explicit_n(self) -> None:
        """``incr(name, n=K)`` adds K, not 1."""
        counters = PatternCounters()
        counters.incr("ssn", 5)
        counters.incr("ssn", 7)
        # 5 + 7 = 12.
        assert counters.snapshot()["ssn"] == 12
