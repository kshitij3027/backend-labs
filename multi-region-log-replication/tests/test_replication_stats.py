"""Unit tests for ``src.replication_stats.ReplicationStatsTracker``.

We exercise:

* Counter bookkeeping for ``record(success=True)`` vs
  ``record(success=False)`` — and confirm failed attempts do **not**
  contaminate the lag deque.
* :meth:`percentiles` — edge cases (empty deque returns zeros) and a
  known sequence ``[10, 20, ..., 100]`` that pins the indexing
  algorithm (``floor(p/100 * (n-1))``).
* :meth:`success_rate` — division by zero, a basic 8-of-10 case.
* :meth:`snapshot` — shape per region (the dashboard depends on these
  exact keys).
* The deque ``maxlen`` actually caps memory at ``window_size``.
"""

from __future__ import annotations

import pytest

from src.replication_stats import ReplicationStatsTracker


REGIONS = ["us-east", "europe", "asia"]


# ---------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------


def test_record_success_increments_success_counter():
    """A successful record bumps the success counter and adds a lag sample."""
    tracker = ReplicationStatsTracker(REGIONS)
    tracker.record("us-east", lag_ms=12.5, success=True)
    tracker.record("us-east", lag_ms=15.0, success=True)

    snap = tracker.snapshot()["us-east"]
    assert snap["success_rate"] == pytest.approx(1.0)
    assert snap["sample_count"] == 2


def test_record_failure_increments_failure_counter():
    """A failure bumps the failure counter and does NOT touch the lag deque."""
    tracker = ReplicationStatsTracker(REGIONS)
    # Use a non-zero lag_ms here on purpose: even though the controller
    # passes 0.0 for offline-secondary failures, the tracker contract is
    # "lag is ignored when success is False" so a non-zero arg must
    # still be ignored.
    tracker.record("us-east", lag_ms=999.0, success=False)
    tracker.record("us-east", lag_ms=999.0, success=False)

    snap = tracker.snapshot()["us-east"]
    # No samples in the deque despite two failed records.
    assert snap["sample_count"] == 0
    # Two failures, zero successes ⇒ rate is 0.0.
    assert snap["success_rate"] == pytest.approx(0.0)
    # Percentiles fall back to zeros when the deque is empty.
    assert (snap["p50"], snap["p95"], snap["p99"]) == (0.0, 0.0, 0.0)


# ---------------------------------------------------------------------
# percentiles
# ---------------------------------------------------------------------


def test_percentiles_no_samples_returns_zeros():
    """Brand-new tracker reports ``(0, 0, 0)`` for any region's percentiles."""
    tracker = ReplicationStatsTracker(REGIONS)
    assert tracker.percentiles("us-east") == (0.0, 0.0, 0.0)
    assert tracker.percentiles("europe") == (0.0, 0.0, 0.0)


def test_percentiles_known_sequence():
    """Feed [10, 20, ..., 100]; verify floor-indexing yields p50=50, p95=p99=90.

    With ``n=10`` samples and the algorithm ``floor(p/100 * (n-1))``:

    * p50 → ``floor(0.50 * 9) = 4`` → sorted[4] = 50
    * p95 → ``floor(0.95 * 9) = 8`` → sorted[8] = 90
    * p99 → ``floor(0.99 * 9) = 8`` → sorted[8] = 90  (same index!)

    The fact p95 == p99 here is intentional — with only 10 samples the
    99th-percentile index collapses onto the 95th's. The dashboard
    handles this gracefully; the test is just pinning the math.
    """
    tracker = ReplicationStatsTracker(REGIONS)
    for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        tracker.record("us-east", lag_ms=float(v), success=True)

    p50, p95, p99 = tracker.percentiles("us-east")
    assert p50 == pytest.approx(50.0)
    assert p95 == pytest.approx(90.0)
    assert p99 == pytest.approx(90.0)


# ---------------------------------------------------------------------
# success_rate
# ---------------------------------------------------------------------


def test_success_rate_zero_division_returns_zero():
    """Empty tracker — no successes, no failures — returns 0.0, not NaN."""
    tracker = ReplicationStatsTracker(REGIONS)
    assert tracker.success_rate("us-east") == pytest.approx(0.0)


def test_success_rate_basic():
    """8 successes + 2 failures ⇒ 0.8."""
    tracker = ReplicationStatsTracker(REGIONS)
    for _ in range(8):
        tracker.record("europe", lag_ms=10.0, success=True)
    for _ in range(2):
        tracker.record("europe", lag_ms=10.0, success=False)

    assert tracker.success_rate("europe") == pytest.approx(0.8)


# ---------------------------------------------------------------------
# snapshot
# ---------------------------------------------------------------------


def test_snapshot_shape():
    """Per region, the snapshot exposes p50/p95/p99/success_rate/sample_count."""
    tracker = ReplicationStatsTracker(REGIONS)
    tracker.record("us-east", lag_ms=10.0, success=True)
    tracker.record("europe", lag_ms=20.0, success=True)
    tracker.record("asia", lag_ms=30.0, success=False)

    snap = tracker.snapshot()
    expected_keys = {"p50", "p95", "p99", "success_rate", "sample_count"}
    for region in REGIONS:
        assert region in snap
        assert set(snap[region].keys()) == expected_keys


# ---------------------------------------------------------------------
# Window cap
# ---------------------------------------------------------------------


def test_window_size_caps_samples():
    """Pumping 1500 samples into a window=1000 deque caps the deque at 1000.

    The success counter, however, keeps a faithful running total — we
    only cap the *lag* memory, not the count of attempts.
    """
    tracker = ReplicationStatsTracker(REGIONS, window_size=1000)
    for i in range(1500):
        tracker.record("us-east", lag_ms=float(i), success=True)

    snap = tracker.snapshot()["us-east"]
    assert snap["sample_count"] == 1000
    # 1500 successes / 1500 attempts ⇒ success rate is exactly 1.0.
    assert snap["success_rate"] == pytest.approx(1.0)
    # The deque kept the *most recent* 1000 samples (500..1499), so the
    # smallest sample is now 500. Percentile math reads from this
    # sorted window.
    p50, _, _ = tracker.percentiles("us-east")
    # Sorted window is [500, 501, ..., 1499]; index 499 → 999.
    assert p50 == pytest.approx(999.0)
