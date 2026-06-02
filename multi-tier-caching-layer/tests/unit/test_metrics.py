"""Unit tests for the cache metrics aggregator (src/metrics.py)."""
from __future__ import annotations

from src.metrics import HIT_TIERS, MISS_TIERS, Metrics


def test_module_tier_constants() -> None:
    """Hit/miss tier constants name the four cache tiers."""
    assert HIT_TIERS == ("l1", "l2", "l3")
    assert MISS_TIERS == ("backend",)


def test_hit_rate_math_and_performance_counts() -> None:
    """3 L1 hits + 1 backend miss -> 0.75 hit rate, 4 total, 3 hits / 1 miss."""
    m = Metrics()
    for _ in range(3):
        m.record_request("l1", 1.0)
    m.record_request("backend", 200.0)

    assert m.overall_hit_rate == 0.75
    assert m.total_requests == 4

    perf = m.snapshot()["performance"]
    assert perf["overall_hit_rate"] == 0.75
    assert perf["total_requests"] == 4
    assert perf["hits"] == 3
    assert perf["misses"] == 1


def test_overall_hit_rate_zero_when_idle() -> None:
    """No requests recorded -> hit rate is 0.0, not a division error."""
    m = Metrics()
    assert m.overall_hit_rate == 0.0
    assert m.snapshot()["performance"]["total_requests"] == 0


def test_per_tier_isolation() -> None:
    """Recording an l2 hit must not bump the l1 count; tiers track separately."""
    m = Metrics()
    m.record_request("l1", 1.0)
    m.record_request("l1", 1.0)
    m.record_request("l2", 5.0)
    m.record_request("l3", 9.0)
    m.record_request("backend", 150.0)

    tiers = m.snapshot()["tiers"]
    assert tiers["l1"]["hits"] == 2
    assert tiers["l2"]["hits"] == 1
    assert tiers["l3"]["hits"] == 1
    assert tiers["backend"]["misses"] == 1


def test_per_tier_isolation_l2_does_not_touch_l1() -> None:
    """Explicit check: recording only l2 leaves l1 at zero."""
    m = Metrics()
    m.record_request("l2", 5.0)
    m.record_request("l2", 5.0)
    tiers = m.snapshot()["tiers"]
    assert tiers["l1"]["hits"] == 0
    assert tiers["l2"]["hits"] == 2


def test_degradation_alert_fires_below_threshold() -> None:
    """threshold 0.5, min_requests 4: 1 hit + 3 misses (0.25) -> alert fires."""
    m = Metrics(degradation_threshold=0.5, min_requests_for_alert=4)
    m.record_request("l1", 1.0)
    for _ in range(3):
        m.record_request("backend", 150.0)

    assert m.overall_hit_rate == 0.25
    alert = m.degradation_alert()
    assert alert is not None
    assert alert["reason"] == "low_hit_rate"
    assert alert["hit_rate"] == 0.25
    # Snapshot surfaces the same alert object.
    assert m.snapshot()["alert"] is not None


def test_degradation_alert_clears_above_threshold() -> None:
    """Adding enough hits to push the rate above the threshold clears the alert."""
    m = Metrics(degradation_threshold=0.5, min_requests_for_alert=4)
    m.record_request("l1", 1.0)
    for _ in range(3):
        m.record_request("backend", 150.0)
    assert m.degradation_alert() is not None

    # Add many hits: 9 hits + 3 misses = 0.75 > 0.5.
    for _ in range(8):
        m.record_request("l1", 1.0)
    assert m.overall_hit_rate > 0.5
    assert m.degradation_alert() is None
    assert m.snapshot()["alert"] is None


def test_alert_suppressed_below_min_requests() -> None:
    """A low hit rate with too few requests does not fire an alert."""
    m = Metrics(degradation_threshold=0.5, min_requests_for_alert=20)
    # 1 hit + 3 misses = 0.25, but only 4 requests < 20 minimum.
    m.record_request("l1", 1.0)
    for _ in range(3):
        m.record_request("backend", 150.0)
    assert m.overall_hit_rate < 0.5
    assert m.degradation_alert() is None


def test_l2_degraded_flag_fires_alert_despite_good_hit_rate() -> None:
    """mark_l2_degraded(True) forces an alert even with a healthy hit rate."""
    m = Metrics(degradation_threshold=0.5, min_requests_for_alert=4)
    for _ in range(10):
        m.record_request("l1", 1.0)  # 100% hit rate
    assert m.overall_hit_rate == 1.0
    assert m.degradation_alert() is None

    m.mark_l2_degraded(True)
    alert = m.degradation_alert()
    assert alert is not None
    assert alert["reason"] == "l2_degraded"
    snap = m.snapshot()
    assert snap["degraded"] is True
    assert snap["alert"] is not None

    # Clearing the flag removes the alert again.
    m.mark_l2_degraded(False)
    assert m.degradation_alert() is None
    assert m.snapshot()["degraded"] is False


def test_series_capped_at_history_points() -> None:
    """history_points=10, record 25 requests -> series holds only the last 10."""
    m = Metrics(history_points=10)
    for _ in range(25):
        m.record_request("l1", 1.0)
    series = m.series()
    assert len(series["hit_rate"]) == 10


def test_series_n_argument_truncates() -> None:
    """series(n) returns at most the last n points."""
    m = Metrics(history_points=60)
    for _ in range(30):
        m.record_request("l1", 1.0)
    assert len(m.series(5)["hit_rate"]) == 5
    assert len(m.series()["hit_rate"]) == 30


def test_percentiles_zero_on_empty_timing() -> None:
    """With no samples, all timing percentiles/averages are 0.0."""
    m = Metrics()
    timing = m.snapshot()["timing_ms"]
    assert timing["cached_p50"] == 0.0
    assert timing["cached_p90"] == 0.0
    assert timing["cached_avg"] == 0.0
    assert timing["uncached_p50"] == 0.0
    assert timing["uncached_p90"] == 0.0
    assert timing["uncached_avg"] == 0.0


def test_cached_p90_within_sample_range() -> None:
    """Non-empty cached timing: p90 lies within the observed sample range."""
    m = Metrics()
    samples = [float(x) for x in range(1, 101)]  # 1..100 ms
    for s in samples:
        m.record_request("l1", s)
    timing = m.snapshot()["timing_ms"]
    assert min(samples) <= timing["cached_p90"] <= max(samples)
    # For 1..100, nearest-rank p90 is the 90th value = 90.
    assert timing["cached_p90"] == 90.0
    # Average of 1..100 is 50.5.
    assert timing["cached_avg"] == 50.5


def test_cached_and_uncached_timing_separated() -> None:
    """Cached times feed cached_* buckets; misses feed uncached_* buckets."""
    m = Metrics()
    m.record_request("l1", 2.0)
    m.record_request("l2", 4.0)
    m.record_request("backend", 300.0)
    timing = m.snapshot()["timing_ms"]
    # Cached samples are 2 and 4 -> avg 3.0; uncached single sample 300.
    assert timing["cached_avg"] == 3.0
    assert timing["uncached_avg"] == 300.0
    assert timing["uncached_p90"] == 300.0


def test_snapshot_has_memory_placeholder() -> None:
    """snapshot() exposes a 'memory' key as an empty-dict placeholder."""
    m = Metrics()
    snap = m.snapshot()
    assert "memory" in snap
    assert snap["memory"] == {}


def test_snapshot_top_level_shape() -> None:
    """snapshot() exposes the documented top-level keys."""
    m = Metrics()
    m.record_request("l1", 1.0)
    snap = m.snapshot()
    assert set(snap.keys()) == {
        "performance",
        "tiers",
        "timing_ms",
        "memory",
        "degraded",
        "alert",
    }
