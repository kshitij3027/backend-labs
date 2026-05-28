from __future__ import annotations

from src.analysis.statistics import (
    baseline_window,
    ewma,
    percentile,
    rolling_zscore,
)
from src.metrics.sample import MetricSample


def _make_sample(ts: float) -> MetricSample:
    return MetricSample(
        stage="parse",
        ts=ts,
        cpu_pct=0.0,
        mem_mb=0.0,
        io_read_bytes=0,
        io_write_bytes=0,
        queue_depth=0,
        latency_ms=0.0,
    )


def test_percentile_50_sorted_list():
    assert percentile([1, 2, 3, 4, 5], 50) == 3


def test_percentile_empty():
    assert percentile([], 50) == 0.0


def test_percentile_out_of_bound_clamps():
    assert percentile([1, 2, 3], -10) == 1
    assert percentile([1, 2, 3], 200) == 3


def test_zscore_constant_series_zero():
    assert rolling_zscore([5, 5, 5, 5], 5) == 0.0


def test_zscore_positive():
    assert rolling_zscore([1, 2, 3, 4, 5], 10) > 2


def test_zscore_negative():
    assert rolling_zscore([10, 11, 12], 5) < 0


def test_ewma_initial():
    assert ewma([3.0]) == 3.0
    assert ewma([]) == 0.0


def test_baseline_window_slicing():
    now = 1000.0
    samples = [_make_sample(now - i) for i in range(10)]
    result = baseline_window(samples, now, lookback_sec=8, detection_window_sec=3)
    # Window is [now-8, now-3): inclusive lower, exclusive upper.
    # ts = now - i; lower=992, upper=997 → 3 < i <= 8 → i in {4,5,6,7,8}.
    assert len(result) == 5
    returned_ts = sorted(s.ts for s in result)
    assert returned_ts == [now - 8, now - 7, now - 6, now - 5, now - 4]
