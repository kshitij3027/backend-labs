"""Unit tests for the synthetic metric generator (C2).

These tests are pure-Python (no DB / no Redis): they exercise determinism,
value-range constraints, point counting, timestamp spacing/tz-awareness, the
presence of a daily seasonal signal, and the default-dataset shape.

The seasonality assertion is deliberately *statistical but robust*: it compares
the daily peak-window mean against the daily trough-window mean over a clean,
spike-free metric (``throughput``) generated with a fixed seed, so it is not
flaky.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from src.generator import (
    METRIC_NAMES,
    generate_default_dataset,
    generate_series,
)


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #
def test_generate_series_same_seed_identical() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    a = generate_series("response_time", start, end, 300, seed=7)
    b = generate_series("response_time", start, end, 300, seed=7)

    assert len(a) == len(b) > 0
    assert [p.value for p in a] == [p.value for p in b]
    assert [p.timestamp for p in a] == [p.timestamp for p in b]


def test_generate_series_different_seed_differs() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    a = generate_series("response_time", start, end, 300, seed=1)
    b = generate_series("response_time", start, end, 300, seed=2)

    assert len(a) == len(b)
    # Not byte-identical: at least one value differs.
    assert [p.value for p in a] != [p.value for p in b]


# --------------------------------------------------------------------------- #
# Value-range constraints per metric
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("metric", METRIC_NAMES)
def test_value_ranges_and_finiteness(metric: str) -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=2)
    pts = generate_series(metric, start, end, 300, seed=123)
    assert pts, "expected a non-empty series"

    values = [p.value for p in pts]
    assert all(math.isfinite(v) for v in values)

    if metric == "response_time":
        assert all(v > 0 for v in values)
    elif metric == "error_rate":
        assert all(0.0 <= v <= 1.0 for v in values)
    elif metric == "throughput":
        assert all(v >= 0 for v in values)
    else:  # pragma: no cover - guard for future metric additions
        pytest.fail(f"unhandled metric {metric!r}")


# --------------------------------------------------------------------------- #
# Point count + timestamp spacing / tz-awareness / monotonicity
# --------------------------------------------------------------------------- #
def test_point_count_and_timestamp_spacing() -> None:
    start = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    interval = 300
    # 1 day @ 300s -> floor(86400 / 300) = 288 points.
    end = start + timedelta(days=1)
    pts = generate_series("throughput", start, end, interval, seed=42)

    assert len(pts) == 288

    # First timestamp is exactly start; all tz-aware; strictly increasing at the
    # right spacing; window is half-open [start, end).
    assert pts[0].timestamp == start
    for i, p in enumerate(pts):
        assert p.timestamp.tzinfo is not None
        assert p.timestamp == start + timedelta(seconds=interval * i)
    deltas = [
        (pts[i + 1].timestamp - pts[i].timestamp).total_seconds()
        for i in range(len(pts) - 1)
    ]
    assert all(d == interval for d in deltas)
    assert pts[-1].timestamp < end


def test_generate_series_invalid_args() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        generate_series("throughput", start, start + timedelta(days=1), 0, seed=1)
    with pytest.raises(ValueError):
        generate_series("throughput", start, start, 300, seed=1)
    with pytest.raises(ValueError):
        generate_series("not_a_metric", start, start + timedelta(days=1), 300, seed=1)


def test_naive_start_coerced_to_utc() -> None:
    naive_start = datetime(2026, 1, 1, 0, 0)  # no tzinfo
    end = naive_start.replace(tzinfo=timezone.utc) + timedelta(hours=2)
    pts = generate_series("throughput", naive_start, end, 600, seed=5)
    assert pts
    assert all(p.timestamp.tzinfo is not None for p in pts)


# --------------------------------------------------------------------------- #
# Daily seasonality signal present
# --------------------------------------------------------------------------- #
def test_daily_seasonality_peak_above_trough() -> None:
    """Daytime window mean should clearly exceed the nighttime window mean.

    The daily sinusoid (``sin(phase - pi/2)``) troughs near 00:00 and peaks near
    12:00 of seconds-of-day. We use throughput (large daily amplitude, no spikes,
    modest noise) over 3 days and compare the mean of points in 10:00-14:00 vs
    22:00-02:00. With amp=220 vs noise_sd=18, the gap dwarfs noise.
    """
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=3)
    pts = generate_series("throughput", start, end, 300, seed=99)

    day_vals = [p.value for p in pts if 10 <= p.timestamp.hour < 14]
    night_vals = [p.value for p in pts if p.timestamp.hour >= 22 or p.timestamp.hour < 2]
    assert day_vals and night_vals

    day_mean = float(np.mean(day_vals))
    night_mean = float(np.mean(night_vals))
    # Peak-to-trough spread must clearly exceed noise (sd ~18).
    assert day_mean - night_mean > 100.0


def test_daily_seasonality_correlation_nonzero() -> None:
    """Series correlates with a 24h sinusoid matched to the generator phase."""
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(days=3)
    pts = generate_series("throughput", start, end, 300, seed=11)

    secs_of_day = np.array(
        [
            p.timestamp.hour * 3600 + p.timestamp.minute * 60 + p.timestamp.second
            for p in pts
        ],
        dtype=float,
    )
    ref = np.sin(2.0 * np.pi * (secs_of_day / 86400.0) - np.pi / 2.0)
    values = np.array([p.value for p in pts], dtype=float)
    corr = float(np.corrcoef(values, ref)[0, 1])
    # Strong, clearly nonzero positive correlation with the daily signal.
    assert corr > 0.7


# --------------------------------------------------------------------------- #
# Default dataset
# --------------------------------------------------------------------------- #
def test_generate_default_dataset_shape_7d() -> None:
    fixed_end = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    ds = generate_default_dataset(days=7, interval_seconds=300, seed=42, end=fixed_end)

    assert set(ds.keys()) == set(METRIC_NAMES)
    # 7d @ 300s -> 7 * 288 = 2016 points each.
    for name in METRIC_NAMES:
        assert len(ds[name]) == 2016
        assert all(p.metric_name == name for p in ds[name])


def test_generate_default_dataset_deterministic() -> None:
    fixed_end = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
    a = generate_default_dataset(days=1, interval_seconds=600, seed=42, end=fixed_end)
    b = generate_default_dataset(days=1, interval_seconds=600, seed=42, end=fixed_end)
    for name in METRIC_NAMES:
        assert [p.value for p in a[name]] == [p.value for p in b[name]]


def test_generate_default_dataset_rejects_nonpositive_days() -> None:
    with pytest.raises(ValueError):
        generate_default_dataset(days=0)
