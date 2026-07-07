"""Unit tests for src.aggregation.MetricAggregator — ring bucketing, derived
series, NaN semantics, roll sweeping, and timestamp-sanity guards.

Events are built directly (no generator/parser round-trip) so each test controls
exactly which second and metrics a sample lands in. All clocks are simulated;
second 1000 is the conventional base.
"""

import math

import numpy as np
import pytest

from src.aggregation import SERIES, WINDOW_SLOTS, MetricAggregator
from src.models import LogEvent, SourceType

BASE = 1000.0


def make_event(
    source: SourceType = SourceType.WEB,
    ts: float = BASE + 0.5,
    level: str = "INFO",
    error_code: str | None = None,
    **metrics: float,
) -> LogEvent:
    """A minimal LogEvent at ``ts`` carrying exactly the given numeric metrics."""
    return LogEvent(
        id="ev-test",
        timestamp=ts,
        source=source,
        service="test-service",
        level=level,
        message="test event",
        error_code=error_code,
        metrics=dict(metrics),
    )


# --- Bucketing and the completed-seconds window --------------------------------
def test_bucket_placement():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.5, status=200.0))  # lands in second 1000
    agg.roll(1002.0)
    series = agg.series("web.request_count", n=2)  # completed seconds 1000, 1001
    assert series.shape == (2,)
    assert series.tolist() == [1.0, 0.0]


def test_series_excludes_in_progress_second():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.5, status=200.0))
    agg.roll(1000.9)  # second 1000 is still in progress -> invisible to reads
    assert agg.series("web.request_count", n=2).tolist() == [0.0, 0.0]


def test_current_second_helper():
    assert MetricAggregator.current_second(1000.9) == 1000


# --- Derived series --------------------------------------------------------------
def test_latency_avg_is_sum_over_count():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.2, status=200.0, latency_ms=10.0))
    agg.add_event(make_event(ts=1000.8, status=200.0, latency_ms=30.0))
    agg.roll(1002.0)
    series = agg.series("web.latency_ms_avg", n=2)
    assert series[0] == pytest.approx(20.0)
    assert math.isnan(series[1])  # empty second -> NaN, never a fake 0.0


def test_error_rate_is_5xx_fraction_of_requests():
    agg = MetricAggregator()
    for status in (200.0, 200.0, 200.0, 500.0):
        level = "ERROR" if status >= 500 else "INFO"
        agg.add_event(make_event(ts=1000.1, level=level, status=status))
    agg.roll(1002.0)
    assert agg.series("web.error_5xx_count", n=2)[0] == 1.0
    assert agg.series("web.error_rate", n=2)[0] == pytest.approx(0.25)


def test_pool_utilization_uses_remembered_pool_size():
    agg = MetricAggregator()
    agg.add_event(
        make_event(source=SourceType.DATABASE, ts=1000.3, pool_in_use=10.0, pool_size=20.0)
    )
    agg.roll(1002.0)
    util = agg.series("db.pool_utilization", n=2)
    assert util[0] == pytest.approx(0.5)
    assert math.isnan(util[1])  # no db samples that second -> unknown, not 0


def test_pool_utilization_nan_when_pool_size_never_seen():
    agg = MetricAggregator()
    agg.add_event(make_event(source=SourceType.DATABASE, ts=1000.3))  # no pool metrics
    agg.roll(1002.0)
    assert np.isnan(agg.series("db.pool_utilization", n=2)).all()


# --- Empty-data semantics ---------------------------------------------------------
def test_empty_seconds_read_zero_counts_and_nan_rates():
    agg = MetricAggregator()
    agg.roll(1005.0)  # a head exists but no event ever arrived
    assert agg.series("web.request_count", n=3).tolist() == [0.0, 0.0, 0.0]
    assert np.isnan(agg.series("web.latency_ms_avg", n=3)).all()
    assert np.isnan(agg.series("web.error_rate", n=3)).all()


def test_defaults_before_any_data_at_all():
    agg = MetricAggregator()
    assert agg.series("db.query_count", n=4).tolist() == [0.0] * 4
    assert np.isnan(agg.series("db.pool_in_use_avg", n=4)).all()


# --- Rolling / ring hygiene -------------------------------------------------------
def test_skipped_seconds_read_zero_after_advance():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.4, status=200.0))
    agg.add_event(make_event(ts=1003.2, status=200.0))  # newer event advances the head
    agg.roll(1005.0)
    series = agg.series("web.request_count", n=5)  # seconds 1000..1004
    assert series.tolist() == [1.0, 0.0, 0.0, 1.0, 0.0]


def test_roll_sweeps_reused_slots_after_wraparound():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.4, status=200.0))
    # Advance one full window: second 1120 reuses second 1000's ring slot.
    agg.roll(BASE + WINDOW_SLOTS)
    agg.add_event(make_event(ts=1120.6, status=200.0))
    agg.roll(1122.0)
    series = agg.series("web.request_count", n=2)  # seconds 1120, 1121
    # Exactly 1.0: the stale count from second 1000 was swept, not accumulated.
    assert series.tolist() == [1.0, 0.0]


# --- Timestamp-sanity guards ------------------------------------------------------
def test_stale_event_older_than_window_is_ignored():
    agg = MetricAggregator()
    agg.roll(2200.0)  # head at 2200 over an empty ring
    # 2075 is 125s behind the head: its slot now belongs to second 2195, so
    # accepting it would fabricate a count in the readable window.
    agg.add_event(make_event(ts=2075.5, status=200.0))
    assert not agg.series("web.request_count", n=WINDOW_SLOTS - 1).any()


def test_future_event_beyond_tolerance_is_ignored():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.5, status=200.0))
    agg.add_event(make_event(ts=1010.0, status=200.0))  # 10s ahead of the head: dropped
    agg.add_event(make_event(ts=1004.0, status=200.0))  # 4s ahead: fine, advances head
    agg.roll(1012.0)
    series = agg.series("web.request_count", n=12)  # seconds 1000..1011
    assert series[0] == 1.0
    assert series[4] == 1.0
    assert series.sum() == 2.0  # nothing landed in second 1010


# --- Aligned reads and error presence ---------------------------------------------
def test_aligned_returns_equal_length_same_window_arrays():
    agg = MetricAggregator()
    agg.add_event(make_event(ts=1000.1, level="ERROR", status=500.0, latency_ms=80.0))
    agg.roll(1002.0)
    names = ("web.request_count", "web.latency_ms_avg", "web.error_rate")
    out = agg.aligned(names, n=30)
    assert set(out) == set(names)
    assert all(arr.shape == (30,) for arr in out.values())
    # The event's second sits at the same position in every aligned series.
    assert out["web.request_count"][-2] == 1.0
    assert out["web.latency_ms_avg"][-2] == pytest.approx(80.0)
    assert out["web.error_rate"][-2] == pytest.approx(1.0)


def test_error_presence_is_binary_per_source():
    agg = MetricAggregator()
    agg.add_event(make_event(source=SourceType.DATABASE, ts=1000.2, level="ERROR"))
    agg.add_event(make_event(source=SourceType.DATABASE, ts=1000.7, level="ERROR"))
    agg.add_event(make_event(source=SourceType.WEB, ts=1000.5, status=200.0))
    agg.roll(1002.0)
    # Two db errors in one second still read 1.0 — presence, not a count.
    assert agg.error_presence("database", n=2).tolist() == [1.0, 0.0]
    assert agg.error_presence("web", n=2).tolist() == [0.0, 0.0]


# --- Registry --------------------------------------------------------------------
def test_every_registered_series_resolves():
    agg = MetricAggregator()
    assert len(SERIES) == len(set(SERIES)) == 19
    for name in SERIES:
        assert agg.series(name, n=5).shape == (5,)
