"""Unit tests for :class:`src.sliding_window.SlidingWindow`.

Coverage goals:

* Empty windows produce zeroed snapshots.
* Basic add-then-snapshot behaviour yields correct count/sum/mean/min/max.
* Time-based expiry kicks in on both ``add`` and ``snapshot`` paths.
* The ``max_size`` cap evicts the oldest events and keeps stats in sync.
* A large random input stream agrees with NumPy on mean and std-dev.
* Min/max correctly reflect only the events still within the time window.
"""

from __future__ import annotations

import random

import numpy as np
import pytest

from src.sliding_window import SlidingWindow


def _make_window(
    *,
    window_size: float = 10.0,
    slide_interval: float = 5.0,
    max_size: int = 100,
    name: str = "test_window",
    resolution: str = "1s",
) -> SlidingWindow:
    """Factory helper so individual tests stay concise."""
    return SlidingWindow(
        name=name,
        resolution=resolution,
        window_size=window_size,
        slide_interval=slide_interval,
        max_size=max_size,
    )


def test_empty_snapshot(make_event) -> None:
    """A fresh window snapshots to all-zero stats."""
    window = _make_window()
    result = window.snapshot(now=100.0)

    assert result.window_name == "test_window"
    assert result.resolution == "1s"
    assert result.count == 0
    assert result.sum == 0.0
    assert result.average == 0.0
    assert result.min == 0.0
    assert result.max == 0.0
    assert result.std_dev == 0.0
    # The window should still span [now - window_size, now].
    assert result.window_start == pytest.approx(90.0)
    assert result.window_end == pytest.approx(100.0)


def test_basic_add_and_snapshot(make_event) -> None:
    """Five events in a fresh 10s window produce the expected aggregates."""
    window = _make_window(window_size=10.0, slide_interval=5.0, max_size=100)

    for i in range(1, 6):
        window.add(make_event(timestamp=float(i), value=float(i)))

    result = window.snapshot(now=5.0)
    assert result.count == 5
    assert result.sum == pytest.approx(15.0)
    assert result.average == pytest.approx(3.0)
    assert result.min == pytest.approx(1.0)
    assert result.max == pytest.approx(5.0)


def test_time_based_expiry(make_event) -> None:
    """Events older than ``now - window_size`` must be dropped on add."""
    window = _make_window(window_size=10.0, max_size=1000)

    # Add events at t=0..20 (21 events total).
    for i in range(21):
        window.add(make_event(timestamp=float(i), value=float(i)))

    result = window.snapshot(now=20.0)

    # cutoff = 20 - 10 = 10; events with ts in [10, 20] survive → 11 events.
    assert result.count == 11
    # Values are the same as the timestamps: 10 + 11 + ... + 20 = 165.
    assert result.sum == pytest.approx(165.0)
    assert result.min == pytest.approx(10.0)
    assert result.max == pytest.approx(20.0)


def test_max_size_cap(make_event) -> None:
    """``max_size`` clamps the buffer to the most-recent N events."""
    window = _make_window(window_size=1000.0, max_size=5)

    # Add 10 events with strictly increasing timestamps well within window_size.
    for i in range(10):
        window.add(make_event(timestamp=float(i), value=float(i * 10)))

    result = window.snapshot(now=9.0)
    assert result.count == 5

    # Only events i=5..9 should survive → values 50..90.
    expected_values = [50.0, 60.0, 70.0, 80.0, 90.0]
    assert result.sum == pytest.approx(sum(expected_values))
    assert result.average == pytest.approx(sum(expected_values) / 5)
    assert result.min == pytest.approx(min(expected_values))
    assert result.max == pytest.approx(max(expected_values))
    assert window.size() == 5


def test_incremental_stats_vs_numpy(make_event) -> None:
    """On 5000 in-window events, mean/std-dev must agree with NumPy."""
    window = _make_window(window_size=60.0, max_size=10000)

    rng = random.Random(2024)
    values: list[float] = []
    # Generate timestamps spread across [0, 60) so none expire at snapshot(60).
    for i in range(5000):
        ts = (i / 5000) * 60.0  # 0 <= ts < 60
        value = rng.uniform(0.0, 1000.0)
        values.append(value)
        window.add(make_event(timestamp=ts, value=value))

    result = window.snapshot(now=60.0)

    # cutoff = 60 - 60 = 0; all timestamps are >= 0, so all 5000 survive.
    assert result.count == 5000

    np_values = np.array(values)
    assert result.average == pytest.approx(float(np.mean(np_values)), rel=0.01)
    assert result.std_dev == pytest.approx(float(np.std(np_values, ddof=0)), rel=0.01)


def test_monotonic_stats_with_expiry(make_event) -> None:
    """Min/max must reflect only events whose timestamp is still in-window."""
    window = _make_window(window_size=10.0, max_size=10000)

    # 20 events at t=0..19 with distinct values. Give early events the
    # extreme values so we can verify they really do get dropped at expiry.
    distinct_values = [
        100.0,  # t=0  (should expire)
        1.0,    # t=1  (should expire — was min before expiry)
        200.0,  # t=2  (should expire — was max before expiry)
        50.0,   # t=3
        60.0,   # t=4
        70.0,   # t=5
        80.0,   # t=6
        90.0,   # t=7
        95.0,   # t=8
        20.0,   # t=9  (lives; new min)
        30.0,   # t=10
        40.0,   # t=11
        45.0,   # t=12
        55.0,   # t=13
        65.0,   # t=14
        75.0,   # t=15
        85.0,   # t=16
        92.0,   # t=17
        98.0,   # t=18
        150.0,  # t=19 (lives; new max)
    ]
    for i, v in enumerate(distinct_values):
        window.add(make_event(timestamp=float(i), value=v))

    # Snapshot at t=19 with window_size=10: cutoff=9 → events with ts >= 9.
    result = window.snapshot(now=19.0)
    survivors = distinct_values[9:]  # indices 9..19 inclusive
    assert result.count == len(survivors)
    assert result.min == pytest.approx(min(survivors))
    assert result.max == pytest.approx(max(survivors))


def test_lazy_expiry_on_snapshot(make_event) -> None:
    """Snapshotting long after the last add should still expire stale events."""
    window = _make_window(window_size=10.0, max_size=1000)

    for i in range(6):  # t=0..5
        window.add(make_event(timestamp=float(i), value=1.0))

    # Snapshot far in the future — every event is ancient history.
    result = window.snapshot(now=100.0)
    assert result.count == 0
    assert result.sum == 0.0
    assert result.average == 0.0
    assert result.min == 0.0
    assert result.max == 0.0
    assert result.std_dev == 0.0
    assert window.size() == 0


def test_size_cap_with_time_expiry_interaction(make_event) -> None:
    """Time-based expiry should run before the size-cap eviction kicks in.

    With window_size=5 and max_size=3, the size cap would evict aggressively
    — but if events are time-expired first they free up slack without
    needing to drop recent entries prematurely.
    """
    window = _make_window(window_size=5.0, max_size=3)

    # Fill the window exactly to max_size within the time window.
    window.add(make_event(timestamp=0.0, value=10.0))
    window.add(make_event(timestamp=1.0, value=20.0))
    window.add(make_event(timestamp=2.0, value=30.0))
    assert window.size() == 3

    # Add one more event still within the time window — the size cap must
    # evict the oldest (t=0, v=10) since nothing is time-expired yet.
    window.add(make_event(timestamp=3.0, value=40.0))
    result = window.snapshot(now=3.0)
    assert result.count == 3
    assert result.min == pytest.approx(20.0)
    assert result.max == pytest.approx(40.0)

    # Now jump ahead so everything prior is time-expired. Only the new event
    # should remain; size cap should not have triggered at all.
    window.add(make_event(timestamp=100.0, value=99.0))
    result = window.snapshot(now=100.0)
    assert result.count == 1
    assert result.sum == pytest.approx(99.0)
    assert result.min == pytest.approx(99.0)
    assert result.max == pytest.approx(99.0)
