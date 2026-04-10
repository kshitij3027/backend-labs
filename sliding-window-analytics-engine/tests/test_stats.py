"""Unit tests for :mod:`src.stats`.

Two categories:

* **IncrementalStats** — verified for empty/single-value corner cases and
  cross-checked against NumPy for non-trivial inputs (including after
  ``remove`` calls) to make sure the ``E[X^2] - E[X]^2`` formulation agrees
  with a reference implementation within a generous 1% relative tolerance.
* **MonotonicMinMax** — verified for basic running min/max, correct
  behaviour around expiration, empty-state safety, and duplicate values.
"""

from __future__ import annotations

import random

import numpy as np
import pytest

from src.stats import IncrementalStats, MonotonicMinMax


# ---------------------------------------------------------------------------
# IncrementalStats
# ---------------------------------------------------------------------------


def test_incremental_empty() -> None:
    """A fresh IncrementalStats exposes zeros for all derived quantities."""
    stats = IncrementalStats()
    assert stats.count == 0
    assert stats.mean == 0.0
    assert stats.variance == 0.0
    assert stats.std_dev == 0.0


def test_incremental_single_value() -> None:
    """With exactly one sample, variance is 0 (population definition)."""
    stats = IncrementalStats()
    stats.add(5.0)
    assert stats.count == 1
    assert stats.mean == 5.0
    assert stats.variance == 0.0
    assert stats.std_dev == 0.0


def test_incremental_against_numpy() -> None:
    """Cross-check mean/std-dev against NumPy on 1000 seeded random floats."""
    rng = random.Random(42)
    values = [rng.uniform(-100.0, 100.0) for _ in range(1000)]

    stats = IncrementalStats()
    for v in values:
        stats.add(v)

    np_values = np.array(values)
    np_mean = float(np.mean(np_values))
    np_std = float(np.std(np_values, ddof=0))  # population std-dev

    # 1% relative tolerance — generous but still catches algorithmic bugs.
    assert stats.mean == pytest.approx(np_mean, rel=0.01)
    assert stats.std_dev == pytest.approx(np_std, rel=0.01)
    assert stats.count == 1000


def test_incremental_remove() -> None:
    """After removing half the values, stats should match NumPy on the survivors."""
    rng = random.Random(123)
    values = [rng.uniform(0.0, 1000.0) for _ in range(100)]

    stats = IncrementalStats()
    for v in values:
        stats.add(v)

    # Remove the first 50 values; the window now reflects only values[50:].
    for v in values[:50]:
        stats.remove(v)

    remaining = np.array(values[50:])
    assert stats.count == 50
    assert stats.mean == pytest.approx(float(np.mean(remaining)), rel=0.01)
    assert stats.std_dev == pytest.approx(float(np.std(remaining, ddof=0)), rel=0.01)


def test_incremental_remove_to_empty_resets_cleanly() -> None:
    """Draining all values should zero out the aggregates, not leave float residue."""
    stats = IncrementalStats()
    stats.add(1.5)
    stats.add(2.5)
    stats.remove(1.5)
    stats.remove(2.5)
    assert stats.count == 0
    assert stats.total == 0.0
    assert stats.total_sq == 0.0
    assert stats.mean == 0.0
    assert stats.variance == 0.0
    assert stats.std_dev == 0.0


# ---------------------------------------------------------------------------
# MonotonicMinMax
# ---------------------------------------------------------------------------


def test_monotonic_minmax_basic() -> None:
    """Across a small non-monotonic sequence, min and max match the obvious values."""
    mm = MonotonicMinMax()
    entries = [(0.0, 5.0), (1.0, 3.0), (2.0, 8.0), (3.0, 2.0), (4.0, 7.0)]
    for ts, val in entries:
        mm.add(ts, val)
    assert mm.min == 2.0
    assert mm.max == 8.0


def test_monotonic_minmax_expiration() -> None:
    """After expiring entries older than 3.0, only ts >= 3.0 should remain."""
    mm = MonotonicMinMax()
    entries = [(0.0, 5.0), (1.0, 3.0), (2.0, 8.0), (3.0, 2.0), (4.0, 7.0)]
    for ts, val in entries:
        mm.add(ts, val)

    mm.expire_before(3.0)

    # Remaining entries: (3.0, 2.0) and (4.0, 7.0).
    assert mm.min == 2.0
    assert mm.max == 7.0


def test_monotonic_minmax_expiration_drops_old_extremes() -> None:
    """If the current min/max belong to expired entries, they must be dropped."""
    mm = MonotonicMinMax()
    # (0, 1) is the running min; (1, 10) is the running max.
    mm.add(0.0, 1.0)
    mm.add(1.0, 10.0)
    mm.add(2.0, 5.0)
    mm.add(3.0, 6.0)

    mm.expire_before(2.0)  # drops ts=0 and ts=1

    # Survivors: (2, 5), (3, 6). min=5, max=6.
    assert mm.min == 5.0
    assert mm.max == 6.0


def test_monotonic_minmax_empty() -> None:
    """An empty structure returns 0.0 for min/max without raising."""
    mm = MonotonicMinMax()
    assert mm.min == 0.0
    assert mm.max == 0.0


def test_monotonic_minmax_duplicates() -> None:
    """Duplicate values must still be handled correctly across expiration."""
    mm = MonotonicMinMax()
    # Several (ts, 5.0) entries, with a couple of different values mixed in.
    mm.add(0.0, 5.0)
    mm.add(1.0, 5.0)
    mm.add(2.0, 3.0)
    mm.add(3.0, 5.0)
    mm.add(4.0, 7.0)

    assert mm.min == 3.0
    assert mm.max == 7.0

    # Expire everything at or before ts=2 (which removes the 3.0 entry too).
    mm.expire_before(3.0)

    # Survivors: (3, 5), (4, 7). min must still be 5 despite the duplicate popping.
    assert mm.min == 5.0
    assert mm.max == 7.0


def test_monotonic_minmax_clear() -> None:
    """clear() wipes both deques so that min/max fall back to 0.0."""
    mm = MonotonicMinMax()
    mm.add(0.0, 1.0)
    mm.add(1.0, 2.0)
    mm.clear()
    assert mm.min == 0.0
    assert mm.max == 0.0
