"""Unit tests for :mod:`src.load_model`.

All time inputs are injected explicitly so every assertion is deterministic; no
test depends on the wall clock. A fixed ``NOW`` anchors the ramp tests.
"""

import math

import pytest

from src.load_model import LoadModel


BASE = 500.0          # baseline arrival rate used across the suite
NOW = 100.0           # arbitrary fixed timestamp (seconds since epoch) for ramp tests


@pytest.fixture
def model() -> LoadModel:
    """A fresh :class:`LoadModel` at the standard baseline rate."""
    return LoadModel(BASE)


# --------------------------------------------------------------------------- #
# Baseline arrival rate
# --------------------------------------------------------------------------- #
def test_arrival_rate_positive_and_within_band(model):
    """For arbitrary ``now``, the rate is positive and within [0.5, 1.5]*base."""
    for now in (0.0, NOW, 12_345.0, 86_400.0, 1_700_000_000.0):
        rate = model.arrival_rate(now)
        assert rate > 0.0
        assert 0.5 * BASE <= rate <= 1.5 * BASE


def test_arrival_rate_near_base(model):
    """The diurnal factor is mild, so the rate stays in the documented band."""
    # Sampled across a full day, the rate must never leave the [0.5, 1.5]*base band.
    for hour in range(24):
        now = hour * 3600.0
        rate = model.arrival_rate(now)
        assert 0.5 * BASE <= rate <= 1.5 * BASE


def test_time_of_day_factor_band(model):
    """The diurnal multiplier stays within roughly [0.5, 1.5] across the day."""
    for hour in range(24):
        now = hour * 3600.0
        factor = model.time_of_day_factor(now)
        assert 0.5 <= factor <= 1.5


def test_never_negative_with_zero_base():
    """A zero baseline can never produce a negative rate."""
    m = LoadModel(0.0)
    assert m.arrival_rate(NOW) == 0.0
    assert m.arrival_rate(54_321.0) == 0.0


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #
def test_determinism_same_now_same_output(model):
    """Repeated calls with the same ``now`` return identical values."""
    first = model.arrival_rate(NOW)
    for _ in range(5):
        assert model.arrival_rate(NOW) == first


def test_determinism_across_instances():
    """Two independent models agree on the same ``now`` (no hidden state)."""
    a = LoadModel(BASE)
    b = LoadModel(BASE)
    assert a.arrival_rate(NOW) == b.arrival_rate(NOW)
    assert a.arrival_rate(50_000.0) == b.arrival_rate(50_000.0)


# --------------------------------------------------------------------------- #
# Ramp (load-injection hook)
# --------------------------------------------------------------------------- #
def test_ramp_start_equals_pre_ramp_base(model):
    """At the ramp start instant, the rate equals the pre-ramp base*tod value."""
    pre = model.arrival_rate(NOW)
    model.ramp(target_rate=5000.0, seconds=10.0, now=NOW)
    assert model.arrival_rate(NOW) == pytest.approx(pre)


def test_ramp_midpoint_between_base_and_target(model):
    """Halfway through the ramp, the rate sits strictly between base and target."""
    pre = model.arrival_rate(NOW)
    model.ramp(target_rate=5000.0, seconds=10.0, now=NOW)

    mid = model.arrival_rate(NOW + 5.0)
    assert pre < mid < 5000.0
    # Linear interpolation => exactly the midpoint between pre and target at t+5/10.
    assert mid == pytest.approx(pre + 0.5 * (5000.0 - pre))


def test_ramp_reaches_target_at_end(model):
    """At the end of the ramp window the rate has reached the target."""
    model.ramp(target_rate=5000.0, seconds=10.0, now=NOW)
    assert model.arrival_rate(NOW + 10.0) == pytest.approx(5000.0)


def test_ramp_holds_target_after_completion(model):
    """Well past the ramp window the rate holds at the target."""
    model.ramp(target_rate=5000.0, seconds=10.0, now=NOW)
    assert model.arrival_rate(NOW + 100.0) == pytest.approx(5000.0)


def test_ramp_before_start_holds_base(model):
    """Querying before the ramp start clamps to the pre-ramp base value."""
    pre = model.arrival_rate(NOW)
    model.ramp(target_rate=5000.0, seconds=10.0, now=NOW)
    # Times earlier than the start clamp frac to 0 -> base value.
    assert model.arrival_rate(NOW - 50.0) == pytest.approx(pre)


def test_ramp_down_interpolates_toward_lower_target(model):
    """A ramp can also drive demand *down* toward a lower target."""
    pre = model.arrival_rate(NOW)
    target = 0.5 * pre
    model.ramp(target_rate=target, seconds=10.0, now=NOW)

    mid = model.arrival_rate(NOW + 5.0)
    assert target < mid < pre
    assert model.arrival_rate(NOW + 10.0) == pytest.approx(target)


def test_ramp_zero_seconds_jumps_immediately(model):
    """A non-positive duration makes the ramp take effect at once."""
    model.ramp(target_rate=3000.0, seconds=0.0, now=NOW)
    assert model.arrival_rate(NOW) == pytest.approx(3000.0)
    assert model.arrival_rate(NOW + 1.0) == pytest.approx(3000.0)


# --------------------------------------------------------------------------- #
# reset
# --------------------------------------------------------------------------- #
def test_reset_clears_ramp(model):
    """After reset, arrival_rate returns to the pure base*tod product."""
    natural = model.arrival_rate(NOW + 100.0)
    model.ramp(target_rate=5000.0, seconds=10.0, now=NOW)
    assert model.arrival_rate(NOW + 100.0) == pytest.approx(5000.0)

    model.reset()
    assert model.arrival_rate(NOW + 100.0) == pytest.approx(natural)


def test_ramp_factor_is_one_without_ramp(model):
    """With no active ramp, the ramp multiplier is exactly 1.0."""
    assert model.ramp_factor(NOW) == 1.0


def test_arrival_rate_equals_base_times_tod_without_ramp(model):
    """Without a ramp, arrival_rate is exactly base * time_of_day_factor."""
    expected = BASE * model.time_of_day_factor(NOW)
    assert model.arrival_rate(NOW) == pytest.approx(expected)
    assert math.isfinite(model.arrival_rate(NOW))
