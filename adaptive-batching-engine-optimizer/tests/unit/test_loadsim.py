"""Unit tests for src.loadsim — synthetic arrivals with optional bursts.

Every test injects a seeded ``random.Random`` (or disables bursts entirely) so
burst rolls are fully deterministic. No sleeps, no network.
"""

from __future__ import annotations

import random

import pytest

from src.loadsim import LoadSimulator


# --- No-burst base count ----------------------------------------------------


def test_no_burst_base_count() -> None:
    sim = LoadSimulator(
        messages_per_second=100.0, burst_probability=0.0, rng=random.Random(0)
    )
    assert sim.messages_for_interval(5.0) == 500
    assert sim.current_rate() == pytest.approx(100.0)


def test_no_burst_rounds_to_nearest_int() -> None:
    sim = LoadSimulator(
        messages_per_second=10.0, burst_probability=0.0, rng=random.Random(0)
    )
    # 10 * 0.25 = 2.5 -> round() banker's-rounds to 2.
    assert sim.messages_for_interval(0.25) == 2
    assert isinstance(sim.messages_for_interval(1.0), int)


# --- Guaranteed burst -------------------------------------------------------


def test_guaranteed_burst_multiplies_rate() -> None:
    sim = LoadSimulator(
        messages_per_second=100.0,
        burst_probability=1.0,
        burst_multiplier=10.0,
        rng=random.Random(123),
    )
    assert sim.messages_for_interval(1.0) == 1000
    assert sim.current_rate() == pytest.approx(1000.0)


# --- Determinism ------------------------------------------------------------


def test_same_seed_yields_same_sequence() -> None:
    sim_a = LoadSimulator(
        messages_per_second=100.0, burst_probability=0.3, rng=random.Random(7)
    )
    sim_b = LoadSimulator(
        messages_per_second=100.0, burst_probability=0.3, rng=random.Random(7)
    )
    seq_a = [sim_a.messages_for_interval(1.0) for _ in range(50)]
    seq_b = [sim_b.messages_for_interval(1.0) for _ in range(50)]
    assert seq_a == seq_b


# --- Burst frequency (statistical, seeded) ----------------------------------


def test_burst_frequency_roughly_matches_probability() -> None:
    """Over many seeded intervals, the burst fraction sits near burst_probability.

    Bounds are loose; the fixed seed keeps the observed fraction stable so this
    cannot flake.
    """
    sim = LoadSimulator(
        messages_per_second=100.0,
        burst_probability=0.3,
        burst_multiplier=10.0,
        rng=random.Random(2024),
    )
    n = 2000
    bursts = 0
    for _ in range(n):
        sim.messages_for_interval(1.0)
        # A burst interval reports the multiplied effective rate (1000 vs 100).
        if sim.current_rate() > 100.0:
            bursts += 1
    fraction = bursts / n
    assert 0.2 < fraction < 0.4


# --- Edge cases -------------------------------------------------------------


def test_non_positive_interval_returns_zero() -> None:
    sim = LoadSimulator(
        messages_per_second=100.0, burst_probability=1.0, rng=random.Random(0)
    )
    assert sim.messages_for_interval(0.0) == 0
    assert sim.messages_for_interval(-3.0) == 0
    # A non-positive interval resets the effective rate to the steady value and
    # never rolls a burst.
    assert sim.current_rate() == pytest.approx(100.0)


# --- set_rate clamping ------------------------------------------------------


def test_set_rate_clamps_negative_messages_per_second() -> None:
    sim = LoadSimulator(
        messages_per_second=100.0, burst_probability=0.0, rng=random.Random(0)
    )
    sim.set_rate(-50.0)
    assert sim.messages_per_second == pytest.approx(0.0)
    assert sim.messages_for_interval(5.0) == 0


def test_set_rate_updates_to_positive_value() -> None:
    sim = LoadSimulator(
        messages_per_second=100.0, burst_probability=0.0, rng=random.Random(0)
    )
    sim.set_rate(250.0)
    assert sim.messages_per_second == pytest.approx(250.0)
    assert sim.messages_for_interval(2.0) == 500


def test_set_rate_clamps_burst_probability_to_unit_interval() -> None:
    sim = LoadSimulator(
        messages_per_second=100.0, burst_probability=0.5, rng=random.Random(0)
    )
    sim.set_rate(100.0, burst_probability=5.0)
    assert sim.burst_probability == pytest.approx(1.0)
    sim.set_rate(100.0, burst_probability=-2.0)
    assert sim.burst_probability == pytest.approx(0.0)
