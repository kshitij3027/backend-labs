"""Unit tests for :mod:`src.scaler` — the autoscaling decision engine.

The scaler is pure decision logic, so these tests drive :meth:`Scaler.decide`
directly with crafted snapshot / forecast dicts and assert on the canonical
decision payload. Every branch is exercised:

* reactive scale-up on each of cpu / mem / util (and the "largest breach ratio
  wins" tie-break),
* reactive scale-down when all signals are calm,
* predictive scale-up (and its suppression below the confidence threshold),
* the dead-band hold,
* cooldown suppression,
* bounds clamping at both ends,
* and the dict schema / type contract.

Unless a test is specifically about cooldown, ``last_action_ts=0`` and
``now=1e6`` are used so the cooldown window is comfortably elapsed.
"""

import math

import pytest

from src.config import Settings
from src.scaler import Scaler, DECISION_KEYS


# A point in the far past for the last action, and a large "now", so that
# (now - last_action_ts) dwarfs any cooldown window unless we override it.
PAST = 0.0
NOW = 1e6


def make_snapshot(cpu=50.0, mem=50.0, util=50.0):
    """Build a minimal snapshot dict with the three reactive signals.

    Defaults sit in the dead-band (between scale-down and scale-up thresholds) so
    that, absent overrides, no reactive signal fires.
    """
    return {
        "cpu_percent": float(cpu),
        "memory_percent": float(mem),
        "effective_utilization": float(util),
        # Extra keys the scaler must ignore:
        "timestamp": NOW,
        "workers": 4,
    }


def make_forecast(predicted=0.0, confidence=0.0, trend="flat"):
    """Build a minimal forecast dict with the predictive signal + confidence."""
    return {
        "metric": "effective_utilization",
        "predicted": float(predicted),
        "confidence": float(confidence),
        "trend": trend,
    }


@pytest.fixture
def scaler():
    """A :class:`Scaler` built on default :class:`Settings`."""
    return Scaler(Settings())


def decide(scaler, *, snapshot, forecast, current, last_action_ts=PAST, now=NOW):
    """Convenience wrapper around :meth:`Scaler.decide` with test defaults."""
    return scaler.decide(snapshot, forecast, current, last_action_ts, now)


# --------------------------------------------------------------------------- #
# Reactive scale-up
# --------------------------------------------------------------------------- #
def test_reactive_cpu_scale_up(scaler):
    """cpu > 75 (others moderate) → scale_up, reason reactive_cpu, more workers."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=85.0, mem=50.0, util=50.0),
        forecast=make_forecast(),
        current=4,
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_cpu"
    assert d["trigger_metric"] == "cpu_percent"
    assert d["trigger_value"] == 85.0
    assert d["to_workers"] > d["from_workers"]
    assert d["from_workers"] == 4
    # Reactive reasons carry no forecast confidence.
    assert d["confidence"] is None
    assert d["cooldown_active"] is False


def test_reactive_mem_scale_up(scaler):
    """mem > 80 (others moderate) → scale_up, reason reactive_mem."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=90.0, util=50.0),
        forecast=make_forecast(),
        current=4,
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_mem"
    assert d["trigger_metric"] == "memory_percent"
    assert d["trigger_value"] == 90.0
    assert d["to_workers"] > d["from_workers"]
    # mem=90, target=(80+50)/2=65 → ceil(4*90/65)=ceil(5.54)=6
    assert d["to_workers"] == 6


def test_reactive_util_scale_up_uses_aggressive_hpa_sizing(scaler):
    """util=150 (cpu/mem moderate) → scale_up reactive_util with HPA target.

    current=4, util=150, target=(75+40)/2=57.5 → ceil(4*150/57.5)=ceil(10.43)=11.
    """
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=50.0, util=150.0),
        forecast=make_forecast(),
        current=4,
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_util"
    assert d["trigger_metric"] == "effective_utilization"
    assert d["trigger_value"] == 150.0
    assert d["to_workers"] == 11


def test_largest_breach_ratio_wins_tie_break(scaler):
    """When several signals breach, the most-overloaded (largest ratio) one wins.

    util 150/75 = 2.0 beats cpu 80/75 ≈ 1.07 → reason should be reactive_util.
    """
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=80.0, mem=50.0, util=150.0),
        forecast=make_forecast(),
        current=4,
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_util"
    assert d["trigger_metric"] == "effective_utilization"


# --------------------------------------------------------------------------- #
# Reactive scale-down
# --------------------------------------------------------------------------- #
def test_all_low_scales_down_by_one(scaler):
    """All signals calm at current=8 → scale_down, to_workers=7 (single step)."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=10.0, mem=20.0, util=15.0),
        forecast=make_forecast(),
        current=8,
    )
    assert d["action"] == "scale_down"
    assert d["reason"] == "reactive_util"
    assert d["trigger_metric"] == "effective_utilization"
    assert d["trigger_value"] == 15.0
    assert d["from_workers"] == 8
    assert d["to_workers"] == 7
    assert d["confidence"] is None
    assert d["cooldown_active"] is False


def test_one_signal_not_low_blocks_scale_down(scaler):
    """If even one signal is above its low threshold, no scale-down (dead-band hold).

    cpu=10, util=15 are low but mem=60 (> memory_threshold_scale_down=50) and is
    below the scale-up threshold, so the result is a stable hold.
    """
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=10.0, mem=60.0, util=15.0),
        forecast=make_forecast(),
        current=8,
    )
    assert d["action"] == "hold"
    assert d["reason"] == "stable"
    assert d["to_workers"] == 8


# --------------------------------------------------------------------------- #
# Predictive scale-up
# --------------------------------------------------------------------------- #
def test_predictive_scale_up_when_confident(scaler):
    """Confident forecast above threshold with dead-band metrics → predictive scale_up."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        forecast=make_forecast(predicted=120.0, confidence=0.9, trend="rising"),
        current=4,
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "predictive"
    assert d["trigger_metric"] == "predicted"
    assert d["trigger_value"] == 120.0
    # Predictive reason surfaces the forecast confidence verbatim.
    assert d["confidence"] == 0.9
    assert d["to_workers"] > d["from_workers"]
    # current=4, predicted=120, target=57.5 → ceil(4*120/57.5)=ceil(8.35)=9
    assert d["to_workers"] == 9


def test_low_confidence_forecast_does_not_scale(scaler):
    """A breaching forecast below the confidence threshold is ignored → stable hold."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        forecast=make_forecast(predicted=120.0, confidence=0.5, trend="rising"),
        current=4,
    )
    assert d["action"] == "hold"
    assert d["reason"] == "stable"
    assert d["trigger_metric"] == ""
    assert d["trigger_value"] == 0.0
    assert d["confidence"] is None


def test_reactive_preferred_over_predictive(scaler):
    """When reactive AND predictive both fire, the reactive reason is chosen.

    cpu=85 (reactive scale-up) together with a confident predicted=120 forecast →
    action scale_up but reason reactive_cpu, and confidence stays None (reactive).
    """
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=85.0, mem=50.0, util=50.0),
        forecast=make_forecast(predicted=120.0, confidence=0.95, trend="rising"),
        current=4,
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_cpu"
    assert d["confidence"] is None


# --------------------------------------------------------------------------- #
# Anomaly trigger (optional, lowest-precedence scale-up)
# --------------------------------------------------------------------------- #
def test_anomaly_triggers_scale_up_in_dead_band(scaler):
    """Dead-band metrics + an active positive-zscore anomaly → anomaly scale_up.

    No reactive or predictive signal fires (all metrics sit in the dead-band), so
    the optional anomaly trigger is what provokes the (aggressive) scale-up.
    """
    d = scaler.decide(
        make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=PAST,
        now=NOW,
        anomaly={"active": True, "zscore": 5.0},
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "anomaly"
    assert d["trigger_metric"] == "anomaly_zscore"
    assert d["trigger_value"] == 5.0
    assert d["to_workers"] > d["from_workers"]
    # An anomaly carries no forecast confidence.
    assert d["confidence"] is None
    assert d["cooldown_active"] is False


def test_no_anomaly_holds_stable_backward_compat(scaler):
    """Control: identical dead-band inputs with anomaly=None → plain stable hold.

    Proves the anomaly parameter is purely additive — omitting it (or passing
    ``None``) reproduces the pre-existing behaviour exactly.
    """
    d = scaler.decide(
        make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=PAST,
        now=NOW,
        anomaly=None,
    )
    assert d["action"] == "hold"
    assert d["reason"] == "stable"
    assert d["to_workers"] == 4


def test_reactive_preferred_over_anomaly(scaler):
    """A reactive scale-up outranks the anomaly reason when both could fire."""
    d = scaler.decide(
        make_snapshot(cpu=85.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=PAST,
        now=NOW,
        anomaly={"active": True, "zscore": 9.0},
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_cpu"


def test_negative_zscore_anomaly_does_not_scale_up(scaler):
    """An anomalous DIP (negative zscore) is not an upward spike → no scale-up."""
    d = scaler.decide(
        make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=PAST,
        now=NOW,
        anomaly={"active": True, "zscore": -5.0},
    )
    assert d["action"] == "hold"
    assert d["reason"] == "stable"


def test_anomaly_during_cooldown_holds_with_cooldown_reason(scaler):
    """An anomaly inside the cooldown window is suppressed as a cooldown hold."""
    d = scaler.decide(
        make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=0.0,
        now=10.0,  # < 60s scale-up cooldown
        anomaly={"active": True, "zscore": 5.0},
    )
    assert d["action"] == "hold"
    assert d["reason"] == "cooldown"
    assert d["cooldown_active"] is True
    # The would-be anomaly trigger context is preserved for display.
    assert d["trigger_metric"] == "anomaly_zscore"
    assert d["trigger_value"] == 5.0


# --------------------------------------------------------------------------- #
# Dead-band hold
# --------------------------------------------------------------------------- #
def test_dead_band_holds_stable(scaler):
    """Metrics in the dead-band with no forecast breach → hold, reason stable."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=50.0, util=50.0),
        forecast=make_forecast(predicted=50.0, confidence=0.9),
        current=5,
    )
    assert d["action"] == "hold"
    assert d["reason"] == "stable"
    assert d["from_workers"] == 5
    assert d["to_workers"] == 5
    assert d["trigger_metric"] == ""
    assert d["trigger_value"] == 0.0
    assert d["confidence"] is None
    assert d["cooldown_active"] is False


# --------------------------------------------------------------------------- #
# Cooldown
# --------------------------------------------------------------------------- #
def test_scale_up_suppressed_during_cooldown(scaler):
    """A scale-up candidate within the cooldown window holds with reason cooldown.

    now - last_action_ts = 10 < cooldown_period_seconds (60) → hold/cooldown.
    """
    d = scaler.decide(
        make_snapshot(cpu=85.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=0.0,
        now=10.0,
    )
    assert d["action"] == "hold"
    assert d["reason"] == "cooldown"
    assert d["cooldown_active"] is True
    # The intended move is suppressed; pool stays put.
    assert d["from_workers"] == 4
    assert d["to_workers"] == 4
    # Trigger context is preserved for display.
    assert d["trigger_metric"] == "cpu_percent"
    assert d["trigger_value"] == 85.0


def test_scale_down_suppressed_during_longer_cooldown(scaler):
    """A scale-down candidate within the (longer) scale-down cooldown holds.

    now - last_action_ts = 90 < scale_down_cooldown_seconds (120) → hold/cooldown,
    even though 90 already exceeds the scale-up cooldown (60).
    """
    d = scaler.decide(
        make_snapshot(cpu=10.0, mem=20.0, util=15.0),
        make_forecast(),
        8,
        last_action_ts=0.0,
        now=90.0,
    )
    assert d["action"] == "hold"
    assert d["reason"] == "cooldown"
    assert d["cooldown_active"] is True
    assert d["to_workers"] == 8


def test_scale_up_allowed_after_cooldown_elapses(scaler):
    """Once the cooldown window has passed, the scale-up proceeds normally."""
    d = scaler.decide(
        make_snapshot(cpu=85.0, mem=50.0, util=50.0),
        make_forecast(),
        4,
        last_action_ts=0.0,
        now=61.0,  # just past the 60s scale-up cooldown
    )
    assert d["action"] == "scale_up"
    assert d["reason"] == "reactive_cpu"
    assert d["cooldown_active"] is False
    assert d["to_workers"] > d["from_workers"]


# --------------------------------------------------------------------------- #
# Bounds
# --------------------------------------------------------------------------- #
def test_bounds_block_scale_up_at_max(scaler):
    """At max_workers a scale-up cannot move the pool → hold, reason bounds."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=50.0, util=200.0),
        forecast=make_forecast(),
        current=20,  # == max_workers
    )
    assert d["action"] == "hold"
    assert d["reason"] == "bounds"
    assert d["from_workers"] == 20
    assert d["to_workers"] == 20
    assert d["cooldown_active"] is False


def test_bounds_block_scale_down_at_min(scaler):
    """At min_workers a scale-down cannot move the pool → hold, reason bounds."""
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=10.0, mem=20.0, util=15.0),
        forecast=make_forecast(),
        current=2,  # == min_workers
    )
    assert d["action"] == "hold"
    assert d["reason"] == "bounds"
    assert d["from_workers"] == 2
    assert d["to_workers"] == 2
    assert d["cooldown_active"] is False


def test_scale_up_target_clamped_to_max(scaler):
    """An aggressive scale-up target is clamped to max_workers, still a real move."""
    # current=18, util=300 → ceil(18*300/57.5) is huge; clamps to 20.
    d = decide(
        scaler,
        snapshot=make_snapshot(cpu=50.0, mem=50.0, util=300.0),
        forecast=make_forecast(),
        current=18,
    )
    assert d["action"] == "scale_up"
    assert d["to_workers"] == 20  # clamped to max_workers
    assert d["from_workers"] == 18


# --------------------------------------------------------------------------- #
# Schema / type contract
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "snapshot,forecast,current",
    [
        (make_snapshot(cpu=85.0), make_forecast(), 4),                       # reactive up
        (make_snapshot(cpu=10.0, mem=20.0, util=15.0), make_forecast(), 8),  # reactive down
        (make_snapshot(), make_forecast(predicted=120.0, confidence=0.9), 4),  # predictive up
        (make_snapshot(), make_forecast(), 5),                               # stable hold
        (make_snapshot(util=200.0), make_forecast(), 20),                    # bounds up
        (make_snapshot(cpu=10.0, mem=20.0, util=15.0), make_forecast(), 2),  # bounds down
    ],
)
def test_decision_dict_has_all_keys_and_correct_types(scaler, snapshot, forecast, current):
    """Every decision carries all canonical keys with the right types & invariants."""
    d = decide(scaler, snapshot=snapshot, forecast=forecast, current=current)

    # Exactly the canonical key set.
    assert set(d.keys()) == set(DECISION_KEYS)

    assert d["action"] in ("scale_up", "scale_down", "hold")
    assert isinstance(d["reason"], str)
    assert isinstance(d["from_workers"], int)
    assert isinstance(d["to_workers"], int)
    assert isinstance(d["trigger_metric"], str)
    assert isinstance(d["trigger_value"], float)
    assert d["confidence"] is None or isinstance(d["confidence"], float)
    assert isinstance(d["cooldown_active"], bool)
    assert isinstance(d["timestamp"], float)
    assert math.isfinite(d["timestamp"])

    # from_workers always mirrors the supplied current_workers.
    assert d["from_workers"] == current


def test_now_defaults_to_wall_clock(scaler):
    """Omitting ``now`` stamps a real (finite, positive) wall-clock timestamp."""
    d = scaler.decide(make_snapshot(), make_forecast(), 4, PAST)
    assert isinstance(d["timestamp"], float)
    assert d["timestamp"] > 0.0


def test_missing_keys_default_to_zero_yielding_scale_down(scaler):
    """Absent metric/forecast keys are treated as 0.0.

    With cpu=mem=util=0 every signal sits below its low threshold, so the all-low
    scale-down condition is satisfied: from current=5 the pool steps down to 4.
    This proves the defensive ``.get(...)`` defaulting feeds the algorithm cleanly
    rather than raising on missing keys.
    """
    d = scaler.decide({}, {}, 5, PAST, NOW)
    assert d["action"] == "scale_down"
    assert d["reason"] == "reactive_util"
    assert d["trigger_metric"] == "effective_utilization"
    assert d["trigger_value"] == 0.0
    assert d["to_workers"] == 4
