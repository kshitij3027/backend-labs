"""Integration tests for :class:`src.orchestrator.Orchestrator`.

These exercise the *wiring*: the orchestrator drives the real load model, worker
pool, metric collector, rolling history, forecaster and scaler together. Rather
than stubbing collaborators we inject deterministic simulated time (an explicit
``now`` stepped forward by the monitoring interval) and a high-amplitude load ramp,
then assert on the emergent behaviour of the closed control loop.

Key config choices for determinism (see :func:`fast_config`):

* ``cooldown_period_seconds == scale_down_cooldown_seconds == 0`` so every actionable
  decision can fire on the very next tick — no cooldown holds to reason around.
* ``base_arrival_rate=100`` with ``capacity_per_worker=400`` keeps baseline
  effective utilization tiny (well inside the dead-band), so nothing scales until a
  ramp is injected.
* A ramp to ``20000`` msgs/sec drives utilization far above the scale-up threshold
  (20000 / (workers * 400)), forcing the autoscaler to react and climb toward
  ``max_workers``.

Nothing here sleeps: simulated time is supplied explicitly so a few dozen ticks run
in milliseconds.
"""

import pytest

from src.config import Settings
from src.orchestrator import Orchestrator, SystemState


# Documented keys of the canonical status payload (Orchestrator.snapshot).
SNAPSHOT_KEYS = {
    "timestamp",
    "current_metrics",
    "forecast",
    "workers",
    "last_decision",
    "cooldown_remaining_s",
    "scaling_history",
    "anomaly",
    "cost",
}

# A deterministic, far-from-midnight start time so the time-of-day factor is stable
# across the short simulated window (the exact value does not matter — the ramp
# dominates the diurnal swing by orders of magnitude).
T0 = 1_000_000.0


def fast_config(**overrides) -> Settings:
    """Build a :class:`Settings` tuned for fast, deterministic scaling tests.

    Cooldowns are zeroed so actions fire immediately; the workload/capacity numbers
    make baseline utilization negligible while leaving plenty of head-room for an
    injected ramp to push utilization past the scale-up threshold.
    """
    params = dict(
        cooldown_period_seconds=0.0,
        scale_down_cooldown_seconds=0.0,
        monitoring_interval_seconds=5.0,
        orchestration_interval_seconds=5.0,
        min_workers=2,
        max_workers=20,
        capacity_per_worker=400.0,
        base_arrival_rate=100.0,
    )
    params.update(overrides)
    return Settings(**params)


def run_ticks(orch: Orchestrator, start: float, count: int) -> float:
    """Drive ``count`` paired collector+orchestration ticks starting at ``start``.

    Each iteration advances simulated time by the monitoring interval, collects a
    snapshot (advancing the queue simulation) and then runs one orchestration step.

    Returns:
        The timestamp of the final tick (so callers can continue stepping forward).
    """
    interval = orch.config.monitoring_interval_seconds
    t = start
    for _ in range(count):
        orch.collector_tick(now=t)
        orch.orchestration_tick(now=t)
        t += interval
    return t - interval


# --------------------------------------------------------------------------- #
# Construction / wiring
# --------------------------------------------------------------------------- #
def test_construction_wires_all_components():
    """The orchestrator builds every collaborator from a single Settings."""
    orch = Orchestrator(fast_config())

    assert orch.load_model is not None
    assert orch.pool is not None
    assert orch.collector is not None
    assert orch.history is not None
    assert orch.scaler is not None
    assert isinstance(orch.state, SystemState)

    # Pool starts at the configured minimum, on the simulated backend.
    assert orch.pool.current() == orch.config.min_workers
    assert orch.pool.backend == "simulated"

    # State is empty until the loops run.
    assert orch.current_metrics is None
    assert orch.forecast is None
    assert orch.last_decision is None
    assert orch.last_action_ts == 0.0


# --------------------------------------------------------------------------- #
# Collector loop
# --------------------------------------------------------------------------- #
def test_collector_tick_populates_metrics_and_history():
    """A single collector tick records a snapshot and grows the history by one."""
    orch = Orchestrator(fast_config())

    snap = orch.collector_tick(now=T0)

    assert snap["effective_utilization"] is not None
    assert orch.current_metrics == snap
    assert len(orch.history) == 1
    # First tick has no prior prediction, so no residual is recorded yet.
    assert len(orch._recent_residuals) == 0


# --------------------------------------------------------------------------- #
# Scale-up under rising load
# --------------------------------------------------------------------------- #
def test_scale_up_under_rising_load():
    """An injected load ramp drives the autoscaler to add workers."""
    orch = Orchestrator(fast_config())
    min_workers = orch.config.min_workers

    # Inject demand far above what the minimum pool can serve. 20000 / (2 * 400)
    # = 2500% utilization at the start, way past the 75% scale-up threshold.
    orch.load_model.ramp(target_rate=20000.0, seconds=10.0, now=T0)

    run_ticks(orch, T0, count=30)

    # The pool must have grown beyond the floor in response to the load.
    assert orch.pool.current() > min_workers

    # And at least one recorded decision must be an actual scale-up.
    actions = [d["action"] for d in orch.scaling_history]
    assert "scale_up" in actions


# --------------------------------------------------------------------------- #
# Settle / no infinite growth
# --------------------------------------------------------------------------- #
def test_settles_without_infinite_growth():
    """Once capacity is added, growth stops and the pool never exceeds max_workers."""
    orch = Orchestrator(fast_config())
    max_workers = orch.config.max_workers

    orch.load_model.ramp(target_rate=20000.0, seconds=10.0, now=T0)

    # Run long enough to reach the capacity ceiling and then keep ticking.
    run_ticks(orch, T0, count=40)

    # Never breaches the configured upper bound.
    assert orch.pool.current() <= max_workers

    # Capture the worker count, run a few more ticks, and confirm it is no longer
    # increasing (it has either settled below threshold or pinned at max → bounds
    # holds). Demand is constant (ramp held at target), so capacity should be stable.
    settled = orch.pool.current()
    run_ticks(orch, T0 + 40 * orch.config.monitoring_interval_seconds, count=6)
    assert orch.pool.current() <= settled

    # Growth must have stopped: either the pool is pinned at the ceiling (every
    # further scale-up is a bounds-hold that never reaches scaling_history), or the
    # latest decision is a plain hold once equilibrium is reached.
    assert orch.pool.current() == max_workers or orch.last_decision["action"] == "hold"


# --------------------------------------------------------------------------- #
# snapshot() shape
# --------------------------------------------------------------------------- #
def test_snapshot_shape_before_any_tick():
    """snapshot() returns the full documented schema even before any tick runs."""
    orch = Orchestrator(fast_config())

    snap = orch.snapshot()

    assert set(snap.keys()) == SNAPSHOT_KEYS
    assert snap["current_metrics"] == {}
    assert snap["forecast"] == {}
    assert snap["last_decision"] == {}
    assert snap["scaling_history"] == []
    assert snap["cooldown_remaining_s"] == 0
    assert snap["anomaly"] == {"active": False, "zscore": 0.0}
    assert snap["cost"] == {}

    workers = snap["workers"]
    assert workers["current"] == orch.config.min_workers
    assert workers["min"] == orch.config.min_workers
    assert workers["max"] == orch.config.max_workers
    assert workers["backend"] == "simulated"


def test_snapshot_populated_after_ticks():
    """After ticks, snapshot() reflects collected metrics and a forecast."""
    orch = Orchestrator(fast_config())
    run_ticks(orch, T0, count=3)

    snap = orch.snapshot()

    assert set(snap.keys()) == SNAPSHOT_KEYS
    assert snap["current_metrics"]  # non-empty after a collector tick
    assert "effective_utilization" in snap["current_metrics"]
    assert snap["forecast"]  # non-empty after an orchestration tick
    assert snap["forecast"]["metric"] == "effective_utilization"
    assert snap["workers"]["backend"] == "simulated"


# --------------------------------------------------------------------------- #
# Manual scale
# --------------------------------------------------------------------------- #
def test_manual_scale_up_by_one():
    """request_manual_scale(direction='up') adds exactly one worker, reason=manual."""
    orch = Orchestrator(fast_config())
    before = orch.pool.current()

    decision = orch.request_manual_scale(direction="up", now=T0)

    assert orch.pool.current() == before + 1
    assert decision["action"] == "scale_up"
    assert decision["reason"] == "manual"
    assert decision["from_workers"] == before
    assert decision["to_workers"] == before + 1
    assert decision["cooldown_active"] is False
    # Recorded in history and as the last decision.
    assert orch.last_decision["reason"] == "manual"
    assert list(orch.scaling_history)[-1]["reason"] == "manual"


def test_manual_scale_to_target_clamped():
    """request_manual_scale(target=10) sets the pool to 10 (within bounds)."""
    orch = Orchestrator(fast_config())

    decision = orch.request_manual_scale(target=10, now=T0)

    assert orch.pool.current() == 10
    assert decision["to_workers"] == 10
    assert decision["reason"] == "manual"


def test_manual_scale_target_clamps_to_max():
    """A target above max_workers is clamped to the configured maximum."""
    orch = Orchestrator(fast_config())
    max_workers = orch.config.max_workers

    decision = orch.request_manual_scale(target=9999, now=T0)

    assert orch.pool.current() == max_workers
    assert decision["to_workers"] == max_workers


def test_manual_scale_down_by_one():
    """direction='down' removes one worker (and clamps at the floor)."""
    orch = Orchestrator(fast_config())
    # Bump up first so there is room to come down.
    orch.request_manual_scale(target=5, now=T0)

    decision = orch.request_manual_scale(direction="down", now=T0 + 1)

    assert orch.pool.current() == 4
    assert decision["action"] == "scale_down"
    assert decision["reason"] == "manual"


# --------------------------------------------------------------------------- #
# Residual tracking + confidence
# --------------------------------------------------------------------------- #
def test_residual_tracking_and_confidence():
    """After several paired ticks, residuals accumulate and confidence is in [0, 1]."""
    orch = Orchestrator(fast_config())
    # A gentle ramp keeps utilization moving so predictions have something to track.
    orch.load_model.ramp(target_rate=4000.0, seconds=60.0, now=T0)

    run_ticks(orch, T0, count=8)

    # The collector records a residual on every tick after the first prediction.
    assert len(orch._recent_residuals) > 0
    assert all(r >= 0.0 for r in orch._recent_residuals)

    conf = orch.forecast["confidence"]
    assert isinstance(conf, float)
    assert 0.0 <= conf <= 1.0


def test_residuals_are_bounded():
    """The residual deque honours its maxlen (never grows without bound)."""
    orch = Orchestrator(fast_config())
    orch.load_model.ramp(target_rate=4000.0, seconds=60.0, now=T0)

    # Many more ticks than the residual window (20) — it must stay capped.
    run_ticks(orch, T0, count=40)

    assert len(orch._recent_residuals) <= 20


# --------------------------------------------------------------------------- #
# Forecast horizon wiring
# --------------------------------------------------------------------------- #
def test_orchestration_tick_returns_decision_schema():
    """Each orchestration tick returns a well-formed decision dict."""
    orch = Orchestrator(fast_config())
    orch.collector_tick(now=T0)

    decision = orch.orchestration_tick(now=T0)

    for key in (
        "action",
        "reason",
        "from_workers",
        "to_workers",
        "trigger_metric",
        "trigger_value",
        "confidence",
        "cooldown_active",
        "timestamp",
    ):
        assert key in decision
    assert decision["action"] in ("scale_up", "scale_down", "hold")
    # The forecast was stored and a prediction was remembered for residual scoring.
    assert orch.forecast is not None
    assert orch._last_predicted == orch.forecast["predicted"]
