"""Unit tests for src.batcher — the AdaptiveBatcher control loop.

Every test is deterministic. The real :class:`~src.metrics.ResourceMonitor`
samples the live host, so all tests inject a :class:`FakeMonitor` returning
fixed host values; processing is made deterministic with ``noise_std=0.0`` and a
seeded :class:`LoadSimulator`. Where a specific control-loop branch must be
isolated, a small stub component (processor / state machine) is injected so the
branch fires regardless of the cost-model dynamics.

The blend, queue, EMERGENCY/STABLE/OPTIMIZING decision, config/load/reset
behaviour, and status accessor are each asserted in isolation here; end-to-end
convergence over many ticks lives in ``tests/integration/test_pipeline.py``.
"""

from __future__ import annotations

import random

import pytest

from src.batcher import AdaptiveBatcher
from src.constraints import ConstraintHandler
from src.loadsim import LoadSimulator
from src.metrics import MetricsCollector, ResourceReading
from src.models import (
    LoadConfig,
    MetricSnapshot,
    OptimizerConfigUpdate,
    OptimizerState,
    OptimizerStatus,
)
from src.optimizer import OptimizationEngine
from src.processor import BatchProcessor, ProcessResult
from src.states import StateMachine


# --- Deterministic test doubles --------------------------------------------


class FakeMonitor:
    """Resource monitor stub returning a fixed :class:`ResourceReading`.

    Used in place of the real psutil-backed :class:`ResourceMonitor` so the
    blended CPU/memory figures depend only on the (simulated) workload pressure.
    """

    def __init__(self, cpu: float = 5.0, mem: float = 40.0, avail: float = 8000.0) -> None:
        self._r = ResourceReading(cpu, mem, avail)

    def sample(self) -> ResourceReading:
        return self._r


class StubProcessor:
    """Processor stub returning a fixed :class:`ProcessResult` every call.

    The ``batch_size`` field of the result is overwritten with whatever batch the
    loop asks for, mirroring the real processor's contract while keeping
    throughput / latency / pressure constant for exact assertions.
    """

    def __init__(
        self,
        *,
        throughput: float = 10000.0,
        latency_ms: float = 50.0,
        cpu_pressure: float = 40.0,
        mem_pressure: float = 30.0,
    ) -> None:
        self.throughput = throughput
        self.latency_ms = latency_ms
        self.cpu_pressure = cpu_pressure
        self.mem_pressure = mem_pressure

    def process_batch(self, batch_size: int, messages_per_second: float = 0.0) -> ProcessResult:
        return ProcessResult(
            batch_size=batch_size,
            throughput=self.throughput,
            latency_ms=self.latency_ms,
            cpu_pressure=self.cpu_pressure,
            mem_pressure=self.mem_pressure,
        )


class StubStateMachine:
    """State-machine stub that always reports a fixed state from :meth:`update`."""

    def __init__(self, state: OptimizerState) -> None:
        self._state = state
        self.calls: list[dict] = []

    @property
    def state(self) -> OptimizerState:
        return self._state

    def update(self, *, breach: bool, recovery_ready: bool, stable: bool) -> OptimizerState:
        self.calls.append(
            {"breach": breach, "recovery_ready": recovery_ready, "stable": stable}
        )
        return self._state

    def reset(self) -> None:
        pass


class SpyOptimizer:
    """Wraps a real :class:`OptimizationEngine`, counting ``update`` calls.

    Delegates every attribute/method to the wrapped engine so the batcher sees a
    fully functional optimizer, while recording how many times the gradient step
    (:meth:`update`) actually fired.
    """

    def __init__(self, engine: OptimizationEngine) -> None:
        self._engine = engine
        self.update_calls = 0

    def update(self, throughput: float, latency_ms: float):
        self.update_calls += 1
        return self._engine.update(throughput, latency_ms)

    def __getattr__(self, name):
        # Only reached for attributes not set on the spy itself.
        return getattr(self._engine, name)


def _det_simulator(rate: float = 100.0) -> LoadSimulator:
    """A burst-free, seeded load simulator (fully deterministic arrivals)."""
    return LoadSimulator(
        messages_per_second=rate, burst_probability=0.0, rng=random.Random(0)
    )


def _det_batcher(**overrides) -> AdaptiveBatcher:
    """Build an AdaptiveBatcher with all-deterministic default components."""
    kwargs = dict(
        processor=BatchProcessor(noise_std=0.0),
        load_simulator=_det_simulator(),
        optimizer=OptimizationEngine(initial_batch_size=100),
        constraints=ConstraintHandler(),
        state_machine=StateMachine(),
        resource_monitor=FakeMonitor(),
        collector=MetricsCollector(),
    )
    kwargs.update(overrides)
    return AdaptiveBatcher(**kwargs)


# --- tick basics ------------------------------------------------------------


def test_tick_returns_snapshot_and_grows_buffers() -> None:
    b = _det_batcher()
    assert len(b.collector) == 0
    assert b.latest_snapshot() is None

    snap = b.tick(timestamp=0.0, interval=1.0)

    assert isinstance(snap, MetricSnapshot)
    assert len(b.collector) == 1
    assert len(b.decision_history()) == 1
    assert b.latest_snapshot() is snap
    assert snap.timestamp == 0.0
    # The snapshot describes the batch that was *measured* this tick (the
    # starting batch), not the next-tick choice.
    assert snap.batch_size == 100

    snap2 = b.tick(timestamp=1.0, interval=1.0)
    assert len(b.collector) == 2
    assert len(b.decision_history()) == 2
    assert b.latest_snapshot() is snap2


# --- resource blend ---------------------------------------------------------


def test_resource_blend_uses_simulated_pressure_plus_15pct_host() -> None:
    """cpu = pressure + 0.15*host_cpu; mem = pressure + 0.15*host_mem; avail verbatim."""
    b = _det_batcher(
        processor=StubProcessor(
            throughput=10000.0, latency_ms=50.0, cpu_pressure=40.0, mem_pressure=30.0
        ),
        resource_monitor=FakeMonitor(cpu=20.0, mem=60.0, avail=8000.0),
    )

    snap = b.tick(timestamp=0.0, interval=1.0)

    assert snap.cpu_percent == pytest.approx(40.0 + 0.15 * 20.0)  # 43.0
    assert snap.memory_percent == pytest.approx(30.0 + 0.15 * 60.0)  # 39.0
    assert snap.memory_available_mb == pytest.approx(8000.0)
    assert snap.throughput == pytest.approx(10000.0)
    assert snap.latency_ms == pytest.approx(50.0)


def test_resource_blend_clamps_to_100() -> None:
    """A pegged simulated pressure plus host slice never exceeds 100."""
    b = _det_batcher(
        processor=StubProcessor(cpu_pressure=100.0, mem_pressure=100.0),
        resource_monitor=FakeMonitor(cpu=100.0, mem=100.0),
    )
    snap = b.tick(timestamp=0.0, interval=1.0)
    assert snap.cpu_percent == 100.0
    assert snap.memory_percent == 100.0


# --- EMERGENCY path ---------------------------------------------------------


def test_emergency_breach_slams_batch_down() -> None:
    """A breaching tick forces EMERGENCY (beats any state) and shrinks the batch."""
    opt = OptimizationEngine(initial_batch_size=1000, min_batch_size=50)
    b = _det_batcher(
        processor=StubProcessor(cpu_pressure=100.0),  # blended cpu == 100 > 90 -> breach
        optimizer=opt,
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )

    before = opt.batch_size
    snap = b.tick(timestamp=0.0, interval=1.0)

    assert b.state_machine.state is OptimizerState.EMERGENCY
    # emergency_batch_size halves (floored at min): 1000 -> 500.
    assert opt.batch_size == 500
    assert opt.batch_size < before
    # The measured snapshot is for the batch that produced the breach (1000).
    assert snap.batch_size == 1000
    # Decision reason is the constraint breach text (cpu over threshold).
    rec = b.decision_history()[-1]
    assert rec.state is OptimizerState.EMERGENCY
    assert "cpu" in rec.reason


def test_emergency_floors_at_min_batch_size() -> None:
    """The emergency reduction never drops below min_batch_size."""
    opt = OptimizationEngine(initial_batch_size=100, min_batch_size=50)
    b = _det_batcher(
        processor=StubProcessor(cpu_pressure=100.0),
        optimizer=opt,
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )
    b.tick(timestamp=0.0, interval=1.0)
    # int(100 * 0.5) == 50 == min -> stays at the floor.
    assert opt.batch_size == 50


# --- STABLE holds -----------------------------------------------------------


def test_stable_holds_batch_and_does_not_step_optimizer() -> None:
    """When the state machine reports STABLE the optimizer.update is NOT called."""
    spy = SpyOptimizer(OptimizationEngine(initial_batch_size=777))
    b = _det_batcher(
        processor=BatchProcessor(noise_std=0.0),
        optimizer=spy,
        state_machine=StubStateMachine(OptimizerState.STABLE),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )

    current = spy.batch_size
    b.tick(timestamp=0.0, interval=1.0)

    assert spy.update_calls == 0  # optimizer was NOT stepped
    assert spy.batch_size == current == 777  # batch held across the tick

    rec = b.decision_history()[-1]
    assert rec.state is OptimizerState.STABLE
    assert rec.reason == "stable: holding at optimum"
    assert rec.old_batch_size == 777
    assert rec.new_batch_size == 777


# --- OPTIMIZING steps -------------------------------------------------------


def test_optimizing_steps_optimizer_and_reasons_optimizing() -> None:
    """When the state machine reports OPTIMIZING the real optimizer.update fires."""
    spy = SpyOptimizer(OptimizationEngine(initial_batch_size=100))
    b = _det_batcher(
        processor=BatchProcessor(noise_std=0.0),
        optimizer=spy,
        state_machine=StubStateMachine(OptimizerState.OPTIMIZING),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )

    b.tick(timestamp=0.0, interval=1.0)

    assert spy.update_calls == 1  # a real gradient step was taken
    rec = b.decision_history()[-1]
    assert rec.state is OptimizerState.OPTIMIZING
    assert rec.reason.startswith("optimizing")


def test_learning_state_also_steps_optimizer() -> None:
    """A fresh batcher starts in LEARNING; the optimizer still takes a step."""
    spy = SpyOptimizer(OptimizationEngine(initial_batch_size=100))
    b = _det_batcher(
        optimizer=spy,
        state_machine=StateMachine(learning_samples=5),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )
    b.tick(timestamp=0.0, interval=1.0)
    assert b.state_machine.state is OptimizerState.LEARNING
    assert spy.update_calls == 1
    rec = b.decision_history()[-1]
    assert rec.reason.startswith("learning")


# --- queue growth vs drain --------------------------------------------------


def test_queue_grows_when_capacity_below_arrivals() -> None:
    """High arrivals + tiny throughput => the backlog estimate climbs each tick."""
    # throughput=1 rec/s, interval=1s => capacity ~= 1; arrivals = 1000/interval.
    b = _det_batcher(
        processor=StubProcessor(throughput=1.0, latency_ms=10.0, cpu_pressure=1.0, mem_pressure=1.0),
        load_simulator=_det_simulator(rate=1000.0),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )

    depths: list[int] = []
    for i in range(4):
        snap = b.tick(timestamp=float(i), interval=1.0)
        depths.append(snap.queue_depth)

    assert depths[0] > 0
    assert depths == sorted(depths)  # monotonically non-decreasing
    assert depths[-1] > depths[0]  # strictly grew overall


def test_queue_stays_zero_when_capacity_exceeds_arrivals() -> None:
    """High throughput drains everything: the backlog never accumulates."""
    b = _det_batcher(
        processor=StubProcessor(throughput=100000.0, latency_ms=10.0, cpu_pressure=1.0, mem_pressure=1.0),
        load_simulator=_det_simulator(rate=100.0),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )
    for i in range(5):
        snap = b.tick(timestamp=float(i), interval=1.0)
        assert snap.queue_depth == 0


# --- apply_config -----------------------------------------------------------


def test_apply_config_updates_optimizer_constraints_and_interval() -> None:
    """apply_config patches optimizer + constraints and remembers the interval."""
    captured: dict = {}

    class RecordingSimulator(LoadSimulator):
        def messages_for_interval(self, interval_seconds: float) -> int:
            captured["interval"] = interval_seconds
            return super().messages_for_interval(interval_seconds)

    sim = RecordingSimulator(
        messages_per_second=100.0, burst_probability=0.0, rng=random.Random(0)
    )
    opt = OptimizationEngine(initial_batch_size=100)
    constraints = ConstraintHandler()
    b = _det_batcher(
        load_simulator=sim,
        optimizer=opt,
        constraints=constraints,
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )

    b.apply_config(
        OptimizerConfigUpdate(
            smoothing_alpha=0.5,
            max_batch_size=400,
            cpu_constraint_threshold=80.0,
            optimization_interval=2.0,
        )
    )

    assert opt.smoothing_alpha == 0.5
    assert opt.max_batch_size == 400
    assert constraints.cpu_threshold == 80.0

    # A tick with no interval arg must now use the 2.0 override.
    b.tick(timestamp=0.0)
    assert captured["interval"] == pytest.approx(2.0)


# --- set_load ---------------------------------------------------------------


def test_set_load_updates_simulator_rate() -> None:
    b = _det_batcher()
    b.set_load(LoadConfig(messages_per_second=250.0, burst_probability=0.0))
    assert b.load_simulator.messages_per_second == pytest.approx(250.0)
    assert b.load_simulator.burst_probability == pytest.approx(0.0)


# --- reset ------------------------------------------------------------------


def test_reset_clears_history_collector_queue_and_state() -> None:
    # Drive a queue backlog + several ticks first.
    b = _det_batcher(
        processor=StubProcessor(throughput=1.0, cpu_pressure=1.0, mem_pressure=1.0),
        load_simulator=_det_simulator(rate=1000.0),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )
    for i in range(6):
        b.tick(timestamp=float(i), interval=1.0)

    assert len(b.collector) > 0
    assert len(b.decision_history()) > 0
    assert b.latest_snapshot().queue_depth > 0
    # 6 ticks (>= learning_samples=5) advanced past LEARNING.
    assert b.state_machine.state is not OptimizerState.LEARNING

    b.reset()

    assert len(b.collector) == 0
    assert len(b.decision_history()) == 0
    assert b.latest_snapshot() is None
    assert b.state_machine.state is OptimizerState.LEARNING
    # Queue depth reset to 0 (verified via the next snapshot's depth math).
    snap = b.tick(timestamp=100.0, interval=1.0)
    # incoming(1000) - capacity(1) => 999, proving the backlog started from 0.
    assert snap.queue_depth == 999


# --- status() ---------------------------------------------------------------


def test_status_reflects_current_state_and_batch() -> None:
    opt = OptimizationEngine(initial_batch_size=100)
    b = _det_batcher(optimizer=opt, resource_monitor=FakeMonitor(cpu=5.0, mem=40.0))

    status = b.status()
    assert isinstance(status, OptimizerStatus)
    assert status.state is OptimizerState.LEARNING
    assert status.batch_size == 100
    assert status.smoothing_alpha == opt.smoothing_alpha
    assert status.min_batch_size == opt.min_batch_size
    assert status.max_batch_size == opt.max_batch_size
    assert status.constraint_active is False

    # After a breaching tick the status mirrors EMERGENCY + active constraint.
    b2 = _det_batcher(
        processor=StubProcessor(cpu_pressure=100.0),
        optimizer=OptimizationEngine(initial_batch_size=1000),
        resource_monitor=FakeMonitor(cpu=5.0, mem=40.0),
    )
    b2.tick(timestamp=0.0, interval=1.0)
    s2 = b2.status()
    assert s2.state is OptimizerState.EMERGENCY
    assert s2.constraint_active is True
    assert s2.batch_size == 500
    assert "cpu" in s2.reason


# --- metrics_series ---------------------------------------------------------


def test_metrics_series_includes_queue_and_state() -> None:
    b = _det_batcher(resource_monitor=FakeMonitor(cpu=5.0, mem=40.0))
    for i in range(3):
        b.tick(timestamp=float(i), interval=1.0)

    series = b.metrics_series()
    assert len(series["throughput"]) == 3
    assert len(series["queue_depth"]) == 3
    assert series["state"] == b.state_machine.state.value
    # last_n windowing is honoured.
    assert len(b.metrics_series(last_n=2)["batch_size"]) == 2
