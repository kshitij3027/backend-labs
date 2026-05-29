"""The adaptive batching control loop — one :meth:`AdaptiveBatcher.tick` at a time.

:class:`AdaptiveBatcher` is the heart of the system. It wires together the seven
independent components built in earlier commits into a single, synchronous
control-loop iteration:

* :class:`~src.loadsim.LoadSimulator` — how many messages "arrived" this interval.
* :class:`~src.processor.BatchProcessor` — the concave cost model: given a batch
  size, returns throughput, latency, and *simulated* CPU/memory pressure.
* :class:`~src.metrics.ResourceMonitor` — a real, non-blocking ``psutil`` sample
  of the host.
* :class:`~src.metrics.MetricsCollector` — the rolling time-series buffer.
* :class:`~src.optimizer.OptimizationEngine` — the gradient-ascent hill-climber
  that proposes the next batch size.
* :class:`~src.constraints.ConstraintHandler` — hard safety limits and recovery
  hysteresis.
* :class:`~src.states.StateMachine` — the explicit LEARNING/OPTIMIZING/STABLE/
  EMERGENCY operating state.

Every component is *injectable* through the constructor so tests can substitute
deterministic stubs (a seeded RNG processor, a fixed resource monitor, etc.) and
drive :meth:`tick` with an explicit ``timestamp`` and ``interval``. Nothing here
sleeps, opens a socket, or stresses the host — the async loop that calls
:meth:`tick` on a timer lives in a later commit (C7).

Resource-blend design — why CPU/memory are *mostly simulated*
-------------------------------------------------------------
The control loop must be able to drive itself into ``EMERGENCY`` *reproducibly*
and *without ever loading the real machine*, yet the spec also wants ``psutil``
to be genuinely exercised. We reconcile those by blending the two sources, with
the **simulated workload pressure dominating** and only a small slice of the real
host reading mixed in::

    cpu_percent    = min(100.0, res.cpu_pressure + host_metric_weight * real.cpu_percent)
    memory_percent = min(100.0, res.mem_pressure + host_metric_weight * real.memory_percent)
    memory_available_mb = real.memory_available_mb   # taken straight from psutil

Here ``res`` is the :class:`~src.processor.ProcessResult` (simulated pressure that
climbs with batch size / incoming rate) and ``real`` is the live
:class:`~src.metrics.ResourceReading`. With the default ``host_metric_weight=0.15``
even a fully pegged host (100%) contributes at most 15 points, so emergencies
fire on *large batches / traffic bursts* (which push the simulated pressure past
the ~90% thresholds) rather than on incidental host noise. The
``memory_available_mb`` figure is the one value reported verbatim from psutil, so
the dashboard still shows a real number.

State-driven batch decision
---------------------------
The :class:`StateMachine` decides *how* the next batch size is chosen each tick:

* ``EMERGENCY`` — ignore the optimizer's climb and slam the batch down via
  :meth:`ConstraintHandler.emergency_batch_size`; the reason is the breach text.
* ``STABLE`` — hold the current batch (do **not** step the optimizer, so we stop
  probing once settled); the gradient is whatever the optimizer last reported.
* ``LEARNING`` / ``OPTIMIZING`` — take a real :meth:`OptimizationEngine.update`
  gradient-ascent step toward the throughput/latency optimum.
"""

from __future__ import annotations

import time
from collections import deque

from src.constraints import ConstraintHandler
from src.loadsim import LoadSimulator
from src.metrics import MetricsCollector, ResourceMonitor
from src.models import (
    DecisionRecord,
    LoadConfig,
    MetricSnapshot,
    OptimizerConfigUpdate,
    OptimizerState,
    OptimizerStatus,
)
from src.optimizer import OptimizationEngine
from src.processor import BatchProcessor
from src.settings import get_settings
from src.states import StateMachine


class AdaptiveBatcher:
    """Tie the seven control-loop components into one synchronous :meth:`tick`.

    Each collaborating component is injectable (defaulting to a fresh instance
    built from settings) so the loop can be driven deterministically under test.
    The batcher owns only the cross-component glue: the running queue/backlog
    estimate, a bounded history of :class:`~src.models.DecisionRecord`, and the
    last human-readable reason / constraint flag surfaced to the API layer.

    Args:
        processor: Cost-model batch processor. Defaults to a fresh
            :class:`~src.processor.BatchProcessor`.
        load_simulator: Synthetic arrival generator. Defaults to a fresh
            :class:`~src.loadsim.LoadSimulator`.
        optimizer: Gradient-ascent controller. Defaults to a fresh
            :class:`~src.optimizer.OptimizationEngine`.
        constraints: Hard-limit / recovery handler. Defaults to a fresh
            :class:`~src.constraints.ConstraintHandler`.
        state_machine: Operating-state machine. Defaults to a fresh
            :class:`~src.states.StateMachine`.
        resource_monitor: Real psutil sampler. Defaults to a fresh
            :class:`~src.metrics.ResourceMonitor`.
        collector: Rolling metrics buffer. Defaults to a fresh
            :class:`~src.metrics.MetricsCollector`.
        host_metric_weight: Fraction of the *real* psutil CPU/memory reading
            blended on top of the simulated workload pressure (see module
            docstring). ``0.15`` keeps host noise from triggering emergencies.
        stable_batch_rel_threshold: Relative spread ``(max-min)/mean`` of the
            recent batch sizes below which the loop is considered STABLE.
        stable_window: Number of most-recent recorded batch sizes inspected by
            :meth:`_is_stable`.
        history_size: ``maxlen`` of the decision history deque; defaults to
            ``settings.metrics_history_size``.
    """

    def __init__(
        self,
        *,
        processor: BatchProcessor | None = None,
        load_simulator: LoadSimulator | None = None,
        optimizer: OptimizationEngine | None = None,
        constraints: ConstraintHandler | None = None,
        state_machine: StateMachine | None = None,
        resource_monitor: ResourceMonitor | None = None,
        collector: MetricsCollector | None = None,
        host_metric_weight: float = 0.15,
        stable_batch_rel_threshold: float = 0.05,
        stable_window: int = 5,
        history_size: int | None = None,
    ) -> None:
        settings = get_settings()

        # --- Injectable collaborators (fresh defaults from settings) ---
        self.processor: BatchProcessor = processor or BatchProcessor()
        self.load_simulator: LoadSimulator = load_simulator or LoadSimulator()
        self.optimizer: OptimizationEngine = optimizer or OptimizationEngine()
        self.constraints: ConstraintHandler = constraints or ConstraintHandler()
        self.state_machine: StateMachine = state_machine or StateMachine()
        self.resource_monitor: ResourceMonitor = resource_monitor or ResourceMonitor()
        self.collector: MetricsCollector = collector or MetricsCollector()

        # --- Blend / stability tuning ---
        self.host_metric_weight = host_metric_weight
        self.stable_batch_rel_threshold = stable_batch_rel_threshold
        self.stable_window = stable_window

        # --- Cross-component glue state ---
        self._history: deque[DecisionRecord] = deque(
            maxlen=history_size or settings.metrics_history_size
        )
        self._queue_depth: int = 0
        self._last_reason: str = ""
        self._constraint_active: bool = False
        # Optional interval override set by apply_config; tick() prefers it when
        # no explicit interval argument is supplied.
        self._interval_override: float | None = None

    # ------------------------------------------------------------------
    # Control loop
    # ------------------------------------------------------------------
    def tick(
        self, *, timestamp: float | None = None, interval: float | None = None
    ) -> MetricSnapshot:
        """Run one control-loop iteration and return the snapshot it produced.

        The single iteration: pull synthetic load, process the *current* batch
        through the cost model, blend simulated + real resource readings, update
        the running backlog estimate, evaluate hard constraints and recovery
        hysteresis, advance the state machine, and then choose the next batch
        size in a way dictated by the resulting state (see the module docstring's
        "State-driven batch decision"). The returned
        :class:`~src.models.MetricSnapshot` describes the batch that was *just
        measured* (its ``batch_size`` is the size that produced the metrics, not
        the size chosen for next tick).

        Args:
            timestamp: Epoch seconds to stamp on the snapshot/decision. Defaults
                to :func:`time.time`. Supplying it (including ``0.0``) makes the
                tick fully deterministic for tests.
            interval: Length of the simulated interval in seconds. Defaults to
                ``self._interval_override`` (from :meth:`apply_config`) when set,
                otherwise ``settings.optimization_interval``.

        Returns:
            The :class:`~src.models.MetricSnapshot` recorded this tick.
        """
        if interval is None:
            interval = self._interval_override or get_settings().optimization_interval
        if timestamp is None:
            timestamp = time.time()

        # 1) Synthetic arrivals for this interval (burst-aware).
        incoming = self.load_simulator.messages_for_interval(interval)

        # 2) The batch size currently in effect — this is what we measure.
        current_batch = self.optimizer.batch_size

        # 3) Run the cost model at the current batch and current arrival rate.
        res = self.processor.process_batch(
            current_batch, messages_per_second=self.load_simulator.current_rate()
        )

        # 4) Blend simulated workload pressure (dominant) with a small slice of
        #    the real psutil reading. See the module docstring for the rationale.
        real = self.resource_monitor.sample()
        cpu_percent = min(
            100.0, res.cpu_pressure + self.host_metric_weight * real.cpu_percent
        )
        memory_percent = min(
            100.0, res.mem_pressure + self.host_metric_weight * real.memory_percent
        )
        latency_ms = res.latency_ms
        throughput = res.throughput

        # 5) Update the running queue/backlog estimate: arrivals minus the
        #    records we could drain this interval, floored at zero.
        capacity = throughput * interval
        self._queue_depth = max(
            0, int(round(self._queue_depth + incoming - capacity))
        )
        self.collector.set_queue_depth(self._queue_depth)

        # 6) Evaluate hard constraints, then feed the recovery hysteresis.
        status = self.constraints.check(cpu_percent, memory_percent, latency_ms)
        breach = status.breach
        self._constraint_active = breach
        self.constraints.note_cycle(cpu_percent, memory_percent, latency_ms)
        recovery_ready = self.constraints.recovery_ready()

        # 7) Decide whether the recent batch sizes have settled.
        stable = self._is_stable()

        # 8) Advance the operating state from the per-tick signals.
        new_state = self.state_machine.update(
            breach=breach, recovery_ready=recovery_ready, stable=stable
        )

        # 9) Choose the next batch size in a state-dependent way.
        if new_state is OptimizerState.EMERGENCY:
            new_batch = self.constraints.emergency_batch_size(current_batch)
            self.optimizer.set_batch_size(new_batch)
            gradient = self.optimizer.last_gradient
            reason = status.reason
        elif new_state is OptimizerState.STABLE:
            # Hold at the optimum: do not step the optimizer (stop probing).
            new_batch = current_batch
            gradient = self.optimizer.last_gradient
            reason = "stable: holding at optimum"
        else:  # LEARNING or OPTIMIZING — take a real gradient-ascent step.
            step = self.optimizer.update(throughput, latency_ms)
            new_batch = step.new_batch_size
            gradient = step.gradient
            reason = f"{new_state.value}: gradient={gradient:.4g}"
        self._last_reason = reason

        # 10) Record the measured snapshot (batch_size = the size that produced
        #     these metrics, i.e. current_batch, not the next-tick choice).
        snap = self.collector.record_metrics(
            timestamp=timestamp,
            batch_size=current_batch,
            throughput=throughput,
            latency_ms=latency_ms,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_available_mb=real.memory_available_mb,
            queue_depth=self._queue_depth,
        )

        # 11) Log the decision for the dashboard's history view.
        self._history.append(
            DecisionRecord(
                timestamp=timestamp,
                old_batch_size=current_batch,
                new_batch_size=new_batch,
                gradient=gradient,
                state=new_state,
                reason=reason,
            )
        )

        return snap

    def _is_stable(self) -> bool:
        """Return whether the recent batch sizes have settled into a tight band.

        Looks at the last :attr:`stable_window` *recorded* batch sizes and
        computes their relative spread ``(max - min) / mean``. The loop is
        considered STABLE when that spread is below
        :attr:`stable_batch_rel_threshold`. Using a relative spread over recent
        batch sizes (rather than a raw gradient magnitude) is scale-free and
        robust: it behaves the same whether the optimum sits at 50 or 5000.

        Returns:
            ``False`` if fewer than ``stable_window`` snapshots exist yet or the
            mean batch size is zero; otherwise ``spread < stable_batch_rel_threshold``.
        """
        window = self.collector.snapshot(self.stable_window)
        if len(window) < self.stable_window:
            return False
        sizes = [s.batch_size for s in window]
        mean = sum(sizes) / len(sizes)
        if mean == 0:
            return False
        spread = (max(sizes) - min(sizes)) / mean
        return spread < self.stable_batch_rel_threshold

    # ------------------------------------------------------------------
    # Accessors / control
    # ------------------------------------------------------------------
    def status(self) -> OptimizerStatus:
        """Return the current optimizer status for the API / WebSocket stream."""
        return OptimizerStatus(
            state=self.state_machine.state,
            batch_size=self.optimizer.batch_size,
            last_gradient=self.optimizer.last_gradient,
            smoothing_alpha=self.optimizer.smoothing_alpha,
            min_batch_size=self.optimizer.min_batch_size,
            max_batch_size=self.optimizer.max_batch_size,
            constraint_active=self._constraint_active,
            reason=self._last_reason,
        )

    def metrics_series(self, last_n: int | None = None) -> dict:
        """Return chartable parallel series for the last ``last_n`` snapshots.

        Extends :meth:`MetricsCollector.to_series` with a ``queue_depth`` list
        (built from the same window) and the current operating ``state`` so the
        dashboard can render everything from a single payload.
        """
        window = self.collector.snapshot(last_n)
        series = self.collector.to_series(last_n)
        series["queue_depth"] = [s.queue_depth for s in window]
        series["state"] = self.state_machine.state.value
        return series

    def latest_snapshot(self) -> MetricSnapshot | None:
        """Return the most recent recorded snapshot, or ``None`` if none yet."""
        return self.collector.latest()

    def decision_history(self, last_n: int | None = None) -> list[DecisionRecord]:
        """Return recent decision records, oldest→newest.

        Args:
            last_n: Number of most-recent records to return; ``None`` returns all.
        """
        items = list(self._history)
        if last_n is None or last_n >= len(items):
            return items
        return items[-last_n:] if last_n > 0 else []

    def set_load(self, config: LoadConfig) -> None:
        """Retarget the live synthetic traffic from a :class:`LoadConfig`."""
        self.load_simulator.set_rate(
            config.messages_per_second, config.burst_probability
        )

    def apply_config(self, update: OptimizerConfigUpdate) -> None:
        """Apply a partial reconfiguration across the optimizer and safety layer.

        The optimizer applies the fields it owns (alpha, bounds, probe factors,
        objective weights) via :meth:`OptimizationEngine.apply_config`. Any
        supplied constraint thresholds are pushed onto the
        :class:`ConstraintHandler`, and an ``optimization_interval`` override is
        remembered so :meth:`tick` honours it when called without an explicit
        ``interval``.

        Args:
            update: Patch object whose non-``None`` fields are applied in place.
        """
        self.optimizer.apply_config(update)

        if update.cpu_constraint_threshold is not None:
            self.constraints.cpu_threshold = update.cpu_constraint_threshold
        if update.memory_constraint_threshold is not None:
            self.constraints.memory_threshold = update.memory_constraint_threshold
        if update.latency_constraint_threshold is not None:
            self.constraints.latency_threshold = update.latency_constraint_threshold

        if update.optimization_interval is not None:
            self._interval_override = update.optimization_interval

    def reset(self) -> None:
        """Reset every component and clear the batcher's own glue state."""
        self.optimizer.reset()
        self.state_machine.reset()
        self.constraints.reset()
        self.collector.clear()
        self._history.clear()
        self._queue_depth = 0
        self._constraint_active = False
        self._last_reason = ""
