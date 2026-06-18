"""Orchestrator — the control plane that wires every component together.

This module owns the *system state* and runs the system's **two control loops**:

* **Collector loop** (:meth:`Orchestrator.collector_tick`) — the fast, reactive
  loop. Each tick it advances the workload/capacity simulation by the elapsed
  interval (:meth:`WorkerPool.observe`), samples a canonical metric snapshot
  (:class:`MetricCollector`), appends it to the :class:`RollingHistory`, and
  records it as ``current_metrics``. It is a *reader plus simulation stepper*: it
  never makes scaling decisions.
* **Orchestration loop** (:meth:`Orchestrator.orchestration_tick`) — the slower,
  deliberative loop. Each tick it pulls the recent utilization series, builds a
  short-horizon forecast (:func:`build_forecast`), asks the :class:`Scaler` for a
  decision, and — if the decision is not a hold — actually scales the worker pool
  and records the action in ``scaling_history``.

The two loops share state through a single green-thread-safe lock so the dashboard
(API + SocketIO) can read a consistent :meth:`Orchestrator.snapshot` at any moment.

**Residual tracking.** The collector remembers the utilization the *previous*
orchestration tick predicted (``_last_predicted``) and, on the next sample, records
the absolute error against the observed value into ``_recent_residuals``. Those
recent errors are fed back into :func:`build_forecast`, so the forecast's reported
``confidence`` improves (or degrades) as the predictor's recent track record
becomes known — confidence is earned from measured accuracy, not assumed.

The lock is a plain :class:`threading.Lock`. Under eventlet (used by the dashboard's
SocketIO server) the standard library is monkey-patched, so this is automatically a
cooperative *green* lock rather than an OS mutex; nothing eventlet-specific is
imported here, keeping this module usable with or without eventlet.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional

from src.config import Settings
from src.forecast import build_forecast
from src.load_model import LoadModel
from src.metrics import MetricCollector, RollingHistory
from src.patterns import AnomalyDetector, PatternLearner
from src.scaler import Scaler
from src.workers import create_worker_pool

__all__ = ["Orchestrator", "SystemState"]

# How many history points to feed the forecaster. At a 5s interval this is ~10
# minutes of signal — plenty for Holt's level/trend without dragging in stale data.
_FORECAST_POINTS = 120

# The forecaster targets effective utilization; it is the single metric the scaler
# keys its predictive branch on, so it is the only series we need to project.
_FORECAST_METRIC = "effective_utilization"

# Bounded depths for the rolling state windows.
_SCALING_HISTORY_MAXLEN = 50
_RESIDUALS_MAXLEN = 20


@dataclass
class SystemState:
    """Mutable, shared system state guarded by :attr:`Orchestrator._lock`.

    Grouping the state in one dataclass keeps the lock's invariant obvious: every
    field here is read/written only while holding the orchestrator's lock.

    Attributes:
        current_metrics: The most recent metric snapshot, or ``None`` before the
            first :meth:`Orchestrator.collector_tick`.
        forecast: The most recent forecast payload, or ``None`` before the first
            :meth:`Orchestrator.orchestration_tick`.
        last_decision: The most recent scaling decision (hold or otherwise).
        scaling_history: Bounded log of decisions that actually moved the pool
            (plus manual scales), newest last.
        last_action_ts: Wall-clock timestamp of the last pool-moving action; ``0.0``
            means "no action yet" and disables the cooldown countdown.
        anomaly: The most recent anomaly-detector result, shaped
            ``{"active": bool, "zscore": float}``. Holds the neutral placeholder
            until the first :meth:`Orchestrator.orchestration_tick` populates it.
        _recent_residuals: Recent absolute forecast errors feeding confidence.
        _last_collect_ts: Timestamp of the previous collector tick (for ``dt``).
        _last_predicted: Utilization the previous orchestration tick predicted,
            compared against the next observation to produce a residual.
    """

    current_metrics: Optional[dict] = None
    forecast: Optional[dict] = None
    last_decision: Optional[dict] = None
    scaling_history: Deque[dict] = field(
        default_factory=lambda: deque(maxlen=_SCALING_HISTORY_MAXLEN)
    )
    last_action_ts: float = 0.0
    anomaly: dict = field(default_factory=lambda: {"active": False, "zscore": 0.0})
    _recent_residuals: Deque[float] = field(
        default_factory=lambda: deque(maxlen=_RESIDUALS_MAXLEN)
    )
    _last_collect_ts: Optional[float] = None
    _last_predicted: Optional[float] = None


class Orchestrator:
    """Builds and owns every component, and drives the two control loops.

    Construction wires the full pipeline from a single :class:`Settings`:

        load_model -> worker_pool -> metric_collector -> rolling_history -> scaler

    The orchestrator exposes a small, deterministic API (each tick accepts an
    explicit ``now`` so tests can drive simulated time without sleeping):

    * :meth:`collector_tick` — advance the simulation and sample metrics.
    * :meth:`orchestration_tick` — forecast, decide, and scale.
    * :meth:`request_manual_scale` — operator override (bypasses cooldown).
    * :meth:`snapshot` — the canonical status payload for the API/SocketIO layer.
    """

    def __init__(self, config: Settings) -> None:
        """Construct and wire all components from ``config``.

        Args:
            config: Fully-populated :class:`Settings` controlling intervals,
                thresholds, worker bounds, forecast parameters and the workload
                model. No I/O is performed here beyond ``MetricCollector`` priming
                psutil's CPU counters.
        """
        self.config = config

        # Demand (load_model) and capacity (pool) are the two halves the collector
        # samples; the history feeds the forecaster; the scaler turns forecast +
        # metrics into decisions. None of these own each other's lifecycle.
        self.load_model = LoadModel(config.base_arrival_rate)
        self.pool = create_worker_pool(config)
        self.collector = MetricCollector(config, self.load_model, self.pool)
        self.history = RollingHistory(
            config.history_window_minutes, config.metrics_retention_hours
        )
        self.scaler = Scaler(config)

        # Adaptive analysis layers (both pure, stateless w.r.t. shared state):
        #  * the anomaly detector flags sudden utilization spikes the threshold /
        #    forecast logic may not have caught yet (optional scale-up trigger);
        #  * the pattern learner accumulates a time-of-day profile so the forecast
        #    can be pre-positioned for the level a recurring hour is known to need.
        self.anomaly_detector = AnomalyDetector()
        self.pattern_learner = PatternLearner()

        # All mutable, observable state lives behind one lock.
        self.state = SystemState()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Convenience accessors (read shared state under the lock)
    # ------------------------------------------------------------------ #
    # These mirror the dotted spec names (``orch.current_metrics`` etc.) so callers
    # and tests can read state directly while the canonical storage stays in
    # ``self.state``. Each takes the lock for a consistent read.

    @property
    def current_metrics(self) -> Optional[dict]:
        """The latest metric snapshot (``None`` before the first collector tick)."""
        with self._lock:
            return self.state.current_metrics

    @property
    def forecast(self) -> Optional[dict]:
        """The latest forecast payload (``None`` before the first orchestration tick)."""
        with self._lock:
            return self.state.forecast

    @property
    def last_decision(self) -> Optional[dict]:
        """The most recent scaling decision, hold or otherwise."""
        with self._lock:
            return self.state.last_decision

    @property
    def scaling_history(self) -> Deque[dict]:
        """The bounded deque of pool-moving decisions (newest last)."""
        with self._lock:
            return self.state.scaling_history

    @property
    def last_action_ts(self) -> float:
        """Timestamp of the last pool-moving action (``0.0`` if none yet)."""
        with self._lock:
            return self.state.last_action_ts

    # Internal residual/state accessors exposed for tests and introspection.
    @property
    def _recent_residuals(self) -> Deque[float]:
        with self._lock:
            return self.state._recent_residuals

    @property
    def _last_collect_ts(self) -> Optional[float]:
        with self._lock:
            return self.state._last_collect_ts

    @property
    def _last_predicted(self) -> Optional[float]:
        with self._lock:
            return self.state._last_predicted

    # ------------------------------------------------------------------ #
    # Collector loop
    # ------------------------------------------------------------------ #
    def collector_tick(self, now: Optional[float] = None) -> dict:
        """Advance the simulation by one interval and sample a metric snapshot.

        Steps:
            1. Read the current arrival rate from the load model.
            2. Compute ``dt`` since the previous tick (falling back to the
               configured monitoring interval on the first tick or if the clock
               does not advance), then advance the worker-pool simulation.
            3. Sample a canonical snapshot, append it to the rolling history, and
               record it as ``current_metrics``.
            4. If a previous orchestration tick made a prediction, record the
               absolute forecast error as a residual (feedback for confidence).

        Args:
            now: Optional wall-clock timestamp (seconds). Defaults to
                :func:`time.time`. Injectable so tests can drive simulated time.

        Returns:
            The metric snapshot dict that was just collected.
        """
        if now is None:
            now = time.time()

        # The arrival rate is read OUTSIDE the lock (load_model is independent of
        # shared state); only the brief state read/write below is serialized.
        arrival = float(self.load_model.arrival_rate(now))

        with self._lock:
            last_collect = self.state._last_collect_ts
            # Elapsed time since the previous tick. On the first tick (or if the
            # supplied clock did not advance) fall back to the nominal interval so
            # the simulation still makes forward progress.
            if last_collect is not None:
                dt = now - last_collect
            else:
                dt = self.config.monitoring_interval_seconds
            if dt <= 0:
                dt = self.config.monitoring_interval_seconds
            self.state._last_collect_ts = now

            # Advance the queue simulation, then sample the resulting state. Both the
            # pool step and the sample happen under the lock so a concurrent reader
            # never sees the pool advanced but the snapshot not yet stored.
            self.pool.observe(arrival, dt)
            snap = self.collector.sample(now)
            self.history.add(snap)
            self.state.current_metrics = snap

            # Feed the time-of-day pattern learner with the observed utilization so
            # the next orchestration tick can pre-position capacity for this hour.
            self.pattern_learner.observe(
                now, float(snap.get("effective_utilization", 0.0) or 0.0)
            )

            # Residual tracking: compare the previous prediction against what we just
            # observed. abs() because confidence cares about error magnitude only.
            if self.state._last_predicted is not None:
                observed = float(snap.get("effective_utilization", 0.0) or 0.0)
                residual = abs(self.state._last_predicted - observed)
                self.state._recent_residuals.append(residual)

        return snap

    # ------------------------------------------------------------------ #
    # Orchestration loop
    # ------------------------------------------------------------------ #
    def orchestration_tick(self, now: Optional[float] = None) -> dict:
        """Forecast, decide, and (if warranted) scale the worker pool.

        Steps:
            1. Pull the recent utilization series and build a short-horizon forecast
               (the forecaster degrades gracefully on short series).
            2. Ask the scaler for a decision given the latest metrics, the forecast,
               the current worker count and the last-action timestamp.
            3. If the decision is not ``"hold"``, scale the pool to the target,
               stamp ``last_action_ts``, and append the decision to the history.
            4. Always record the decision as ``last_decision``.

        Args:
            now: Optional wall-clock timestamp (seconds). Defaults to
                :func:`time.time`. Injectable so tests can drive simulated time.

        Returns:
            The canonical scaling-decision dict produced this tick.
        """
        if now is None:
            now = time.time()

        # Horizon in steps = horizon (minutes) / monitoring interval (seconds).
        # At least one step so a sub-interval horizon still projects forward once.
        interval = self.config.monitoring_interval_seconds
        horizon_steps = max(1, round(self.config.horizon_minutes * 60.0 / interval))

        with self._lock:
            # Snapshot the inputs we need under the lock so the forecast/decision are
            # computed against a consistent view of state.
            series = self.history.series(_FORECAST_METRIC, points=_FORECAST_POINTS)
            recent_residuals = list(self.state._recent_residuals)
            snapshot_for_decision = self.state.current_metrics or {}

            # build_forecast handles empty/short series defensively; no guard needed.
            fc = build_forecast(
                series,
                horizon_steps,
                metric=_FORECAST_METRIC,
                horizon_minutes=self.config.horizon_minutes,
                alpha=self.config.forecast_alpha,
                beta=self.config.forecast_beta,
                recent_residuals=recent_residuals,
            )

            # Pre-positioning: nudge the forecast by the learned time-of-day factor
            # so we provision for the level this hour is historically known to need
            # (factor > 1 -> hotter hour -> higher predicted; < 1 -> cooler). The
            # rest of the forecast payload is left intact.
            factor = self.pattern_learner.seasonality_factor(now)
            fc["seasonality_factor"] = round(factor, 3)
            fc["predicted"] = round(fc["predicted"] * factor, 2)

            self.state.forecast = fc
            # Remember this (pre-positioned) prediction so the NEXT collector tick
            # can score it against the observed value as a residual.
            self.state._last_predicted = fc["predicted"]

            # Detect a sudden spike in recent utilization (reusing the same recent
            # series already pulled for the forecast). The result is stored so
            # snapshot() surfaces the real anomaly state, and is passed to the
            # scaler as an optional (lowest-precedence) scale-up trigger.
            anomaly = self.anomaly_detector.detect(series)
            self.state.anomaly = anomaly

            current = int(self.pool.current())
            decision = self.scaler.decide(
                snapshot_for_decision,
                fc,
                current,
                self.state.last_action_ts,
                now,
                anomaly=anomaly,
            )

            # Only an actionable decision moves the pool and resets the cooldown clock.
            if decision["action"] != "hold":
                self.pool.scale_to(int(decision["to_workers"]))
                self.state.last_action_ts = now
                self.state.scaling_history.append(decision)

            self.state.last_decision = decision

        return decision

    # ------------------------------------------------------------------ #
    # Manual override
    # ------------------------------------------------------------------ #
    def request_manual_scale(
        self,
        direction: Optional[str] = None,
        target: Optional[int] = None,
        now: Optional[float] = None,
    ) -> dict:
        """Apply an operator-requested scale, bypassing thresholds and cooldown.

        Resolution of the desired worker count:
            * ``target`` provided -> use it (clamped to ``[min_workers, max_workers]``).
            * else ``direction == "up"`` -> current + 1.
            * else ``direction == "down"`` -> current - 1.
            * else (neither) -> no change.

        The pool is scaled immediately and a decision dict with ``reason="manual"``
        is recorded. Manual scaling deliberately ignores the cooldown so an operator
        is never blocked by anti-thrash damping.

        Args:
            direction: ``"up"`` or ``"down"`` (ignored if ``target`` is given).
            target: Explicit desired worker count (takes precedence over direction).
            now: Optional wall-clock timestamp (seconds). Defaults to
                :func:`time.time`.

        Returns:
            The canonical scaling-decision dict describing the manual action. Its
            ``action`` is ``"scale_up"``, ``"scale_down"``, or ``"hold"`` (when the
            resolved target equals the current count).
        """
        if now is None:
            now = time.time()

        with self._lock:
            current = int(self.pool.current())

            if target is not None:
                desired = int(target)
            elif direction == "up":
                desired = current + 1
            elif direction == "down":
                desired = current - 1
            else:
                desired = current

            # Clamp into the operating band so a manual request can never violate
            # the configured min/max worker bounds.
            desired = max(int(self.config.min_workers), min(int(self.config.max_workers), desired))

            if desired > current:
                action = "scale_up"
            elif desired < current:
                action = "scale_down"
            else:
                action = "hold"

            self.pool.scale_to(desired)

            decision = {
                "action": action,
                "reason": "manual",
                "from_workers": current,
                "to_workers": desired,
                "trigger_metric": "manual",
                "trigger_value": float(desired),
                "confidence": None,
                "cooldown_active": False,
                "timestamp": float(now),
            }

            # A manual scale is a real action: update the cooldown clock and log it,
            # even when it is a no-op hold (so the history reflects operator intent).
            self.state.last_action_ts = now
            self.state.last_decision = decision
            self.state.scaling_history.append(decision)

        return decision

    # ------------------------------------------------------------------ #
    # Status payload
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        """Return the canonical status payload — the single source of truth.

        This is the exact object the HTTP API and the SocketIO broadcast serialize.
        It is assembled under the lock so every field reflects one consistent moment
        of system state.

        Returns:
            A dict with keys ``timestamp``, ``current_metrics``, ``forecast``,
            ``workers`` (``current``/``min``/``max``/``backend``), ``last_decision``,
            ``cooldown_remaining_s``, ``scaling_history``, ``anomaly`` and ``cost``.
            ``anomaly`` reflects the detector's latest result (neutral default
            before the first orchestration tick); ``cost`` is a placeholder
            populated by a later commit (cost reporting).
        """
        now = time.time()
        cfg = self.config

        with self._lock:
            last_action_ts = self.state.last_action_ts
            # Remaining cooldown is meaningful only after a real action; before the
            # first action (ts == 0.0) we report 0 rather than a huge bogus number.
            if last_action_ts:
                remaining = cfg.cooldown_period_seconds - (now - last_action_ts)
                cooldown_remaining_s = int(max(0, remaining))
            else:
                cooldown_remaining_s = 0

            return {
                "timestamp": now,
                "current_metrics": self.state.current_metrics or {},
                "forecast": self.state.forecast or {},
                "workers": {
                    "current": self.pool.current(),
                    "min": cfg.min_workers,
                    "max": cfg.max_workers,
                    "backend": self.pool.backend,
                },
                "last_decision": self.state.last_decision or {},
                "cooldown_remaining_s": cooldown_remaining_s,
                "scaling_history": list(self.state.scaling_history),
                # The detector's latest result; the neutral default until the first
                # orchestration tick runs (see SystemState.anomaly).
                "anomaly": dict(self.state.anomaly),
                # Placeholder; populated in a later commit (cost reporting).
                "cost": {},
            }
