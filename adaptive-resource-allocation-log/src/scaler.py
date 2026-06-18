"""Autoscaling decision engine for the Adaptive Resource Allocation System.

This module is **pure decision logic** — it depends only on the standard library
(:mod:`math`, :mod:`time`), performs no I/O, and imports nothing else from
:mod:`src`. It consumes the *dict contracts* produced by the metrics collector
and the forecaster (it duck-types them rather than importing the modules):

* **snapshot** (from :mod:`src.metrics`) supplies the reactive signals
  ``cpu_percent``, ``memory_percent`` and ``effective_utilization`` (a percent
  that may exceed 100 when demand outstrips provisioned capacity).
* **forecast** (from :func:`src.forecast.build_forecast`) supplies the predictive
  signal ``predicted`` (forecasted effective utilization at the horizon) and its
  ``confidence`` in ``[0, 1]``.

The single entry point :meth:`Scaler.decide` returns the *canonical scaling
decision* dict (never ``None``) describing the intended action and why.

Design philosophy
------------------
* **Hysteresis.** Separate scale-up and scale-down thresholds create a dead-band
  between them where nothing happens, so the autoscaler does not oscillate around
  a single trip point.
* **Availability-first / asymmetric response.** Scaling *up* is aggressive
  (HPA-style ratio sizing, may add several workers at once) because under-
  provisioning hurts users immediately; scaling *down* is conservative (one
  worker at a time, only when *every* reactive signal is calm, never driven by a
  forecast) because over-provisioning merely costs a little money.
* **Cooldowns** damp thrash: after any action the engine reports a "would act but
  cooling down" hold until the relevant cooldown window has elapsed (scale-down
  is held longer than scale-up).
"""

from __future__ import annotations

import math
import time

__all__ = ["Scaler", "DECISION_KEYS"]


# Canonical scaling-decision keys, declared once so callers/tests can introspect
# the schema without constructing a Scaler.
DECISION_KEYS = (
    "action",
    "reason",
    "from_workers",
    "to_workers",
    "trigger_metric",
    "trigger_value",
    "confidence",
    "cooldown_active",
    "timestamp",
)


class Scaler:
    """Stateless autoscaling decision engine.

    The scaler holds only a reference to ``config`` (a :class:`src.config.Settings`
    or any object exposing the same threshold / bound / cooldown attributes). It
    keeps no mutable state of its own: every decision is a pure function of the
    arguments passed to :meth:`decide`, which makes it trivial to unit-test and
    safe to share across threads.
    """

    def __init__(self, config) -> None:
        self._config = config

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def decide(
        self,
        snapshot: dict,
        forecast: dict,
        current_workers: int,
        last_action_ts: float,
        now: float | None = None,
        anomaly: dict | None = None,
    ) -> dict:
        """Decide whether to scale up, scale down, or hold.

        Args:
            snapshot: Latest metric snapshot. Reads ``cpu_percent``,
                ``memory_percent`` and ``effective_utilization`` (other keys are
                ignored). Missing values default to ``0.0``.
            forecast: Forecast payload. Reads ``predicted`` and ``confidence``
                (defaulting to ``0.0`` each if absent).
            current_workers: The currently provisioned worker count.
            last_action_ts: Wall-clock timestamp (seconds) of the most recent
                scaling action, used for the cooldown check.
            now: Optional wall-clock timestamp (seconds). Defaults to
                :func:`time.time`. Stamped onto the returned decision.
            anomaly: Optional anomaly-detection result (see
                :class:`src.patterns.AnomalyDetector`) shaped
                ``{"active": bool, "zscore": float}``. When provided, an *active*
                anomaly with a *positive* z-score (an upward spike) acts as an
                extra scale-up trigger — but only if neither the reactive nor the
                predictive branch already chose a scale-up (they take precedence).
                Defaults to ``None``, in which case behaviour is exactly as if no
                anomaly signal existed (fully backward-compatible).

        Returns:
            The canonical scaling-decision dict (see :data:`DECISION_KEYS`). The
            ``action`` is always one of ``"scale_up"``, ``"scale_down"`` or
            ``"hold"`` and the result is never ``None`` — a hold always carries a
            ``reason`` explaining itself (``"stable"``, ``"bounds"`` or
            ``"cooldown"``).
        """
        if now is None:
            now = time.time()

        cfg = self._config
        current_workers = int(current_workers)

        # --- Read the input contracts defensively ---
        cpu = _as_float(snapshot.get("cpu_percent"))
        mem = _as_float(snapshot.get("memory_percent"))
        util = _as_float(snapshot.get("effective_utilization"))
        predicted = _as_float(forecast.get("predicted"))
        forecast_conf = _as_float(forecast.get("confidence"))

        # --- 1) Reactive signal -------------------------------------------------
        # Scale-up fires if ANY signal breaches its high threshold; among the
        # breached signals we pick the most-overloaded one (largest value/threshold
        # ratio) so the chosen reason/trigger reflects the worst pressure.
        reactive_up = self._reactive_scale_up(cpu, mem, util)

        # Scale-down requires EVERY signal below its (lower) low threshold. The gap
        # between the up/down thresholds is the hysteresis dead-band.
        reactive_down = (
            cpu < cfg.cpu_threshold_scale_down
            and mem < cfg.memory_threshold_scale_down
            and util < cfg.util_threshold_scale_down
        )

        # --- 2) Predictive signal (scale-UP only, conservative) -----------------
        # Only a sufficiently confident forecast that clears the scale-up threshold
        # is allowed to provoke pre-emptive capacity. We never scale *down* on a
        # forecast — predicting calm is not a reason to remove capacity.
        predictive_up = (
            forecast_conf >= cfg.confidence_threshold
            and predicted > cfg.util_threshold_scale_up
        )

        # --- 2b) Anomaly signal (scale-UP only, lowest precedence) --------------
        # An *active* anomaly with a *positive* z-score is a sudden upward spike the
        # reactive/predictive branches may not have caught yet. It only fires when
        # neither of those already chose a scale-up, so observed/forecast pressure
        # always wins the reason; an anomalous dip (negative z-score) is ignored
        # here — removing capacity is never anomaly-driven.
        anomaly_zscore = _as_float((anomaly or {}).get("zscore"))
        anomaly_up = bool(anomaly) and bool(anomaly.get("active")) and anomaly_zscore > 0.0

        # --- 3) Combine (availability-first) ------------------------------------
        # Any scale-up signal wins over a scale-down opportunity. When reactive and
        # predictive both fire, prefer the reactive reason (observed > forecast);
        # the anomaly trigger sits below both.
        if reactive_up is not None or predictive_up:
            action = "scale_up"
            if reactive_up is not None:
                reason, trigger_metric, trigger_value = reactive_up
                confidence = None  # reactive reasons carry no forecast confidence
            else:
                reason = "predictive"
                trigger_metric = "predicted"
                trigger_value = predicted
                confidence = forecast_conf
        elif anomaly_up:
            # Treat the spike as an aggressive scale-up. The decision surfaces the
            # z-score as the trigger, but the worker target is sized off the SAME
            # util-based HPA path as ``reactive_util`` (see ``_target_up``).
            action = "scale_up"
            reason = "anomaly"
            trigger_metric = "anomaly_zscore"
            trigger_value = anomaly_zscore
            confidence = None
        elif reactive_down:
            action = "scale_down"
            reason = "reactive_util"
            trigger_metric = "effective_utilization"
            trigger_value = util
            confidence = None
        else:
            action = "hold"
            reason = "stable"
            trigger_metric = ""
            trigger_value = 0.0
            confidence = None

        # --- 4) Compute the target worker count ---------------------------------
        if action == "scale_up":
            desired = self._target_up(reason, trigger_value, cpu, mem, util, current_workers)
        elif action == "scale_down":
            # Conservative: step down by exactly one worker.
            desired = current_workers - 1
        else:
            desired = current_workers

        # Clamp into the configured operating band.
        desired = _clamp(desired, int(cfg.min_workers), int(cfg.max_workers))

        # --- 5) Bounds check ----------------------------------------------------
        # If clamping pinned the target back to the current count, the action can't
        # actually move the pool (we're already at a limit) — convert to a hold.
        if action in ("scale_up", "scale_down") and desired == current_workers:
            action = "hold"
            reason = "bounds"
            confidence = None
            return _decision(
                action=action,
                reason=reason,
                from_workers=current_workers,
                to_workers=current_workers,
                trigger_metric=trigger_metric,
                trigger_value=trigger_value,
                confidence=confidence,
                cooldown_active=False,
                timestamp=now,
            )

        # --- 6) Cooldown check (anti-thrash) ------------------------------------
        # Applied AFTER the intended action is known so the dashboard can show
        # "would scale but cooling down". Scale-down is held for longer than
        # scale-up to further damp flapping.
        if action == "scale_up":
            cooldown_window = float(cfg.cooldown_period_seconds)
        elif action == "scale_down":
            cooldown_window = float(cfg.scale_down_cooldown_seconds)
        else:
            cooldown_window = 0.0

        if action in ("scale_up", "scale_down") and (now - last_action_ts) < cooldown_window:
            # Suppress the move but remember what it would have been (reason/trigger
            # are preserved for display); report the cooldown explicitly.
            return _decision(
                action="hold",
                reason="cooldown",
                from_workers=current_workers,
                to_workers=current_workers,
                trigger_metric=trigger_metric,
                trigger_value=trigger_value,
                confidence=confidence,
                cooldown_active=True,
                timestamp=now,
            )

        # --- 7) Final decision (active move, or plain stable hold) --------------
        to_workers = desired if action in ("scale_up", "scale_down") else current_workers
        return _decision(
            action=action,
            reason=reason,
            from_workers=current_workers,
            to_workers=to_workers,
            trigger_metric=trigger_metric,
            trigger_value=trigger_value,
            confidence=confidence,
            cooldown_active=False,
            timestamp=now,
        )

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #
    def _reactive_scale_up(
        self, cpu: float, mem: float, util: float
    ) -> tuple[str, str, float] | None:
        """Return the winning reactive scale-up trigger, or ``None`` if none breach.

        Each of CPU / memory / utilization is compared to its high threshold. Among
        the signals that breach, the one with the largest ``value / threshold`` ratio
        wins — i.e. the *most overloaded* signal drives the decision (e.g. util
        150/75 = 2.0 beats cpu 80/75 ≈ 1.07).

        Returns:
            ``(reason, trigger_metric, trigger_value)`` for the winner, or ``None``.
        """
        cfg = self._config
        candidates: list[tuple[float, str, str, float]] = []

        if cpu > cfg.cpu_threshold_scale_up:
            ratio = cpu / cfg.cpu_threshold_scale_up
            candidates.append((ratio, "reactive_cpu", "cpu_percent", cpu))
        if mem > cfg.memory_threshold_scale_up:
            ratio = mem / cfg.memory_threshold_scale_up
            candidates.append((ratio, "reactive_mem", "memory_percent", mem))
        if util > cfg.util_threshold_scale_up:
            ratio = util / cfg.util_threshold_scale_up
            candidates.append((ratio, "reactive_util", "effective_utilization", util))

        if not candidates:
            return None

        # Largest breach ratio wins (most overloaded signal).
        _ratio, reason, trigger_metric, trigger_value = max(candidates, key=lambda c: c[0])
        return reason, trigger_metric, trigger_value

    def _target_up(
        self,
        reason: str,
        trigger_value: float,
        cpu: float,
        mem: float,
        util: float,
        current_workers: int,
    ) -> int:
        """Compute the desired worker count for an (aggressive) scale-up.

        HPA-style ratio sizing: pick a *target* operating point at the midpoint of
        the relevant up/down thresholds, then scale the worker count by how far the
        driving metric overshoots that target::

            desired = ceil(current_workers * value / target)

        The midpoint target (rather than the high threshold) leaves the pool sitting
        comfortably inside the dead-band after the move, which avoids immediately
        re-tripping. The metric and target are chosen to match the trigger reason:

        * ``reactive_cpu``  → cpu vs midpoint(cpu_up, cpu_down)
        * ``reactive_mem``  → mem vs midpoint(mem_up, mem_down)
        * ``reactive_util`` / ``predictive`` → util-or-predicted vs midpoint(util_up, util_down)
        * ``anomaly`` → util vs midpoint(util_up, util_down) (same aggressive
          util path as ``reactive_util``; the anomaly's ``trigger_value`` is a
          z-score, so we size off the observed utilization instead)

        At least one worker is always added on a scale-up (``desired >=
        current_workers + 1``); final clamping to ``[min, max]`` happens in the
        caller.
        """
        cfg = self._config

        if reason == "reactive_cpu":
            value = cpu
            target = (cfg.cpu_threshold_scale_up + cfg.cpu_threshold_scale_down) / 2.0
        elif reason == "reactive_mem":
            value = mem
            target = (cfg.memory_threshold_scale_up + cfg.memory_threshold_scale_down) / 2.0
        elif reason == "anomaly":
            # An anomaly spike sizes off observed utilization (its trigger_value is
            # a z-score, not a util %), using the same aggressive util target.
            value = util
            target = (cfg.util_threshold_scale_up + cfg.util_threshold_scale_down) / 2.0
        else:
            # reactive_util or predictive — both size against effective utilization.
            # trigger_value already holds util (reactive) or predicted (predictive).
            value = trigger_value
            target = (cfg.util_threshold_scale_up + cfg.util_threshold_scale_down) / 2.0

        if target <= 0.0:
            # Degenerate config guard: fall back to a single-step increase.
            return current_workers + 1

        desired = math.ceil(current_workers * value / target)

        # Aggressive but bounded: always add at least one worker when scaling up.
        return max(desired, current_workers + 1)


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #
def _as_float(value, default: float = 0.0) -> float:
    """Coerce ``value`` to ``float``, returning ``default`` when not possible."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: int, low: int, high: int) -> int:
    """Clamp integer ``value`` into the inclusive range ``[low, high]``."""
    return max(low, min(high, value))


def _decision(
    *,
    action: str,
    reason: str,
    from_workers: int,
    to_workers: int,
    trigger_metric: str,
    trigger_value: float,
    confidence: float | None,
    cooldown_active: bool,
    timestamp: float,
) -> dict:
    """Build the canonical scaling-decision dict with normalized field types."""
    return {
        "action": action,
        "reason": reason,
        "from_workers": int(from_workers),
        "to_workers": int(to_workers),
        "trigger_metric": str(trigger_metric),
        "trigger_value": float(trigger_value),
        "confidence": (None if confidence is None else float(confidence)),
        "cooldown_active": bool(cooldown_active),
        "timestamp": float(timestamp),
    }
