"""Validation / feedback loop: close the predict -> observe -> learn cycle (C10).

This module is the **feedback layer**. The forecasting path (C8) emits forecasts
for future timestamps; the scheduled jobs (C9) keep ingesting real metric points.
Once enough wall-clock time has passed, the actual observed values for a past
forecast's horizon exist in the ``metrics`` table — so we can finally score how
well each model (and the ensemble) did, and *act* on it.

It satisfies three ``project_requirements.md`` (Feature Area A) requirements:

* *"Build a validation system that compares each prediction against the actual
  observed value."* -> :func:`evaluate_forecast_accuracy` matches each stored
  forecast step to the nearest real :class:`~src.db.models.Metric`, scores the
  per-model + ensemble error, and persists an :class:`AccuracyRecord` per
  (model, metric, step).
* *"Adjust model weights dynamically based on recent per-model accuracy."* ->
  :func:`recent_model_accuracy` aggregates the recent accuracy ledger and
  :func:`adjust_weights` blends it (via :func:`src.validation.accuracy_to_weights`)
  with the current/prior weights, persisting the result to ``ModelMetadata``.
* *"Trigger retraining when prediction accuracy drops below threshold."* ->
  :func:`should_retrain` / :func:`maybe_trigger_retrain` compare the measured
  recent accuracy against ``accuracy_deploy_threshold`` and (if breached) fire the
  retrain task.

:func:`run_feedback_cycle` orchestrates all three for one metric and is what the
Celery beat job (:func:`src.tasks.run_feedback`) calls.

Design contract
---------------
* **Integration layer.** May use a DB session + :mod:`src.db.repository` +
  :mod:`src.models.metrics` + :mod:`src.validation` + :mod:`src.tasks`. No API
  routes, no dashboard. Reuses the canonical metric math
  (:mod:`src.models.metrics`) and the weighting formula
  (:func:`src.validation.accuracy_to_weights`) — never reimplements either.
* **Never raise** from a public function. Feedback runs in the background; on
  missing forecasts/actuals it degrades to zero/empty summaries and does *not*
  retrain blindly.
* **Matching actuals to step_timestamps** is *tolerant*: each forecast step's ISO
  timestamp is matched to the nearest stored metric within roughly half a sampling
  interval; unmatched steps are skipped, and only forecasts whose horizon has
  fully elapsed (every step has an actual within window) are scored.

Import-cycle note
-----------------
:mod:`src.tasks` imports the feedback functions at module top (it defines the
``run_feedback`` Celery wrapper). To avoid a cycle, this module does **not** import
:mod:`src.tasks` at module load: :func:`maybe_trigger_retrain` imports
``run_retrain`` *lazily* inside the function body. The retrain callable is also
injectable (``retrain_fn``) so tests can drive the loop synchronously without a
Celery worker / broker.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any, Callable

import numpy as np
from sqlalchemy.orm import Session

from src.config import get_settings
from src.db import repository
from src.db.models import Forecast, Metric
from src.models import metrics as metric_math
from src.validation import accuracy_to_weights

logger = logging.getLogger(__name__)

# The ensemble member names tracked alongside the synthetic "ensemble" series.
_ENSEMBLE_KEY = "ensemble"

# Neutral per-model accuracy used when there is no recent ledger data. Sits at the
# default deploy threshold so "no data" never *itself* triggers a retrain or
# distorts weights toward/away from any member.
_NEUTRAL_ACCURACY = 0.5

# How many recent AccuracyRecords to aggregate per model for weight/threshold
# decisions when the caller does not specify a lookback.
_DEFAULT_LOOKBACK = 200

# How many recent forecasts to evaluate per call by default (newest-first).
_DEFAULT_MAX_FORECASTS = 50


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _finite_or(value: object, default: float) -> float:
    """Coerce ``value`` to a finite float, else ``default``."""
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


def _parse_iso(ts: object) -> datetime | None:
    """Parse an ISO-8601 string (or pass a datetime) to a tz-aware UTC datetime."""
    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_utc(dt: datetime) -> datetime:
    """Best-effort coercion of a (possibly naive) datetime to tz-aware UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _step_interval_seconds(forecast: Forecast, step_times: list[datetime]) -> float:
    """Estimate the spacing between forecast steps in seconds.

    Prefers the spacing implied by ``horizon_minutes / horizon_steps``; falls back
    to the median gap of the parsed step timestamps, then to 5 minutes.
    """
    try:
        steps = int(forecast.horizon_steps or 0)
        minutes = float(forecast.horizon_minutes or 0)
        if steps > 0 and minutes > 0:
            return (minutes / steps) * 60.0
    except (TypeError, ValueError):
        pass
    if len(step_times) >= 2:
        gaps = [
            (step_times[i + 1] - step_times[i]).total_seconds()
            for i in range(len(step_times) - 1)
        ]
        gaps = [g for g in gaps if g > 0]
        if gaps:
            return float(np.median(gaps))
    return 5.0 * 60.0


def _build_actual_index(metric_rows: list[Metric]) -> tuple[np.ndarray, list[float]]:
    """Return ``(sorted_epoch_seconds, values)`` from metric rows for fast lookup."""
    pairs: list[tuple[float, float]] = []
    for m in metric_rows:
        ts = getattr(m, "timestamp", None)
        if ts is None:
            continue
        try:
            epoch = _to_utc(ts).timestamp()
        except (TypeError, ValueError, OverflowError):
            continue
        pairs.append((epoch, float(m.value)))
    pairs.sort(key=lambda p: p[0])
    epochs = np.array([p[0] for p in pairs], dtype=float)
    values = [p[1] for p in pairs]
    return epochs, values


def _nearest_actual(
    target_epoch: float,
    epochs: np.ndarray,
    values: list[float],
    tolerance_s: float,
) -> float | None:
    """Nearest actual value to ``target_epoch`` within ``tolerance_s`` (else None)."""
    if epochs.size == 0:
        return None
    pos = int(np.searchsorted(epochs, target_epoch))
    best_idx = -1
    best_diff = float("inf")
    for cand in (pos - 1, pos, pos + 1):
        if 0 <= cand < epochs.size:
            diff = abs(epochs[cand] - target_epoch)
            if diff < best_diff:
                best_diff = diff
                best_idx = cand
    if best_idx < 0 or best_diff > tolerance_s:
        return None
    return values[best_idx]


# --------------------------------------------------------------------------- #
# 1. Match forecasts to actuals + persist AccuracyRecords
# --------------------------------------------------------------------------- #
def evaluate_forecast_accuracy(
    session: Session,
    metric_name: str,
    *,
    since: datetime | None = None,
    max_forecasts: int | None = None,
) -> dict[str, Any]:
    """Score recent forecasts for ``metric_name`` against observed actuals.

    For each recent :class:`Forecast` whose horizon has fully elapsed (an actual
    exists within tolerance for *every* step), each predicted step is matched to
    the nearest stored :class:`Metric` and per-model + ensemble errors are
    computed. An :class:`AccuracyRecord` is persisted per matched (model, step)
    via :func:`src.db.repository.add_accuracy_record`.

    Matching tolerance: a step's timestamp matches the nearest metric within
    **half the step interval** (interval inferred from ``horizon_minutes /
    horizon_steps``). Steps with no in-window actual are skipped; a forecast with
    *any* unmatched step is treated as not-yet-elapsed and skipped entirely.

    Per-model accuracy / MAPE / RMSE are computed over the collected
    (predicted, actual) pairs using :mod:`src.models.metrics` (the single source
    of truth). The ``percentage_error`` stored on each record is the per-point
    absolute fractional error (``|pred - actual| / max(|actual|, eps)``).

    Returns a summary dict (zeros/empty, ``evaluated_forecasts=0`` when there is
    nothing to score). **Never raises.**
    """
    summary: dict[str, Any] = {
        "metric_name": metric_name,
        "evaluated_forecasts": 0,
        "matched_points": 0,
        "per_model_accuracy": {},
        "per_model_mape": {},
        "per_model_rmse": {},
    }
    try:
        limit = int(max_forecasts) if max_forecasts is not None else _DEFAULT_MAX_FORECASTS
        forecasts = repository.get_forecast_history(session, metric_name, limit=limit)
        if since is not None:
            since_utc = _to_utc(since)
            forecasts = [
                f for f in forecasts if _to_utc(f.created_at) >= since_utc
            ]
        if not forecasts:
            return summary

        # Load enough actuals to cover every forecast's horizon. Use the oldest
        # forecast creation time as a floor so we pull the right window once.
        oldest = min(_to_utc(f.created_at) for f in forecasts)
        metric_rows = repository.get_metrics(session, metric_name, since=oldest)
        epochs, values = _build_actual_index(metric_rows)

        now = _now()
        # Collected (predicted, actual) pairs per model (+ ensemble) across all
        # evaluated forecasts, for the aggregate metrics.
        preds: dict[str, list[float]] = {}
        actuals: dict[str, list[float]] = {}
        evaluated = 0
        matched_points = 0

        for forecast in forecasts:
            scored = _evaluate_one_forecast(
                session,
                forecast,
                metric_name,
                epochs,
                values,
                now,
            )
            if scored is None:
                continue
            evaluated += 1
            matched_points += scored["matched_points"]
            for model_name, (p_list, a_list) in scored["pairs"].items():
                preds.setdefault(model_name, []).extend(p_list)
                actuals.setdefault(model_name, []).extend(a_list)

        if evaluated:
            try:
                session.commit()
            except Exception:  # noqa: BLE001 - feedback must not crash the caller
                logger.exception("evaluate_forecast_accuracy: commit failed")
                try:
                    session.rollback()
                except Exception:  # noqa: BLE001
                    pass

        per_acc: dict[str, float] = {}
        per_mape: dict[str, float] = {}
        per_rmse: dict[str, float] = {}
        for model_name in preds:
            y_pred = preds[model_name]
            y_true = actuals[model_name]
            per_acc[model_name] = metric_math.accuracy_score_ts(y_true, y_pred)
            per_mape[model_name] = _finite_or(
                metric_math.mape(y_true, y_pred), float("inf")
            )
            per_rmse[model_name] = _finite_or(
                metric_math.rmse(y_true, y_pred), float("inf")
            )

        summary.update(
            {
                "evaluated_forecasts": evaluated,
                "matched_points": matched_points,
                "per_model_accuracy": per_acc,
                "per_model_mape": per_mape,
                "per_model_rmse": per_rmse,
            }
        )
        return summary
    except Exception:  # noqa: BLE001 - never raise from feedback
        logger.exception("evaluate_forecast_accuracy failed for %r", metric_name)
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass
        return summary


def _evaluate_one_forecast(
    session: Session,
    forecast: Forecast,
    metric_name: str,
    epochs: np.ndarray,
    values: list[float],
    now: datetime,
) -> dict[str, Any] | None:
    """Score a single forecast; persist AccuracyRecords. Return per-model pairs.

    Returns ``None`` (skip) when the forecast's horizon has not fully elapsed
    (i.e. an actual is missing for any step within tolerance), or when it carries
    no usable step timestamps / predictions.
    """
    raw_steps = list(forecast.step_timestamps or [])
    if not raw_steps:
        return None
    step_times = [_parse_iso(ts) for ts in raw_steps]
    if any(st is None for st in step_times):
        # Cannot reliably align; skip this forecast.
        return None
    step_times_ok: list[datetime] = [st for st in step_times if st is not None]

    # Only score forecasts whose horizon has fully elapsed.
    if step_times_ok and step_times_ok[-1] > now:
        return None

    interval_s = _step_interval_seconds(forecast, step_times_ok)
    tolerance_s = max(1.0, interval_s / 2.0)

    individual = dict(forecast.individual_forecasts or {})
    ensemble_pred = list(forecast.ensemble_prediction or [])
    series_map: dict[str, list[float]] = dict(individual)
    if ensemble_pred:
        series_map[_ENSEMBLE_KEY] = ensemble_pred

    if not series_map:
        return None

    # First, find the actual for every step; require ALL steps matched (elapsed).
    step_actuals: list[float | None] = []
    for st in step_times_ok:
        actual = _nearest_actual(st.timestamp(), epochs, values, tolerance_s)
        step_actuals.append(actual)
    if any(a is None for a in step_actuals):
        return None

    horizon_minutes = int(forecast.horizon_minutes or 0)
    pairs: dict[str, tuple[list[float], list[float]]] = {}
    matched_points = 0

    for model_name, model_preds in series_map.items():
        plist: list[float] = []
        alist: list[float] = []
        for idx, st in enumerate(step_times_ok):
            if idx >= len(model_preds):
                break
            predicted = _finite_or(model_preds[idx], float("nan"))
            actual = step_actuals[idx]
            if actual is None or not math.isfinite(predicted):
                continue
            abs_err = abs(predicted - actual)
            denom = max(abs(actual), 1e-9)
            pct_err = abs_err / denom
            plist.append(predicted)
            alist.append(actual)
            matched_points += 1
            try:
                repository.add_accuracy_record(
                    session,
                    model_name=model_name,
                    metric_name=metric_name,
                    evaluated_at=now,
                    horizon_minutes=horizon_minutes,
                    predicted_value=float(predicted),
                    actual_value=float(actual),
                    absolute_error=float(abs_err),
                    percentage_error=float(pct_err),
                    commit=False,
                )
            except Exception:  # noqa: BLE001 - one record write must not abort
                logger.warning(
                    "add_accuracy_record failed for %s/%s", model_name, metric_name
                )
        if plist:
            pairs[model_name] = (plist, alist)

    if not pairs:
        return None
    return {"pairs": pairs, "matched_points": matched_points}


# --------------------------------------------------------------------------- #
# 2. Recent per-model accuracy from the AccuracyRecord ledger
# --------------------------------------------------------------------------- #
def recent_model_accuracy(
    session: Session,
    metric_name: str | None = None,
    *,
    lookback: int | None = None,
) -> dict[str, float]:
    """Aggregate recent :class:`AccuracyRecord`s into per-model accuracy in [0, 1].

    For each model with recent records (newest ``lookback`` per model) the recent
    accuracy is :func:`src.models.metrics.accuracy_score_ts` over the stored
    (predicted, actual) pairs — i.e. ``1 - sMAPE`` over the recent ledger. Records
    lacking an ``actual_value`` are skipped.

    The model universe is taken from configured ``model_weights`` plus the
    ``ensemble`` pseudo-model and any extra model names present in the ledger.
    Models with no usable recent data default to :data:`_NEUTRAL_ACCURACY`.

    Returns ``{model_name: accuracy}``. **Never raises** — returns a neutral map
    on any failure.
    """
    lb = int(lookback) if lookback is not None else _DEFAULT_LOOKBACK
    model_names = _candidate_model_names(session)
    out: dict[str, float] = {name: _NEUTRAL_ACCURACY for name in model_names}
    try:
        for name in list(model_names):
            try:
                records = repository.get_recent_accuracy(
                    session, name, metric_name=metric_name, limit=lb
                )
            except Exception:  # noqa: BLE001
                continue
            y_pred: list[float] = []
            y_true: list[float] = []
            for r in records:
                if r.actual_value is None:
                    continue
                pv = _finite_or(r.predicted_value, float("nan"))
                av = _finite_or(r.actual_value, float("nan"))
                if math.isfinite(pv) and math.isfinite(av):
                    y_pred.append(pv)
                    y_true.append(av)
            if y_pred:
                out[name] = metric_math.accuracy_score_ts(y_true, y_pred)
        return out
    except Exception:  # noqa: BLE001
        logger.exception("recent_model_accuracy failed for %r", metric_name)
        return out


def _candidate_model_names(session: Session) -> list[str]:
    """Union of configured ensemble members + ``ensemble`` + deployed metadata."""
    names: list[str] = []
    try:
        names.extend(get_settings().model_weights.keys())
    except Exception:  # noqa: BLE001 - config must not break feedback
        pass
    if _ENSEMBLE_KEY not in names:
        names.append(_ENSEMBLE_KEY)
    try:
        for md in repository.list_model_metadata(session):
            if md.model_name not in names:
                names.append(md.model_name)
    except Exception:  # noqa: BLE001
        pass
    return names


# --------------------------------------------------------------------------- #
# 3. Dynamic weight adjustment (persisted to ModelMetadata)
# --------------------------------------------------------------------------- #
def adjust_weights(
    session: Session,
    metric_name: str,
    *,
    base_weights: dict[str, float] | None = None,
    persist: bool = True,
) -> dict[str, float]:
    """Recompute ensemble weights from recent per-model accuracy and persist them.

    Flow:

    1. Resolve the **prior** weights: ``base_weights`` if given, else the current
       stored ``ModelMetadata.weight`` values (preferred — they carry forward the
       last decision), falling back to ``settings.model_weights``.
    2. Read recent per-model accuracy (:func:`recent_model_accuracy`).
    3. Determine the *deployed* universe (members with a positive prior weight, or
       the configured members if none are positive yet) and feed an
       ``evaluate_models``-shaped dict into
       :func:`src.validation.accuracy_to_weights`, which blends prior * accuracy
       and renormalises over the deployed members.
    4. If ``persist``: write the new ``weight`` for each member to
       ``ModelMetadata`` (preserving ``is_deployed`` / ``accuracy``) and commit.

    Returns ``{model_name: weight}`` summing to ~1.0 over the deployed members (or
    the prior weights if nothing could be computed). **Never raises.**
    """
    try:
        prior = _resolve_prior_weights(session, base_weights)
        accuracy = recent_model_accuracy(session, metric_name)

        # Deployed universe: ensemble members with a positive prior weight. The
        # synthetic "ensemble" key is never itself an ensemble member.
        deployed = [
            name
            for name, w in prior.items()
            if name != _ENSEMBLE_KEY and _finite_or(w, 0.0) > 0.0
        ]
        if not deployed:
            deployed = [name for name in prior if name != _ENSEMBLE_KEY]
        if not deployed:
            return {k: float(v) for k, v in prior.items() if k != _ENSEMBLE_KEY}

        results = {
            name: {
                "accuracy": float(accuracy.get(name, _NEUTRAL_ACCURACY)),
                "passed": True,
            }
            for name in deployed
        }
        eval_shaped = {"results": results, "deployed": deployed}
        new_weights = accuracy_to_weights(eval_shaped, base_weights=prior)

        if persist and new_weights:
            _persist_weights(session, new_weights)
        return {k: float(v) for k, v in new_weights.items()}
    except Exception:  # noqa: BLE001 - never raise from feedback
        logger.exception("adjust_weights failed for %r", metric_name)
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass
        # Best-effort fallback: return the configured weights.
        try:
            return {
                k: float(v)
                for k, v in get_settings().model_weights.items()
            }
        except Exception:  # noqa: BLE001
            return {}


def _resolve_prior_weights(
    session: Session, base_weights: dict[str, float] | None
) -> dict[str, float]:
    """Pick the prior weights: explicit arg > stored metadata > settings default."""
    if base_weights:
        return {str(k): _finite_or(v, 0.0) for k, v in base_weights.items()}

    settings_weights: dict[str, float] = {}
    try:
        settings_weights = {
            str(k): _finite_or(v, 0.0)
            for k, v in get_settings().model_weights.items()
        }
    except Exception:  # noqa: BLE001
        settings_weights = {}

    stored: dict[str, float] = {}
    try:
        for md in repository.list_model_metadata(session):
            if md.model_name == _ENSEMBLE_KEY:
                continue
            stored[md.model_name] = _finite_or(md.weight, 0.0)
    except Exception:  # noqa: BLE001
        stored = {}

    # Prefer stored weights when any are positive; otherwise fall back to config.
    if any(w > 0.0 for w in stored.values()):
        # Make sure every configured member is represented (default 0 if absent).
        merged = dict(settings_weights)
        merged.update(stored)
        return merged
    return settings_weights


def _persist_weights(session: Session, weights: dict[str, float]) -> None:
    """Write the new ``weight`` for each member to ``ModelMetadata`` and commit."""
    for name, weight in weights.items():
        try:
            repository.upsert_model_metadata(
                session, name, weight=float(weight), commit=False
            )
        except Exception:  # noqa: BLE001 - one write must not abort the rest
            logger.warning("adjust_weights: failed to persist weight for %s", name)
    try:
        session.commit()
    except Exception:  # noqa: BLE001
        logger.exception("adjust_weights: commit failed")
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- #
# 4. Retrain trigger
# --------------------------------------------------------------------------- #
def should_retrain(
    session: Session,
    metric_name: str,
    *,
    threshold: float | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Decide whether ``metric_name``'s models need retraining.

    Compares the measured **recent accuracy** against ``threshold`` (default
    ``settings.accuracy_deploy_threshold``). The measured accuracy prefers the
    ``ensemble`` recent accuracy when it has real data; otherwise it falls back to
    the mean of the per-model recent accuracies. Returns ``(True, info)`` when the
    accuracy has dropped **below** the threshold.

    On no data (no ledger records at all), returns ``(False, info)`` — we never
    retrain blindly. ``info`` carries ``accuracy``, ``threshold``, ``source`` and
    ``has_data``. **Never raises.**
    """
    thr = _resolve_threshold(threshold)
    info: dict[str, Any] = {
        "metric_name": metric_name,
        "accuracy": None,
        "threshold": thr,
        "source": None,
        "has_data": False,
    }
    try:
        accuracy_map = recent_model_accuracy(session, metric_name)
        has_data = _has_ledger_data(session, metric_name)
        info["has_data"] = has_data
        if not has_data:
            return False, info

        # Prefer the ensemble's recent accuracy; else mean of the members.
        ensemble_acc = accuracy_map.get(_ENSEMBLE_KEY)
        member_accs = [
            v for k, v in accuracy_map.items() if k != _ENSEMBLE_KEY
        ]
        if ensemble_acc is not None and _ensemble_has_data(session, metric_name):
            measured = float(ensemble_acc)
            info["source"] = "ensemble"
        elif member_accs:
            measured = float(np.mean(member_accs))
            info["source"] = "members_mean"
        else:
            return False, info

        info["accuracy"] = measured
        return (measured < thr), info
    except Exception:  # noqa: BLE001 - never raise; never retrain blindly
        logger.exception("should_retrain failed for %r", metric_name)
        return False, info


def maybe_trigger_retrain(
    session: Session,
    metric_name: str,
    *,
    threshold: float | None = None,
    retrain_fn: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Trigger retraining for ``metric_name`` iff recent accuracy is below threshold.

    Calls :func:`should_retrain`; if it returns ``True`` the retrain is fired.
    ``retrain_fn`` is **injectable** (default: a synchronous call to
    :func:`src.tasks.run_retrain`, imported lazily to avoid an import cycle). Pass
    ``retrain_fn=lambda m: run_retrain.delay(m)`` to enqueue asynchronously, or a
    stub in tests.

    Returns ``{metric_name, retrained, accuracy, threshold, retrain_result?,
    source, has_data}``. **Never raises.**
    """
    do_retrain, info = should_retrain(session, metric_name, threshold=threshold)
    result: dict[str, Any] = {
        "metric_name": metric_name,
        "retrained": False,
        "accuracy": info.get("accuracy"),
        "threshold": info.get("threshold"),
        "source": info.get("source"),
        "has_data": info.get("has_data"),
    }
    if not do_retrain:
        return result
    try:
        fn = retrain_fn
        if fn is None:
            # Lazy import breaks the src.tasks <-> src.feedback cycle.
            from src.tasks import run_retrain

            fn = run_retrain
        retrain_result = fn(metric_name)
        result["retrained"] = True
        result["retrain_result"] = retrain_result
        return result
    except Exception:  # noqa: BLE001 - a retrain failure must not crash feedback
        logger.exception("maybe_trigger_retrain: retrain failed for %r", metric_name)
        result["retrained"] = False
        result["error"] = "retrain failed"
        return result


# --------------------------------------------------------------------------- #
# 5. End-to-end feedback cycle
# --------------------------------------------------------------------------- #
def run_feedback_cycle(
    session: Session,
    metric_name: str,
    *,
    threshold: float | None = None,
    retrain_fn: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Run the full feedback loop for one metric.

    1. :func:`evaluate_forecast_accuracy` — match past forecasts to actuals and
       persist :class:`AccuracyRecord`s.
    2. :func:`adjust_weights` — blend recent accuracy into new ensemble weights
       and persist them to ``ModelMetadata``.
    3. :func:`maybe_trigger_retrain` — retrain if recent accuracy is below
       threshold.

    Returns ``{metric_name, accuracy_summary, new_weights, retrain}``.
    **Never raises** — this is what the Celery beat job calls.
    """
    accuracy_summary: dict[str, Any] = {}
    new_weights: dict[str, float] = {}
    retrain: dict[str, Any] = {}
    try:
        accuracy_summary = evaluate_forecast_accuracy(session, metric_name)
    except Exception:  # noqa: BLE001
        logger.exception("run_feedback_cycle: evaluate step failed for %r", metric_name)
    try:
        new_weights = adjust_weights(session, metric_name)
    except Exception:  # noqa: BLE001
        logger.exception("run_feedback_cycle: adjust step failed for %r", metric_name)
    try:
        retrain = maybe_trigger_retrain(
            session, metric_name, threshold=threshold, retrain_fn=retrain_fn
        )
    except Exception:  # noqa: BLE001
        logger.exception("run_feedback_cycle: retrain step failed for %r", metric_name)
    return {
        "metric_name": metric_name,
        "accuracy_summary": accuracy_summary,
        "new_weights": new_weights,
        "retrain": retrain,
    }


# --------------------------------------------------------------------------- #
# Internal helpers (threshold / ledger presence)
# --------------------------------------------------------------------------- #
def _resolve_threshold(threshold: float | None) -> float:
    """Return ``threshold`` if valid, else the configured deploy threshold."""
    if threshold is not None:
        t = _finite_or(threshold, float("nan"))
        if math.isfinite(t):
            return t
    try:
        return float(get_settings().accuracy_deploy_threshold)
    except Exception:  # noqa: BLE001
        return 0.6


def _has_ledger_data(session: Session, metric_name: str) -> bool:
    """True when *any* model has a recent scored AccuracyRecord for this metric."""
    for name in _candidate_model_names(session):
        try:
            records = repository.get_recent_accuracy(
                session, name, metric_name=metric_name, limit=1
            )
        except Exception:  # noqa: BLE001
            continue
        if any(r.actual_value is not None for r in records):
            return True
    return False


def _ensemble_has_data(session: Session, metric_name: str) -> bool:
    """True when the ``ensemble`` pseudo-model has a recent scored record."""
    try:
        records = repository.get_recent_accuracy(
            session, _ENSEMBLE_KEY, metric_name=metric_name, limit=1
        )
    except Exception:  # noqa: BLE001
        return False
    return any(r.actual_value is not None for r in records)


__all__ = [
    "evaluate_forecast_accuracy",
    "recent_model_accuracy",
    "adjust_weights",
    "should_retrain",
    "maybe_trigger_retrain",
    "run_feedback_cycle",
]
