"""Prediction service: the integration layer (feature -> models -> ensemble -> DB + cache).

This is the orchestrator that ties the otherwise-pure pieces together (C8). It is
the *only* module in the forecast path that is allowed to touch the database,
Redis, and the model/ensemble layers at once:

    metrics (Postgres) -> features.to_series -> fit 4 models -> ensemble_forecast
        -> persist Forecast (Postgres) + cache (Redis) -> ForecastResponse dict

Graceful degradation is the headline contract:

* **Redis down** -> the forecast is still computed, persisted to Postgres, and
  returned; the cache write is a no-op (handled in :mod:`src.clients.redis`).
* **A model fails to fit/predict** -> it is dropped (the ensemble already drops
  members that raise; we additionally skip models that fail to *fit*).
* **Insufficient data** -> a safe, zeroed *degraded* result is returned (alert
  ``"low"``, a clear ``note``) instead of raising.

Times are tz-aware UTC throughout. ``step_timestamps`` are spaced by the inferred
sampling interval (median spacing of the stored series, falling back to 5 min).
The output dict matches :class:`src.schemas.ForecastResponse` so the API can
return it directly and :func:`src.db.repository.save_forecast` can persist it
column-for-column.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src import ensemble as ensemble_mod
from src import features
from src.clients import redis as redis_client
from src.config import get_settings
from src.db import repository
from src.models import (
    ARIMAForecaster,
    ExpSmoothingForecaster,
    LinearForecaster,
    XGBoostForecaster,
)
from src.models.base import BaseForecaster

logger = logging.getLogger(__name__)

# Fallback sampling interval (minutes) when it cannot be inferred from the data.
_FALLBACK_STEP_MINUTES = 5.0

# Minimum number of cleaned points required to attempt a real forecast.
_MIN_POINTS = 4


# --------------------------------------------------------------------------- #
# Model construction
# --------------------------------------------------------------------------- #
def build_models() -> list[BaseForecaster]:
    """Return fresh, unfitted instances of the four ensemble members.

    Used directly by :func:`generate_prediction` (single-window) and as the body
    of the ``model_factory`` passed to
    :func:`src.ensemble.multi_window_ensemble`.
    """
    return [
        ARIMAForecaster(),
        ExpSmoothingForecaster(),
        LinearForecaster(),
        XGBoostForecaster(),
    ]


# --------------------------------------------------------------------------- #
# Horizon / interval helpers
# --------------------------------------------------------------------------- #
def _infer_step_minutes(series: pd.Series) -> float:
    """Infer the sampling interval (minutes) from the median spacing of ``series``.

    Falls back to :data:`_FALLBACK_STEP_MINUTES` when the index is not a
    ``DatetimeIndex`` or the spacing cannot be determined.
    """
    idx = getattr(series, "index", None)
    if not isinstance(idx, pd.DatetimeIndex) or len(idx) < 2:
        return _FALLBACK_STEP_MINUTES
    deltas_ns = np.diff(idx.asi8)
    deltas_ns = deltas_ns[deltas_ns > 0]
    if deltas_ns.size == 0:
        return _FALLBACK_STEP_MINUTES
    median_ns = float(np.median(deltas_ns))
    minutes = median_ns / 1e9 / 60.0
    if not np.isfinite(minutes) or minutes <= 0.0:
        return _FALLBACK_STEP_MINUTES
    return minutes


def _horizon_to_steps(horizon_minutes: int, step_minutes: float) -> int:
    """Convert a horizon in minutes to a step count, clamped to config bounds."""
    settings = get_settings()
    if step_minutes <= 0.0:
        step_minutes = _FALLBACK_STEP_MINUTES
    steps = int(round(horizon_minutes / step_minutes))
    steps = max(int(settings.horizon_min_steps), steps)
    steps = min(int(settings.horizon_max_steps), steps)
    return max(1, steps)


def _future_timestamps(start: datetime, steps: int, step_minutes: float) -> list[str]:
    """ISO-8601 timestamps for each predicted step, spaced by ``step_minutes``.

    The first step is one interval *after* ``start`` (forecasts are for the
    future, not the generation instant).
    """
    out: list[str] = []
    for i in range(1, steps + 1):
        ts = start + timedelta(minutes=step_minutes * i)
        out.append(ts.isoformat())
    return out


# --------------------------------------------------------------------------- #
# Recent accuracy from ModelMetadata (optional confidence signal)
# --------------------------------------------------------------------------- #
def _recent_accuracy(session: Session) -> dict[str, float] | None:
    """Build ``{model_name: accuracy}`` from stored ``ModelMetadata`` rows.

    Returns ``None`` when no usable accuracy is recorded, so the ensemble falls
    back to its neutral prior. Never raises — a metadata read failure simply
    yields ``None``.
    """
    try:
        rows = repository.list_model_metadata(session)
    except Exception as exc:  # noqa: BLE001 - metadata is best-effort
        logger.warning("could not load model metadata for accuracy: %s", exc)
        return None
    acc: dict[str, float] = {}
    for row in rows:
        if row.accuracy is not None:
            try:
                acc[row.model_name] = float(row.accuracy)
            except (TypeError, ValueError):
                continue
    return acc or None


# --------------------------------------------------------------------------- #
# Degraded result
# --------------------------------------------------------------------------- #
def _degraded_response(
    metric_name: str,
    horizon_minutes: int,
    steps: int,
    now: datetime,
    note: str,
) -> dict[str, Any]:
    """A safe, zeroed forecast returned when there is too little data to model."""
    steps = max(0, int(steps))
    zeros = [0.0] * steps
    return {
        "metric_name": metric_name,
        "timestamp": now.isoformat(),
        "forecast_horizon_minutes": int(horizon_minutes),
        "horizon_steps": steps,
        "step_timestamps": [],
        "ensemble_prediction": list(zeros),
        "ensemble_confidence": list(zeros),
        "individual_forecasts": {},
        "lower": list(zeros),
        "upper": list(zeros),
        "alert_level": "low",
        "confidence": 0.0,
        "weights_used": {},
        "failed_models": [],
        "cached": False,
        "note": note,
    }


# --------------------------------------------------------------------------- #
# Generate
# --------------------------------------------------------------------------- #
def generate_prediction(
    session: Session,
    metric_name: str,
    horizon_minutes: int | None = None,
    *,
    steps: int | None = None,
    use_multi_window: bool = False,
    persist: bool = True,
    cache: bool = True,
) -> dict[str, Any]:
    """Generate a forecast for ``metric_name`` and (optionally) persist + cache it.

    Flow:

    1. Resolve the horizon (``horizon_minutes`` defaults to
       ``settings.default_horizon_min``) and the sampling interval (inferred from
       the stored series spacing, falling back to 5 min). ``horizon_steps`` is
       ``round(horizon_minutes / step_minutes)`` clamped to the configured
       ``[horizon_min_steps, horizon_max_steps]``. An explicit ``steps`` override
       wins over the minutes->steps conversion.
    2. Load the recent training window
       (``now - training_window_days`` .. now) via the repository and normalise to
       a :class:`pandas.Series`. Too little data -> a degraded result (no crash).
    3. Build + **fit** the four models (a model that fails to fit is dropped).
    4. Compute the data-quality / pattern-stability confidence signals and an
       optional per-model recent-accuracy signal from ``ModelMetadata``.
    5. Run :func:`src.ensemble.ensemble_forecast` (or
       :func:`src.ensemble.multi_window_ensemble` when ``use_multi_window``).
    6. Assemble a :class:`src.schemas.ForecastResponse`-shaped dict.
    7. ``persist`` -> :func:`src.db.repository.save_forecast` (committed).
       ``cache`` -> :func:`src.clients.redis.cache_prediction` (best-effort).

    Returns the forecast dict (``cached=False``).
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)
    horizon_minutes = (
        int(horizon_minutes)
        if horizon_minutes is not None
        else int(settings.default_horizon_min)
    )

    # --- 2. Load training data ---------------------------------------------
    since = now - timedelta(days=int(settings.training_window_days))
    try:
        metrics = repository.get_metrics(session, metric_name, since=since)
    except Exception as exc:  # noqa: BLE001 - DB read issue -> degrade
        logger.warning("metric read failed for %r: %s", metric_name, exc)
        metrics = []

    points = [(m.timestamp, m.value) for m in metrics]
    series: pd.Series | None = None
    if points:
        try:
            series = features.to_series(points)
        except (ValueError, TypeError):
            series = None

    if series is None or len(series) < _MIN_POINTS:
        # Use the fallback interval to at least report a sensible step count.
        fallback_steps = (
            int(steps)
            if steps is not None
            else _horizon_to_steps(horizon_minutes, _FALLBACK_STEP_MINUTES)
        )
        return _degraded_response(
            metric_name,
            horizon_minutes,
            fallback_steps,
            now,
            note=(
                f"insufficient data: need >= {_MIN_POINTS} points in the last "
                f"{settings.training_window_days}d window to forecast"
            ),
        )

    # --- 1. Resolve interval + steps ---------------------------------------
    step_minutes = _infer_step_minutes(series)
    horizon_steps = (
        int(steps) if steps is not None else _horizon_to_steps(horizon_minutes, step_minutes)
    )
    horizon_steps = max(1, min(int(settings.horizon_max_steps), horizon_steps))

    # --- 4. Confidence signals ---------------------------------------------
    data_quality = features.data_quality_score(series)
    pattern_stability = features.pattern_stability_score(series)
    recent_accuracy = _recent_accuracy(session)
    weights = dict(settings.model_weights)

    # --- 3 + 5. Fit models and run the ensemble ----------------------------
    if use_multi_window:
        # Short window for near-term precision + long window for trend awareness.
        n = len(series)
        windows = sorted({max(_MIN_POINTS, n // 4), n})
        result = ensemble_mod.multi_window_ensemble(
            build_models,
            series,
            horizon_steps,
            windows,
            weights=weights,
            recent_accuracy=recent_accuracy,
        )
    else:
        fitted: list[BaseForecaster] = []
        for model in build_models():
            try:
                model.fit(series)
                fitted.append(model)
            except Exception as exc:  # noqa: BLE001 - drop a model that won't fit
                logger.warning("model %s failed to fit: %s", model.name, exc)
        result = ensemble_mod.ensemble_forecast(
            fitted,
            horizon_steps,
            weights=weights,
            recent_accuracy=recent_accuracy,
            data_quality=data_quality,
            pattern_stability=pattern_stability,
        )

    # --- 6. Assemble the response ------------------------------------------
    effective_steps = int(result.get("steps", horizon_steps)) or horizon_steps
    step_timestamps = _future_timestamps(now, effective_steps, step_minutes)

    payload: dict[str, Any] = {
        "metric_name": metric_name,
        "timestamp": now.isoformat(),
        "forecast_horizon_minutes": horizon_minutes,
        "horizon_steps": effective_steps,
        "step_timestamps": step_timestamps,
        "ensemble_prediction": result.get("ensemble_prediction", []),
        "ensemble_confidence": result.get("ensemble_confidence", []),
        "individual_forecasts": result.get("individual_forecasts", {}),
        "lower": result.get("lower", []),
        "upper": result.get("upper", []),
        "alert_level": result.get("alert_level", "low"),
        "confidence": float(result.get("confidence", 0.0)),
        "weights_used": result.get("weights_used", {}),
        "failed_models": result.get("failed_models", []),
        "cached": False,
        "note": None,
    }

    # --- 7. Persist + cache -------------------------------------------------
    if persist:
        try:
            repository.save_forecast(
                session,
                metric_name=metric_name,
                created_at=now,
                horizon_minutes=horizon_minutes,
                horizon_steps=effective_steps,
                ensemble_prediction=payload["ensemble_prediction"],
                ensemble_confidence=payload["ensemble_confidence"],
                individual_forecasts=payload["individual_forecasts"],
                alert_level=payload["alert_level"],
                step_timestamps=step_timestamps,
                commit=True,
            )
        except Exception as exc:  # noqa: BLE001 - persistence failure is logged
            logger.error("failed to persist forecast for %r: %s", metric_name, exc)
            try:
                session.rollback()
            except Exception:  # noqa: BLE001
                pass

    if cache:
        # Best-effort; cache_prediction itself never raises (Redis down -> no-op).
        redis_client.cache_prediction(metric_name, horizon_minutes, payload)

    return payload


# --------------------------------------------------------------------------- #
# Read (cache-first, Postgres fallback)
# --------------------------------------------------------------------------- #
def get_prediction(
    session: Session,
    metric_name: str,
    horizon_minutes: int | None = None,
) -> dict[str, Any] | None:
    """Fast read path: Redis cache first, then the latest persisted forecast.

    * A cache hit returns the cached dict with ``cached=True``.
    * On a miss, the most recent :class:`src.db.models.Forecast` row is shaped
      into a response dict (``cached=False``).
    * Returns ``None`` when neither source has a forecast for ``metric_name``.
    """
    cached = redis_client.get_cached_prediction(metric_name, horizon_minutes)
    if cached is not None:
        cached["cached"] = True
        return cached

    try:
        row = repository.get_latest_forecast(session, metric_name)
    except Exception as exc:  # noqa: BLE001
        logger.warning("forecast read failed for %r: %s", metric_name, exc)
        return None
    if row is None:
        return None

    return _forecast_row_to_dict(row)


def _forecast_row_to_dict(row: Any) -> dict[str, Any]:
    """Shape a persisted :class:`Forecast` row into a ForecastResponse dict.

    The DB row does not store the ensemble interval (``lower``/``upper``),
    aggregate scalar ``confidence``, ``weights_used`` or ``failed_models``, so
    those are reconstructed conservatively: the interval collapses to the point
    forecast and the scalar confidence is re-derived from the stored per-step
    confidence array (mean of the leading steps, mirroring the ensemble).
    """
    created = row.created_at
    if isinstance(created, datetime):
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        ts = created.isoformat()
    else:
        ts = str(created)

    ens_pred = list(row.ensemble_prediction or [])
    ens_conf = list(row.ensemble_confidence or [])
    confidence = ensemble_mod.aggregate_confidence(ens_conf) if ens_conf else 0.0

    return {
        "metric_name": row.metric_name,
        "timestamp": ts,
        "forecast_horizon_minutes": int(row.horizon_minutes),
        "horizon_steps": int(row.horizon_steps),
        "step_timestamps": list(row.step_timestamps or []),
        "ensemble_prediction": ens_pred,
        "ensemble_confidence": ens_conf,
        "individual_forecasts": dict(row.individual_forecasts or {}),
        "lower": list(ens_pred),
        "upper": list(ens_pred),
        "alert_level": row.alert_level,
        "confidence": float(confidence),
        "weights_used": {},
        "failed_models": [],
        "cached": False,
        "note": None,
    }


__all__ = [
    "build_models",
    "generate_prediction",
    "get_prediction",
]
