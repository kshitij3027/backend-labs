"""Forecast / predictions / models / retrain routes (C11).

This router is the **read-mostly** public API on top of the C8 prediction service
and the C10 feedback layer. Every route is fast (<2s): the "latest" reads hit the
Redis cache then Postgres; only ``GET /forecast/{steps}`` computes on demand (a
bounded fit over the recent window). Nothing here reimplements forecasting — it
delegates to :mod:`src.prediction_service`, :mod:`src.db.repository` and
:mod:`src.feedback`.

Routes
------
* ``GET /predictions``              — latest ensemble forecast (cache->DB).
* ``GET /forecast/{steps}``         — custom-horizon forecast (1..max), computed.
* ``GET /forecast/{metric}/history``— past forecasts + recent accuracy.
* ``GET /models``                   — ensemble roster + weights + deploy flags.
* ``POST /retrain``                 — async retrain (broker .delay, else BG task).

Runtime config (Feature Area B): the on-demand forecast paths fetch the current
weights + alert thresholds from :mod:`src.runtime_config` and pass them through to
:func:`src.prediction_service.generate_prediction`, so dashboard-driven ``/config``
changes take effect without a restart.

Path-collision note
-------------------
``GET /forecast/{steps}`` (one path segment, typed ``int``) and
``GET /forecast/{metric}/history`` (two segments) do **not** structurally collide
in FastAPI. ``{steps}`` is declared ``int`` so a non-integer single segment 422s
(or, for ``/forecast/cpu/history``, matches the two-segment route) rather than
mis-routing. The history route is declared first for clarity.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src import feedback, observability, prediction_service, runtime_config
from src.config import get_settings
from src.db import repository
from src.db.session import get_db
from src.schemas import (
    ForecastHistoryItem,
    ForecastHistoryResponse,
    ForecastResponse,
    ModelInfo,
    ModelsResponse,
    RetrainResponse,
)

router = APIRouter(tags=["forecast"])

logger = observability.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _default_metric(db: Session, metric: str | None) -> str | None:
    """Resolve the metric to use: the explicit arg, else the first known metric.

    Returns ``None`` when no metric was given and the metrics table is empty.
    """
    if metric:
        return metric.strip()
    try:
        names = repository.list_metric_names(db)
    except Exception:  # noqa: BLE001 - degrade to "no metric"
        return None
    return names[0] if names else None


# --------------------------------------------------------------------------- #
# GET /predictions — latest ensemble forecast (cache -> DB)
# --------------------------------------------------------------------------- #
@router.get(
    "/predictions",
    response_model=ForecastResponse,
    summary="Latest ensemble forecast for a metric (with confidence + breakdowns)",
)
def get_predictions(
    metric: str | None = Query(
        default=None,
        description="Metric name; defaults to the first available metric.",
    ),
    horizon: int | None = Query(
        default=None,
        ge=1,
        description="Horizon in minutes (defaults to the cached/latest forecast).",
    ),
    db: Session = Depends(get_db),
) -> ForecastResponse:
    """Return the most recent forecast for ``metric`` from cache, else Postgres.

    The payload carries the §8 fields plus the drill-down internals
    (``individual_forecasts``, ``ensemble_confidence``, ``weights_used``,
    ``failed_models``, scalar ``confidence``, ``alert_level``). Fast by design —
    no model fitting happens here. 404 when no metric exists or no forecast has
    been generated yet.
    """
    metric_name = _default_metric(db, metric)
    if not metric_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no metrics available; ingest data first",
        )

    payload = prediction_service.get_prediction(db, metric_name, horizon)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"no forecast available for metric {metric_name!r}",
        )
    return ForecastResponse.model_validate(payload)


# --------------------------------------------------------------------------- #
# GET /forecast/{metric}/history — past forecasts + recent accuracy
# --------------------------------------------------------------------------- #
@router.get(
    "/forecast/{metric}/history",
    response_model=ForecastHistoryResponse,
    summary="Past forecasts for a metric (historical vs actual drill-down)",
)
def get_forecast_history(
    metric: str,
    limit: int = Query(default=50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> ForecastHistoryResponse:
    """Return recent persisted forecasts for ``metric`` (newest-first) + accuracy.

    Powers the dashboard's historical-vs-actual comparison. ``recent_accuracy`` is
    a best-effort per-model map from the feedback ledger (empty if none scored
    yet). Never 500s on an empty history — returns ``count=0``.
    """
    metric_name = metric.strip()
    try:
        rows = repository.get_forecast_history(db, metric_name, limit=limit)
    except Exception as exc:  # noqa: BLE001
        logger.warning("forecast history read failed", metric=metric_name, error=str(exc))
        rows = []

    items = [
        ForecastHistoryItem(
            id=int(r.id),
            created_at=r.created_at,
            horizon_minutes=int(r.horizon_minutes),
            horizon_steps=int(r.horizon_steps),
            alert_level=r.alert_level,
            ensemble_prediction=list(r.ensemble_prediction or []),
            step_timestamps=list(r.step_timestamps or []),
        )
        for r in rows
    ]

    try:
        accuracy = feedback.recent_model_accuracy(db, metric_name)
    except Exception:  # noqa: BLE001 - accuracy is best-effort
        accuracy = {}

    return ForecastHistoryResponse(
        metric_name=metric_name,
        count=len(items),
        items=items,
        recent_accuracy={k: float(v) for k, v in accuracy.items()},
    )


# --------------------------------------------------------------------------- #
# GET /forecast/{steps} — custom-horizon forecast (computed on demand)
# --------------------------------------------------------------------------- #
@router.get(
    "/forecast/{steps}",
    response_model=ForecastResponse,
    summary="Custom-horizon forecast (1..max steps), computed on demand",
)
def get_custom_forecast(
    steps: int,
    metric: str | None = Query(
        default=None, description="Metric name; defaults to the first available."
    ),
    db: Session = Depends(get_db),
) -> ForecastResponse:
    """Compute a fresh forecast for ``steps`` future steps (interactive horizon).

    ``steps`` must be in ``[horizon_min_steps, horizon_max_steps]`` (default
    1..288) — out-of-range values 422. Computed on demand and **not** persisted or
    cached (so ad-hoc horizons never pollute the scheduled cache/DB). Runtime
    weights + alert thresholds are applied. An insufficient-data metric yields the
    service's degraded response (still 200, with a ``note``).
    """
    settings = get_settings()
    lo = int(settings.horizon_min_steps)
    hi = int(settings.horizon_max_steps)
    if steps < lo or steps > hi:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"steps must be in [{lo}, {hi}]",
        )

    metric_name = _default_metric(db, metric)
    if not metric_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="no metrics available; ingest data first",
        )

    weights = runtime_config.get_weights()
    high_thr, medium_thr = runtime_config.get_thresholds()

    start = time.perf_counter()
    payload = prediction_service.generate_prediction(
        db,
        metric_name,
        steps=steps,
        persist=False,
        cache=False,
        weights=weights,
        high_threshold=high_thr,
        medium_threshold=medium_thr,
    )
    elapsed = time.perf_counter() - start
    observability.observe_compute_seconds(metric_name, elapsed)
    observability.record_prediction(
        metric_name,
        str(payload.get("alert_level", "low")),
        float(payload.get("confidence", 0.0)),
    )
    return ForecastResponse.model_validate(payload)


# --------------------------------------------------------------------------- #
# GET /models — ensemble roster
# --------------------------------------------------------------------------- #
@router.get(
    "/models",
    response_model=ModelsResponse,
    summary="Ensemble members with current weights, accuracy and deploy flags",
)
def get_models(db: Session = Depends(get_db)) -> ModelsResponse:
    """List the ensemble members (model-comparison + weight transparency).

    Reflects the live ``ModelMetadata`` rows written by retrain / feedback, so the
    deployed set + weights here track the latest decisions. Also refreshes the
    Prometheus deployed-model gauge as a side effect.
    """
    try:
        rows = repository.list_model_metadata(db)
    except Exception as exc:  # noqa: BLE001
        logger.warning("model metadata read failed", error=str(exc))
        rows = []

    models = [ModelInfo.model_validate(r) for r in rows]
    deployed = sum(1 for m in models if m.is_deployed)
    observability.set_deployed_models(deployed)
    return ModelsResponse(count=len(models), deployed_count=deployed, models=models)


# --------------------------------------------------------------------------- #
# POST /retrain — out-of-band retrain (async; fast response)
# --------------------------------------------------------------------------- #
@router.post(
    "/retrain",
    response_model=RetrainResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger an out-of-band model retrain (async)",
)
def post_retrain(
    background_tasks: BackgroundTasks,
    metric: str | None = Query(
        default=None,
        description="Metric to retrain; omit to retrain every known metric.",
    ),
) -> RetrainResponse:
    """Schedule a retrain and return 202 immediately (never blocks the request).

    Behaviour: enqueue on the Celery broker via ``.delay()`` when one is reachable
    (``mode="async"``, ``task_id`` set). If the broker is unavailable (no Redis /
    enqueue raises), fall back to a FastAPI ``BackgroundTasks`` job that runs the
    retrain **in-process after the response is sent** (``mode="background"``,
    ``task_id=None``). Either way the client gets a fast 202. ``metric`` omitted
    -> retrain every known metric (``run_scheduled_retrain``).
    """
    # Import here so importing this router never pulls in Celery at module load.
    from src.tasks import run_retrain, run_scheduled_retrain

    target = metric.strip() if metric else None

    # Try the broker first. .delay() raises if the broker is unreachable.
    try:
        if target:
            async_result = run_retrain.delay(target)
        else:
            async_result = run_scheduled_retrain.delay()
        return RetrainResponse(
            status="scheduled",
            metric=target,
            task_id=str(getattr(async_result, "id", None) or ""),
            mode="async",
        )
    except Exception as exc:  # noqa: BLE001 - broker down -> run in-process post-response
        logger.warning("retrain enqueue failed; falling back to background task", error=str(exc))

    if target:
        background_tasks.add_task(run_retrain, target)
    else:
        background_tasks.add_task(run_scheduled_retrain)
    return RetrainResponse(
        status="scheduled", metric=target, task_id=None, mode="background"
    )


__all__ = ["router"]
