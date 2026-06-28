"""System routes: enhanced health, application metrics, Prometheus exposition (C11).

Three endpoints, all fast and crash-proof:

* ``GET /health``            — model status, Redis + DB connectivity, perf snapshot.
* ``GET /metrics``           — application metrics **JSON** (accuracy / processing
                               times / resource usage / counts) — the
                               ``project_requirements.md`` ``/metrics`` endpoint.
* ``GET /metrics/prometheus``— Prometheus **text** exposition.

The ``/metrics`` path split is documented in :mod:`src.observability`. This router
is included **before** the metrics-data router in :mod:`src.api` so the literal
``/metrics/prometheus`` is registered ahead of the parametrised
``/metrics/{metric_name}`` (literal wins), and the bare ``/metrics`` (JSON) does
not collide with the deeper data read.
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from src import feedback, observability
from src.clients import redis as redis_client
from src.db import repository
from src.db.session import get_db
from src.schemas import (
    AppMetricsResponse,
    HealthResponse,
    SubsystemHealth,
)

router = APIRouter(tags=["system"])

#: Reported in /health. Mirrors src.api.SERVICE_VERSION / SERVICE_NAME.
SERVICE_VERSION = "0.1.0"
SERVICE_NAME = "log-forecast-engine"

#: Process start time for a cheap uptime figure in /health.
_START_TIME = time.time()


# --------------------------------------------------------------------------- #
# GET /health — never raises; degraded reported in-body with HTTP 200
# --------------------------------------------------------------------------- #
@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health: model status, Redis + DB connectivity, performance snapshot",
)
def health(db: Session = Depends(get_db)) -> HealthResponse:
    """Report subsystem health without ever 500-ing.

    Checks DB (``SELECT 1``), Redis (``ping``) and counts deployed models. Returns
    HTTP 200 always; ``status`` is ``"degraded"`` when DB or Redis is unreachable
    so the dashboard can surface the issue (per-subsystem booleans are included).
    A lightweight perf snapshot (process RSS MB, CPU times, uptime) is attached.
    """
    db_ok = False
    deployed_models = 0
    try:
        db.execute(text("SELECT 1"))
        db_ok = True
        try:
            rows = repository.list_model_metadata(db)
            deployed_models = sum(1 for r in rows if getattr(r, "is_deployed", False))
            observability.set_deployed_models(deployed_models)
        except Exception:  # noqa: BLE001 - count is best-effort
            deployed_models = 0
    except Exception:  # noqa: BLE001 - DB down -> degraded, not 500
        db_ok = False

    redis_ok = False
    try:
        redis_ok = bool(redis_client.ping())
    except Exception:  # noqa: BLE001
        redis_ok = False

    perf: dict[str, Any] = observability.resource_usage()
    perf["uptime_seconds"] = round(time.time() - _START_TIME, 2)

    overall = "ok" if (db_ok and redis_ok) else "degraded"
    return HealthResponse(
        status=overall,
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
        deployed_models=deployed_models,
        subsystems=SubsystemHealth(database=db_ok, redis=redis_ok),
        performance=perf,
    )


# --------------------------------------------------------------------------- #
# GET /metrics — application metrics JSON (the requirements' /metrics)
# --------------------------------------------------------------------------- #
@router.get(
    "/metrics",
    response_model=AppMetricsResponse,
    summary="Application metrics: accuracy, processing times, resource usage",
)
def app_metrics(db: Session = Depends(get_db)) -> AppMetricsResponse:
    """Return application-level metrics in JSON (never 500; best-effort).

    * ``prediction_accuracy`` — recent per-model accuracy aggregated across all
      metrics (feedback ledger; empty until forecasts have been scored).
    * ``processing_times`` — summary of recent on-demand forecast compute times
      (count / mean / p95 / max in ms) recorded by the forecast route.
    * ``resource_usage`` — process RSS MB + CPU times (stdlib ``resource``).
    * ``counts`` — deployed model count + recorded compute samples.
    """
    # Prediction accuracy (across all metrics): mean per model over each metric's
    # recent ledger. Best-effort; empty on any failure.
    accuracy: dict[str, float] = {}
    try:
        names = repository.list_metric_names(db)
        per_model: dict[str, list[float]] = {}
        for name in names:
            acc = feedback.recent_model_accuracy(db, name)
            for model_name, val in acc.items():
                per_model.setdefault(model_name, []).append(float(val))
        accuracy = {
            m: round(sum(v) / len(v), 4) for m, v in per_model.items() if v
        }
    except Exception:  # noqa: BLE001
        accuracy = {}

    # Processing times from the in-memory ring of recent compute durations (ms).
    samples = observability.recent_compute_ms()
    if samples:
        ordered = sorted(samples)
        idx = max(0, int(round(0.95 * (len(ordered) - 1))))
        processing_times: dict[str, Any] = {
            "count": len(ordered),
            "mean_ms": round(sum(ordered) / len(ordered), 3),
            "p95_ms": round(ordered[idx], 3),
            "max_ms": round(ordered[-1], 3),
        }
    else:
        processing_times = {"count": 0, "mean_ms": None, "p95_ms": None, "max_ms": None}

    resource = observability.resource_usage()

    deployed = 0
    try:
        rows = repository.list_model_metadata(db)
        deployed = sum(1 for r in rows if getattr(r, "is_deployed", False))
    except Exception:  # noqa: BLE001
        deployed = 0

    return AppMetricsResponse(
        prediction_accuracy=accuracy,
        processing_times=processing_times,
        resource_usage=resource,
        counts={
            "deployed_models": deployed,
            "compute_samples": len(samples),
        },
    )


# --------------------------------------------------------------------------- #
# GET /metrics/prometheus — Prometheus text exposition
# --------------------------------------------------------------------------- #
@router.get(
    "/metrics/prometheus",
    summary="Prometheus exposition (text/plain)",
    include_in_schema=False,
)
def prometheus_metrics() -> Response:
    """Return the Prometheus text exposition (``CONTENT_TYPE_LATEST``)."""
    body, content_type = observability.metrics_endpoint()
    return Response(content=body, media_type=content_type)


__all__ = ["router"]
