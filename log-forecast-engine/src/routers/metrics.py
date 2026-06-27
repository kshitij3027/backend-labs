"""Metric ingestion + read-back routes.

* ``POST /metrics`` — ingest a batch of metric points into Postgres.
* ``GET /metrics/{metric_name}`` — read recent stored points for one metric.

Note on the route surface: the §requirements ``GET /metrics`` *analytics*
endpoint (accuracy / processing times / resource usage) is a different,
later-commit (C11) concern. To avoid a clash, this module deliberately does
**not** register a bare ``GET /metrics`` — the data read lives under
``GET /metrics/{metric_name}`` only.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src.db import repository
from src.db.session import get_db
from src.ingestion import ingest_metrics
from src.schemas import (
    MetricIngestRequest,
    MetricIngestResponse,
    MetricPoint,
    MetricQueryResponse,
)

router = APIRouter(tags=["metrics"])


@router.post(
    "/metrics",
    response_model=MetricIngestResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest a batch of metric points",
)
def post_metrics(
    body: MetricIngestRequest,
    db: Session = Depends(get_db),
) -> MetricIngestResponse:
    """Validate and persist a batch of metric observations.

    Body shape: ``{"points": [{"metric_name": ..., "value": ..., "timestamp": ...?}]}``.
    ``timestamp`` may be omitted per-point (defaults to now, UTC). Returns the
    number of rows ingested and the distinct metric names seen. Invalid input
    (empty batch, blank name, non-finite value) yields HTTP 422.
    """
    try:
        count = ingest_metrics(db, body.points)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    names = sorted({p.metric_name.strip() for p in body.points})
    return MetricIngestResponse(ingested=count, metric_names=names)


@router.get(
    "/metrics/{metric_name}",
    response_model=MetricQueryResponse,
    summary="Read recent stored points for a metric",
)
def get_metric_points(
    metric_name: str,
    limit: int = Query(default=100, ge=1, le=10000),
    since: datetime | None = Query(
        default=None,
        description="ISO-8601 datetime; only points at or after this time are returned.",
    ),
    db: Session = Depends(get_db),
) -> MetricQueryResponse:
    """Return up to ``limit`` recent points for ``metric_name`` (oldest-first).

    When ``since`` is given, only points with ``timestamp >= since`` are returned.
    Primarily a verification aid for ingestion / E2E flows.
    """
    rows = repository.get_metrics(db, metric_name, since=since, limit=limit)
    points = [MetricPoint.model_validate(r) for r in rows]
    return MetricQueryResponse(
        metric_name=metric_name,
        count=len(points),
        points=points,
    )
