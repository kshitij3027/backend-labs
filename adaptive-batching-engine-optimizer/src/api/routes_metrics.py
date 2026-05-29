"""Read-only metrics endpoint.

Surfaces the current snapshot, the recent chartable series, and the optimizer
status in a single :class:`~src.api.schemas.MetricsResponse` for the dashboard.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from src.api.dependencies import get_batcher
from src.api.schemas import MetricsResponse
from src.batcher import AdaptiveBatcher
from src.settings import get_settings

router = APIRouter(prefix="/api", tags=["metrics"])


@router.get("/metrics")
async def metrics(
    batcher: Annotated[AdaptiveBatcher, Depends(get_batcher)],
) -> MetricsResponse:
    """Return the latest snapshot, recent series, and current optimizer status."""
    points = get_settings().dashboard_points
    return MetricsResponse(
        current=batcher.latest_snapshot(),
        series=batcher.metrics_series(points),
        status=batcher.status(),
    )
