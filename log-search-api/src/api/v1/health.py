from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter

from src.schemas.common import DetailedHealthResponse, HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def get_health() -> HealthResponse:
    return HealthResponse(status="ok", timestamp=datetime.now(UTC))


@router.get("/health/detailed", response_model=DetailedHealthResponse)
async def get_health_detailed() -> DetailedHealthResponse:
    # real probes wired in commit 2
    return DetailedHealthResponse(
        status="ok",
        timestamp=datetime.now(UTC),
        dependencies={"elasticsearch": "unknown", "redis": "unknown"},
    )
