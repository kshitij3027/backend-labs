from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import redis.asyncio as redis
from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends

from src.clients.elasticsearch import get_es, ping_es
from src.clients.redis import get_redis, ping_redis
from src.schemas.common import DetailedHealthResponse, HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def get_health() -> HealthResponse:
    return HealthResponse(status="ok", timestamp=datetime.now(UTC))


@router.get("/health/detailed", response_model=DetailedHealthResponse)
async def get_health_detailed(
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
    redis_client: Annotated[redis.Redis, Depends(get_redis)],
) -> DetailedHealthResponse:
    es_ok, es_reason = await ping_es(es)
    redis_ok, redis_reason = await ping_redis(redis_client)

    dependencies: dict[str, str] = {
        "elasticsearch": "ok" if es_ok else "down",
        "redis": "ok" if redis_ok else "down",
    }
    overall_status = "ok" if (es_ok and redis_ok) else "degraded"

    details: dict[str, str] | None = None
    if overall_status == "degraded":
        details = {}
        if not es_ok and es_reason:
            details["elasticsearch"] = es_reason
        if not redis_ok and redis_reason:
            details["redis"] = redis_reason

    return DetailedHealthResponse(
        status=overall_status,
        timestamp=datetime.now(UTC),
        dependencies=dependencies,
        details=details,
    )
