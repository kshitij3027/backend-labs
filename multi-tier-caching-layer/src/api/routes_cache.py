"""The ``/cache/*`` management endpoints.

* ``GET  /cache/stats``      — overall + per-tier hit/miss, memory, timing.
* ``GET  /cache/hot``        — ranked hot keys from the heuristic engine.
* ``POST /cache/warm``       — proactively warm specific queries (or a sweep).
* ``POST /cache/invalidate`` — selective invalidation by glob pattern / tags.

Stats are assembled from the metrics snapshot plus the live L1/L2 tier ``stats``
and a best-effort L3 row count. Per project §8 the ``performance`` block always
carries ``overall_hit_rate`` and ``total_requests``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from src.api.dependencies import (
    get_cache_manager,
    get_l1,
    get_l2,
    get_metrics,
    get_patterns,
    get_pg_pool,
    get_warmer,
)
from src.api.schemas import (
    HotKeysResponse,
    InvalidateResponse,
    StatsResponse,
    WarmResponse,
)
from src.cache_manager import CacheManager
from src.l1_cache import L1Cache
from src.l2_redis import L2Redis
from src.metrics import Metrics
from src.models import InvalidateRequest, WarmRequest
from src.patterns import PatternEngine
from src.warmer import Warmer

router = APIRouter(prefix="/cache", tags=["cache"])


async def _l3_count(pg_pool) -> int:
    """Best-effort count of materialized L3 rows (0 on any failure)."""
    try:
        async with pg_pool.acquire() as conn:
            value = await conn.fetchval("SELECT count(*) FROM precomputed_aggregates")
        return int(value or 0)
    except Exception:  # noqa: BLE001 — stats must never 500 on an L3 hiccup
        return 0


@router.get("/stats", response_model=StatsResponse)
async def cache_stats(
    request: Request,
    metrics: Annotated[Metrics, Depends(get_metrics)],
    l1: Annotated[L1Cache, Depends(get_l1)],
    l2: Annotated[L2Redis, Depends(get_l2)],
    pg_pool: Annotated[object, Depends(get_pg_pool)],
) -> StatsResponse:
    """Assemble the aggregate cache statistics surface.

    Combines :meth:`Metrics.snapshot` (overall + per-tier hit/miss + timing +
    degradation) with the live :meth:`L1Cache.stats` / :meth:`L2Redis.stats`
    tier detail and a best-effort L3 row count, then fills the ``memory`` block
    (L1 value footprint, the configured cap, and a best-effort total).
    """
    snap = metrics.snapshot()
    l1_stats = l1.stats()
    l2_stats = l2.stats()
    l3_rows = await _l3_count(pg_pool)

    # Merge live tier detail into the metrics tier breakdown.
    tiers = dict(snap.get("tiers", {}))
    tiers["l1"] = {**tiers.get("l1", {}), **l1_stats}
    tiers["l2"] = {**tiers.get("l2", {}), **l2_stats}
    tiers["l3"] = {**tiers.get("l3", {}), "rows": l3_rows}

    # Memory accounting. L1 footprint is exact-ish (JSON-encoded values); L2 is
    # best-effort from Redis INFO (None when degraded/unavailable).
    settings = request.app.state.settings
    l1_mb = float(l1_stats.get("approx_mb", 0.0))
    cap_mb = float(settings.cache_mem_cap_mb)
    total_mb = l1_mb
    l2_used = await l2.mem_used_bytes()
    if l2_used is not None:
        total_mb += l2_used / (1024 * 1024)

    memory = {
        "l1_mb": l1_mb,
        "cap_mb": cap_mb,
        "total_mb": total_mb,
    }

    return StatsResponse(
        performance=snap.get("performance", {}),
        tiers=tiers,
        memory=memory,
        timing_ms=snap.get("timing_ms", {}),
        degraded=snap.get("degraded", False),
        alert=snap.get("alert"),
    )


@router.get("/hot", response_model=HotKeysResponse)
async def cache_hot(
    patterns: Annotated[PatternEngine, Depends(get_patterns)],
) -> HotKeysResponse:
    """Return the hottest tracked keys, ranked by the frecency-with-cost score."""
    return HotKeysResponse(hot=patterns.hot_keys(top_n=20))


@router.post("/warm", response_model=WarmResponse)
async def cache_warm(
    req: WarmRequest,
    warmer: Annotated[Warmer, Depends(get_warmer)],
) -> WarmResponse:
    """Proactively warm explicit queries, or run one sweep when none are given.

    Each requested query is replayed through ``CacheManager.get`` (pulling L3 ->
    L1/L2 where possible). With an empty ``queries`` list, the warmer runs a
    single recommendation-driven sweep instead.
    """
    items = [{"query": q.query, "params": q.params} for q in req.queries]
    warmed = await warmer.warm_now(items if items else None)
    return WarmResponse(warmed=warmed)


@router.post("/invalidate", response_model=InvalidateResponse)
async def cache_invalidate(
    req: InvalidateRequest,
    cm: Annotated[CacheManager, Depends(get_cache_manager)],
) -> InvalidateResponse:
    """Evict entries by glob ``pattern`` and/or ``tags`` across all tiers.

    Requires at least one of ``pattern``/``tags`` — the request model enforces
    this (a bare body is a ``422``); a manager-level ``ValueError`` maps to
    ``400``. Returns the per-tier removal tally.
    """
    try:
        counts = await cm.invalidate(pattern=req.pattern, tags=req.tags or None)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return InvalidateResponse(**counts)
