"""HTTP endpoints for faceted search.

Exposes two routes that both return facet-counting responses:

* ``POST /api/search`` — full search: logs + facets + pagination.
  Takes a ``SearchRequest`` body.
* ``GET /api/facets`` — facet counts only. Takes filters as query
  params (facet dims accept comma-separated values), for clients that
  want to refresh the sidebar without a row fetch.

Both routes are wrapped in a cache-aside layer backed by Redis (see
``src/storage/redis_cache.py``). Cache outages are transparent — the
handler still returns a valid response, ``cached=False``, and the
error counter bumps in ``storage.redis_cache.stats.errors``.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from src.config import settings
from src.models import FacetsOnlyResponse, SearchRequest, SearchResponse
from src.search import facet_counter
from src.storage import redis_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["search"])


@router.post("/search", response_model=SearchResponse)
async def search_logs(
    request: Request,
    payload: SearchRequest,
) -> SearchResponse:
    """Run a faceted search and return logs + per-dimension counts.

    The ``payload.filters`` dict may contain any subset of the
    FACET_DIMS (service, level, region, latency_bucket, hour_bucket);
    unknown keys are ignored silently. Pagination is keyset-based:
    pass ``cursor`` from the previous response's ``next_cursor`` to
    fetch the next page.

    Responses are cached in Redis keyed on the full request body so
    identical queries short-circuit to an in-memory deserialize.
    """
    t0 = time.perf_counter_ns()
    db = request.app.state.db
    redis_client = getattr(request.app.state, "redis", None)

    # Key off the full request body so any filter/cursor/query change
    # produces a distinct entry. ``sort_keys`` in ``make_key`` keeps
    # Python dict iteration order out of the hash.
    key = redis_cache.make_key("search", payload.model_dump())

    async def _compute() -> Dict[str, Any]:
        response = await facet_counter.search(
            conn=db,
            filters=payload.filters,
            query=payload.query,
            ts_start=payload.ts_start,
            ts_end=payload.ts_end,
            cursor=payload.cursor,
            limit=payload.limit,
        )
        return response.model_dump()

    cached_payload, was_hit = await redis_cache.get_or_compute(
        client=redis_client,
        key=key,
        compute=_compute,
        ttl=settings.facet_cache_ttl,
    )

    # On a hit, overwrite ``query_time_ms`` with the true request-
    # level elapsed time so p95 numbers reflect cache speed, not the
    # original miss cost we happened to capture at insert time.
    if was_hit:
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000
        cached_payload["query_time_ms"] = round(elapsed_ms, 3)
    cached_payload["cached"] = was_hit

    logger.info(
        "search filters=%s query=%s limit=%d returned=%d cached=%s query_time_ms=%.3f",
        payload.filters,
        payload.query,
        payload.limit,
        len(cached_payload.get("logs", [])),
        was_hit,
        cached_payload.get("query_time_ms", 0.0),
    )
    return SearchResponse(**cached_payload)


def _split_csv(raw: Optional[str]) -> List[str]:
    """Split a comma-separated query-param value into a clean list.

    FastAPI lets repeatable params pile up as lists, but a single
    ``?service=payments,auth`` string is friendlier for humans on
    the command line, so we accept both forms here.
    """
    if raw is None:
        return []
    return [v for v in (item.strip() for item in raw.split(",")) if v]


@router.get("/facets", response_model=FacetsOnlyResponse)
async def get_facets(
    request: Request,
    query: Optional[str] = Query(None, description="Free-text message search."),
    ts_start: Optional[int] = Query(None, description="Epoch-seconds lower bound."),
    ts_end: Optional[int] = Query(None, description="Epoch-seconds upper bound."),
    service: Optional[str] = Query(None, description="Comma-separated services."),
    level: Optional[str] = Query(None, description="Comma-separated levels."),
    region: Optional[str] = Query(None, description="Comma-separated regions."),
    latency_bucket: Optional[str] = Query(
        None, description="Comma-separated latency buckets."
    ),
    hour_bucket: Optional[str] = Query(
        None, description="Comma-separated hour buckets (0-23)."
    ),
) -> FacetsOnlyResponse:
    """Return facet counts only (no log rows) for the given filter set.

    Filters arrive as comma-separated query parameters for ease of
    use from ``curl`` and the frontend's URL-sync logic. The response
    is shaped identically to the ``facets`` block of ``/api/search``
    and is cached with the same TTL.
    """
    t0 = time.perf_counter_ns()
    filters = {
        "service": _split_csv(service),
        "level": _split_csv(level),
        "region": _split_csv(region),
        "latency_bucket": _split_csv(latency_bucket),
        # ``_split_csv`` returns strings; facet_counter coerces to int
        # for hour_bucket when binding, so we don't need to convert here.
        "hour_bucket": _split_csv(hour_bucket),
    }
    # Drop empty lists so _normalize_filters sees a clean dict.
    filters = {k: v for k, v in filters.items() if v}

    db = request.app.state.db
    redis_client = getattr(request.app.state, "redis", None)

    # Cache-key payload mirrors the logical request shape (filters +
    # free-text + time window). Using ``sort_keys=True`` inside
    # ``make_key`` keeps us order-independent.
    key_payload = {
        "filters": filters,
        "query": query,
        "ts_start": ts_start,
        "ts_end": ts_end,
    }
    key = redis_cache.make_key("facets_only", key_payload)

    async def _compute() -> Dict[str, Any]:
        response = await facet_counter.facets_only(
            conn=db,
            filters=filters,
            query=query,
            ts_start=ts_start,
            ts_end=ts_end,
        )
        return response.model_dump()

    cached_payload, was_hit = await redis_cache.get_or_compute(
        client=redis_client,
        key=key,
        compute=_compute,
        ttl=settings.facet_cache_ttl,
    )

    if was_hit:
        elapsed_ms = (time.perf_counter_ns() - t0) / 1_000_000
        cached_payload["query_time_ms"] = round(elapsed_ms, 3)
    cached_payload["cached"] = was_hit

    logger.info(
        "facets filters=%s query=%s cached=%s query_time_ms=%.3f",
        filters,
        query,
        was_hit,
        cached_payload.get("query_time_ms", 0.0),
    )
    return FacetsOnlyResponse(**cached_payload)
