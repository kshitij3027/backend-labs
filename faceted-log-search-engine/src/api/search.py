"""HTTP endpoints for faceted search.

Exposes two routes that both return facet-counting responses:

* ``POST /api/search`` — full search: logs + facets + pagination.
  Takes a ``SearchRequest`` body.
* ``GET /api/facets`` — facet counts only. Takes filters as query
  params (facet dims accept comma-separated values), for clients that
  want to refresh the sidebar without a row fetch.

Both routes share the same underlying ``facet_counter`` engine so the
excluded-self semantics are identical between them.
"""

from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import APIRouter, Query, Request

from src.models import FacetsOnlyResponse, SearchRequest, SearchResponse
from src.search import facet_counter

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
    """
    db = request.app.state.db
    response = await facet_counter.search(
        conn=db,
        filters=payload.filters,
        query=payload.query,
        ts_start=payload.ts_start,
        ts_end=payload.ts_end,
        cursor=payload.cursor,
        limit=payload.limit,
    )
    logger.info(
        "search filters=%s query=%s limit=%d returned=%d query_time_ms=%.3f",
        payload.filters,
        payload.query,
        payload.limit,
        len(response.logs),
        response.query_time_ms,
    )
    return response


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
    is shaped identically to the ``facets`` block of ``/api/search``.
    """
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
    response = await facet_counter.facets_only(
        conn=db,
        filters=filters,
        query=query,
        ts_start=ts_start,
        ts_end=ts_end,
    )
    logger.info(
        "facets filters=%s query=%s query_time_ms=%.3f",
        filters,
        query,
        response.query_time_ms,
    )
    return response
