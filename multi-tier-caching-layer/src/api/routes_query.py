"""The ``POST /query`` endpoint — the read-through entry point over HTTP.

A query is resolved through the full tier hierarchy by the
:class:`~src.cache_manager.CacheManager`; the response carries the result plus
the :class:`~src.models.CacheMeta` describing *which* tier served it and how long
it took. An unsupported query surfaces as a :class:`ValueError` from the backend,
which is mapped to a ``400``; Pydantic handles malformed bodies as ``422``.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from src.api.dependencies import get_cache_manager
from src.cache_manager import CacheManager
from src.models import CacheMeta, QueryRequest, QueryResponse

router = APIRouter(tags=["query"])


@router.post("/query", response_model=QueryResponse)
async def run_query(
    req: QueryRequest,
    cm: Annotated[CacheManager, Depends(get_cache_manager)],
) -> QueryResponse:
    """Resolve ``(query, params)`` through L1 -> L2 -> L3 -> backend.

    Returns the result plus cache metadata (serving tier, elapsed ms, key, and
    the current L2-degraded flag). An unknown query raises ``ValueError`` in the
    backend, which we translate to ``400 Bad Request``.
    """
    try:
        res = await cm.get(req.query, req.params)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return QueryResponse(
        result=res.result,
        meta=CacheMeta(
            tier=res.tier,
            elapsed_ms=res.elapsed_ms,
            key=res.key,
            degraded=res.degraded,
        ),
    )
