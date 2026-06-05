"""The ``POST /api/query`` endpoint — fan a query across a tenant's partitions.

A query is resolved by the :class:`~src.query_engine.QueryEngine`, which
classifies it, reads every (non-skippable) partition through its own format's
backend, and either returns the unioned rows or computes the requested
aggregations. The engine yields a :class:`~src.query_engine.QueryResult` whose
``rows`` xor ``aggregates`` is populated plus a ``meta`` accounting dict; those
map directly onto :class:`~src.api.schemas.QueryResponse` /
:class:`~src.api.schemas.QueryMeta`. Malformed bodies are rejected as ``422`` by
Pydantic's validation of :class:`~src.models.QueryRequest`.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from src.api.dependencies import get_query_engine
from src.api.schemas import QueryMeta, QueryResponse
from src.models import QueryRequest
from src.query_engine import QueryEngine

router = APIRouter(prefix="/api", tags=["query"])


@router.post("/query", response_model=QueryResponse)
async def query(
    req: QueryRequest,
    engine: Annotated[QueryEngine, Depends(get_query_engine)],
) -> QueryResponse:
    """Run ``req`` against ``req.tenant`` and return rows *or* aggregates.

    Delegates to :meth:`~src.query_engine.QueryEngine.query`, forwarding the
    optional projection / filters / aggregations / grouping / limit. The
    resulting :class:`~src.query_engine.QueryResult` is repackaged: its ``meta``
    dict is unpacked into :class:`~src.api.schemas.QueryMeta`, and ``rows`` /
    ``aggregates`` pass through (exactly one is populated).
    """
    qr = await engine.query(
        req.tenant,
        columns=req.columns,
        filters=req.filters,
        aggregations=req.aggregations,
        group_by=req.group_by,
        limit=req.limit,
    )
    return QueryResponse(
        rows=qr.rows,
        aggregates=qr.aggregates,
        meta=QueryMeta(**qr.meta),
    )
