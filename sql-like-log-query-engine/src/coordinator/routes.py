"""HTTP routes for the coordinator service.

The endpoints wire together the pre-built pipeline:

    ``parse_sql`` → ``QueryPlanner(partitions).plan`` → ``QueryExecutor.run``
    → ``aggregator.merge`` → ``QueryResponse``
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from src.parser import parse_sql
from src.parser.errors import ParseError
from src.planner import QueryPlanner, render_plan_text
from src.shared.models import (
    ExecutionPlan,
    PartitionMetadata,
    QueryRequest,
    QueryResponse,
)

from . import aggregator


router = APIRouter()


# ---------------------------------------------------------------------------
# health / partitions
# ---------------------------------------------------------------------------


@router.get("/api/health")
async def api_health(request: Request) -> dict[str, Any]:
    registry = request.app.state.registry
    partitions = registry.partitions()
    return {
        "status": "ok",
        "partitions": [
            {"id": p.id, "healthy": p.healthy, "url": p.url} for p in partitions
        ],
    }


@router.get("/api/partitions")
async def api_partitions(request: Request) -> list[dict[str, Any]]:
    registry = request.app.state.registry
    return [p.model_dump(mode="json") for p in registry.partitions()]


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@router.post("/api/query", response_model=QueryResponse)
async def api_query(body: QueryRequest, request: Request) -> QueryResponse:
    t_start = time.perf_counter()

    try:
        ast_root = parse_sql(body.query)
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    registry = request.app.state.registry
    executor = request.app.state.executor

    healthy = registry.healthy_partitions()
    plan = QueryPlanner(healthy).plan(ast_root)

    partition_lookup: dict[str, PartitionMetadata] = {p.id: p for p in healthy}

    run_result = await executor.run(
        plan=plan,
        partition_lookup=partition_lookup,
        progress_callback=None,
    )

    partials = run_result["partials"]
    failed_partitions: list[str] = list(run_result.get("failed_partitions", []))
    records_processed: int = int(run_result.get("records_processed", 0))

    # If every healthy partition reported zero rows or we have no partials at
    # all (e.g. no healthy nodes), merge() still handles it cleanly.
    results = aggregator.merge(partials, ast_root)

    elapsed_ms = (time.perf_counter() - t_start) * 1000.0
    query_id = uuid.uuid4().hex

    return QueryResponse(
        query_id=query_id,
        results=results,
        records_processed=records_processed,
        execution_time_ms=round(elapsed_ms, 3),
        optimizations_applied=list(plan.optimization_notes),
        plan=plan,
        partial_results=bool(failed_partitions),
        failed_partitions=failed_partitions,
    )


@router.post("/api/explain")
async def api_explain(body: QueryRequest, request: Request) -> dict[str, Any]:
    try:
        ast_root = parse_sql(body.query)
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    registry = request.app.state.registry
    healthy = registry.healthy_partitions()
    plan: ExecutionPlan = QueryPlanner(healthy).plan(ast_root)

    return {
        "plan_text": render_plan_text(plan),
        "plan": plan.model_dump(),
    }


# ---------------------------------------------------------------------------
# UI placeholder (filled in Commit 7)
# ---------------------------------------------------------------------------


@router.get("/")
async def root() -> JSONResponse:
    return JSONResponse(
        status_code=501,
        content={"detail": "UI coming in commit 7"},
    )
