"""HTTP routes for the coordinator service.

The endpoints wire together the pre-built pipeline:

    ``parse_sql`` → ``QueryPlanner(partitions).plan`` → ``QueryExecutor.run``
    → ``aggregator.merge`` → ``QueryResponse``

Commit 7 adds the streaming side:

    ``POST /api/query/stream`` kicks off ``QueryExecutor.run`` in the
    background with a ``progress_callback`` bound to a per-query
    ``ProgressEmitter`` registered in ``app.state.progress``; clients then
    open ``WS /ws/query/{id}`` to receive the progress events in order and
    the final ``QueryResponse`` as the ``done`` event's payload.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from src.parser import parse_sql
from src.parser.errors import ParseError
from src.planner import QueryPlanner, render_plan_text
from src.shared.models import (
    ExecutionPlan,
    PartitionMetadata,
    ProgressEvent,
    QueryRequest,
    QueryResponse,
)

from . import aggregator
from .progress import ProgressEmitter, ProgressRegistry


logger = logging.getLogger(__name__)

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
# streaming query
# ---------------------------------------------------------------------------


@router.post("/api/query/stream")
async def api_query_stream(
    body: QueryRequest, request: Request
) -> dict[str, str]:
    """Kick off a streaming query execution.

    Parse + plan the query synchronously (so invalid SQL surfaces a 400 to the
    POST caller rather than over the WebSocket). If the plan succeeds we
    mint a fresh ``query_id``, register a ``ProgressEmitter`` under it, and
    launch the executor as a background task that will emit progress events
    and finally a ``done`` event carrying the complete ``QueryResponse``.
    The caller uses the returned ``query_id`` to open
    ``/ws/query/{query_id}`` and drain the events.
    """

    try:
        ast_root = parse_sql(body.query)
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    registry = request.app.state.registry
    executor = request.app.state.executor
    progress: ProgressRegistry = request.app.state.progress

    healthy = registry.healthy_partitions()
    plan = QueryPlanner(healthy).plan(ast_root)
    partition_lookup: dict[str, PartitionMetadata] = {p.id: p for p in healthy}

    query_id = uuid.uuid4().hex
    emitter = await progress.create(query_id)

    # Schedule the background run. We deliberately do NOT await — the
    # response must return ``{query_id}`` immediately so the UI can open the
    # WebSocket before events start flowing. Per-query emitter buffering
    # means events emitted before the WS connects are queued, not lost.
    asyncio.create_task(
        _run_query_streamed(
            executor=executor,
            plan=plan,
            partition_lookup=partition_lookup,
            ast_root=ast_root,
            query_id=query_id,
            emitter=emitter,
        )
    )

    return {"query_id": query_id}


async def _run_query_streamed(
    *,
    executor: Any,
    plan: ExecutionPlan,
    partition_lookup: dict[str, PartitionMetadata],
    ast_root: Any,
    query_id: str,
    emitter: ProgressEmitter,
) -> None:
    """Run a query in the background, streaming progress via ``emitter``.

    The executor itself emits ``plan_ready`` / ``partition_*_started`` /
    ``partition_*_complete`` / ``aggregation_start`` / ``done`` events as
    side-effects. After the executor's own ``done`` event we emit one final
    ``done`` event carrying the complete ``QueryResponse`` payload — that's
    the event the UI uses to render the results table.
    """

    t_start = time.perf_counter()
    final_response: QueryResponse | None = None

    async def _emit_intermediate(event: ProgressEvent) -> None:
        # We want to let the executor's ``plan_ready``, ``partition_*`` and
        # ``aggregation_start`` events flow through unchanged, but we
        # suppress the executor's synthetic ``done`` event — this route
        # emits its own richer ``done`` below carrying the full
        # QueryResponse payload.
        if event.stage == "done":
            return
        await emitter.emit(event)

    try:
        run_result = await executor.run(
            plan=plan,
            partition_lookup=partition_lookup,
            progress_callback=_emit_intermediate,
        )

        partials = run_result.get("partials", [])
        failed_partitions: list[str] = list(run_result.get("failed_partitions", []))
        records_processed: int = int(run_result.get("records_processed", 0))

        results = aggregator.merge(partials, ast_root)

        elapsed_ms = (time.perf_counter() - t_start) * 1000.0

        final_response = QueryResponse(
            query_id=query_id,
            results=results,
            records_processed=records_processed,
            execution_time_ms=round(elapsed_ms, 3),
            optimizations_applied=list(plan.optimization_notes),
            plan=plan,
            partial_results=bool(failed_partitions),
            failed_partitions=failed_partitions,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("streaming query failed: query_id=%s", query_id)
        await emitter.emit(
            ProgressEvent(
                stage="error",
                payload={"error": str(exc), "query_id": query_id},
            )
        )
    finally:
        # Emit the final ``done`` event with the full QueryResponse payload
        # (or an empty payload on failure) and close the emitter so the
        # WebSocket iterator terminates cleanly. We deliberately do NOT
        # remove the emitter from the registry here — a WS subscriber may
        # connect slightly after this coroutine finishes, and the buffered
        # events in the queue still need to be drainable. The WS handler is
        # responsible for ``progress_registry.remove(query_id)`` once it
        # has finished draining the iterator.
        if final_response is not None:
            payload = final_response.model_dump(mode="json")
        else:
            payload = {"query_id": query_id}
        await emitter.emit(ProgressEvent(stage="done", payload=payload))
        await emitter.close()


@router.websocket("/ws/query/{query_id}")
async def ws_query(websocket: WebSocket, query_id: str) -> None:
    """Stream ProgressEvents for ``query_id`` over a WebSocket.

    The handler:
      1. Accepts the socket.
      2. Looks up the emitter in ``app.state.progress``.
      3. If unknown → closes with 4404 (application-level "not found").
      4. Otherwise iterates events and forwards each as a JSON frame of the
         shape ``{"stage": ..., "payload": ...}``.
      5. Closes cleanly when the iterator ends (after ``done``).
    """

    progress: ProgressRegistry = websocket.app.state.progress
    emitter = progress.get(query_id)

    await websocket.accept()

    if emitter is None:
        # Unknown query id — tell the client explicitly via a custom
        # application close code. 4000–4999 is the reserved range for
        # application-defined close codes per RFC 6455.
        await websocket.close(code=4404, reason="unknown query_id")
        return

    try:
        async for event in emitter.iter():
            await websocket.send_json(
                {"stage": event.stage, "payload": event.payload}
            )
    except WebSocketDisconnect:
        # Client went away mid-stream — nothing to do; just exit.
        return
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception(
            "websocket handler for %s failed: %s", query_id, exc
        )
    finally:
        # The emitter has finished (sentinel received) or the client
        # disconnected — either way, remove the registry entry to free
        # memory and close the socket cleanly.
        try:
            await progress.remove(query_id)
        except Exception:
            pass
        try:
            await websocket.close()
        except Exception:
            # Already closed — swallow the double-close.
            pass


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def root(request: Request) -> Any:
    """Serve the single-page UI.

    The template lives in ``src/templates/index.html``. If the coordinator
    was packaged without UI assets (``app.state.templates is None``) we fall
    back to a small HTML stub that still carries the sentinel elements our
    tests look for (``<textarea id="sql">``, Run / Explain buttons) so the
    smoke test remains meaningful.
    """

    templates = getattr(request.app.state, "templates", None)
    if templates is None:
        return HTMLResponse(_UI_FALLBACK_HTML)

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "title": "Distributed SQL Log Query Engine"},
    )


_UI_FALLBACK_HTML = """\
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Distributed SQL Log Query Engine</title>
  </head>
  <body>
    <h1>Distributed SQL Log Query Engine</h1>
    <p>UI assets missing from this deployment. API endpoints still work.</p>
    <textarea id="sql" rows="4" cols="80"></textarea>
    <button id="run-btn">Run</button>
    <button id="explain-btn">Explain</button>
  </body>
</html>
"""
