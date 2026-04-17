"""Tests for the Commit 7 streaming pipeline.

These tests exercise the triple:

    POST /api/query/stream   →   {query_id}
    WS   /ws/query/{query_id} →   ordered ProgressEvent JSON frames ending
                                  with a `done` event whose payload is the
                                  full QueryResponse
    GET  /                    →   the single-page UI HTML

For isolation we don't bring up real partition services — instead we
monkey-patch ``QueryExecutor.run`` (at the method level) to emit a
deterministic sequence of events and return a fixed partials/failure
tuple. That keeps the tests fast, hermetic, and purely exercising the
routes + ``ProgressEmitter`` plumbing.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.coordinator.node_client as node_client_mod
from src.coordinator.executor import QueryExecutor
from src.coordinator.progress import ProgressRegistry
from src.coordinator.registry import PartitionRegistry
from src.coordinator.routes import router as coordinator_router
from src.shared.config import CoordinatorSettings
from src.shared.models import (
    PartitionExecuteResponse,
    PartitionMetadata,
    ProgressEvent,
    TimeRange,
)


# ---------------------------------------------------------------------------
# canned partition metadata — keeps tests independent of the real registry
# ---------------------------------------------------------------------------


_PARTITION_IDS: list[str] = ["partition-1", "partition-2"]


def _canned_partitions() -> list[PartitionMetadata]:
    return [
        PartitionMetadata(
            id=pid,
            url=f"http://{pid}:810{i + 1}",
            time_range=TimeRange(
                start="2026-04-01T00:00:00", end="2026-04-30T23:59:59"
            ),
            indexed_fields=["level", "service", "timestamp"],
            healthy=True,
        )
        for i, pid in enumerate(_PARTITION_IDS)
    ]


# ---------------------------------------------------------------------------
# app factory (reuses the monkey-patched node_client so the registry's
# health poll during startup does not hang on unreachable hosts)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _stub_lifespan(app: FastAPI, settings: CoordinatorSettings):
    client = httpx.AsyncClient()
    registry = PartitionRegistry(settings.partition_urls_dict())
    executor = QueryExecutor(
        client=client, request_timeout=settings.request_timeout
    )

    # Replace the registry's partition list with the canned metadata so we
    # don't rely on any real network refresh.
    for p in _canned_partitions():
        registry._partitions[p.id] = p  # type: ignore[attr-defined]

    app.state.settings = settings
    app.state.client = client
    app.state.registry = registry
    app.state.executor = executor
    app.state.progress = ProgressRegistry()

    try:
        yield
    finally:
        await client.aclose()


def _build_stub_app() -> FastAPI:
    settings = CoordinatorSettings(
        coordinator_port=8000,
        partition_urls=(
            "partition-1=http://partition-1:8101,"
            "partition-2=http://partition-2:8102"
        ),
        request_timeout=1.0,
        default_limit=1000,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with _stub_lifespan(app, settings):
            yield

    app = FastAPI(lifespan=lifespan)
    app.include_router(coordinator_router)
    return app


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_executor_run(monkeypatch: pytest.MonkeyPatch):
    """Monkey-patch ``QueryExecutor.run`` to emit a controlled event sequence.

    Emitted order:
        plan_ready → partition_partition-1_started
                   → partition_partition-1_complete
                   → partition_partition-2_started
                   → partition_partition-2_complete
                   → aggregation_start

    (The route suppresses the executor's own ``done`` event and emits its
    own final ``done`` carrying the QueryResponse payload.)

    The stub returns a ``partials`` list that ``aggregator.merge`` can
    handle: two responses with zero rows so the merge result is either
    empty or — if the AST is a COUNT(*) — a single row.
    """

    async def fake_run(
        self: QueryExecutor,
        plan: Any,
        partition_lookup: dict[str, PartitionMetadata],
        progress_callback=None,
    ) -> dict[str, Any]:
        emit = progress_callback or _noop

        await emit(
            ProgressEvent(
                stage="plan_ready",
                payload={"steps": len(plan.steps), "parallelism": plan.parallelism},
            )
        )
        partials: list[tuple[str, PartitionExecuteResponse]] = []
        for pid in _PARTITION_IDS:
            await emit(
                ProgressEvent(
                    stage=f"partition_{pid}_started", payload={"op": "filter"}
                )
            )
            resp = PartitionExecuteResponse(
                rows=[], partial_aggregate=None, records_scanned=1
            )
            partials.append((pid, resp))
            await emit(
                ProgressEvent(
                    stage=f"partition_{pid}_complete",
                    payload={"rows": 0, "records_scanned": 1},
                )
            )

        await emit(ProgressEvent(stage="aggregation_start", payload={}))
        await emit(
            ProgressEvent(
                stage="done",
                payload={"partials": len(partials), "failed": 0},
            )
        )

        return {
            "partials": partials,
            "failed_partitions": [],
            "records_processed": len(partials),
        }

    async def _noop(_event: ProgressEvent) -> None:
        return None

    monkeypatch.setattr(QueryExecutor, "run", fake_run)
    yield


@pytest.fixture
def stub_node_client(monkeypatch: pytest.MonkeyPatch):
    """Neutralise the node_client functions in case any code path reaches
    them during the test (it shouldn't, given ``patched_executor_run``, but
    belt-and-braces keeps this hermetic)."""

    async def fake_check_health(*_a: Any, **_kw: Any) -> bool:
        return True

    async def fake_fetch_metadata(
        client: httpx.AsyncClient, partition_url: str, timeout: float = 2.0
    ) -> PartitionMetadata:
        pid = partition_url.split("//", 1)[-1].split(":", 1)[0]
        return PartitionMetadata(
            id=pid,
            url=partition_url,
            time_range=TimeRange(
                start="2026-04-01T00:00:00", end="2026-04-30T23:59:59"
            ),
            indexed_fields=["level", "service", "timestamp"],
            healthy=True,
        )

    async def fake_post_execute(*_a: Any, **_kw: Any) -> PartitionExecuteResponse:
        return PartitionExecuteResponse(rows=[], records_scanned=0)

    monkeypatch.setattr(node_client_mod, "check_health", fake_check_health)
    monkeypatch.setattr(node_client_mod, "fetch_metadata", fake_fetch_metadata)
    monkeypatch.setattr(node_client_mod, "post_execute", fake_post_execute)
    yield


@pytest.fixture
def client(
    patched_executor_run: None,
    stub_node_client: None,
) -> TestClient:
    app = _build_stub_app()
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _drain_ws(
    ws,
    *,
    max_events: int = 50,
    timeout_s: float = 5.0,
) -> list[dict]:
    """Drain events from a TestClient websocket until the ``done`` frame.

    ``TestClient.websocket_connect`` returns a synchronous helper whose
    ``receive_json`` blocks on the underlying asyncio portal. Any
    well-formed stream ends with a ``done`` event — we bail out then.
    """

    events: list[dict] = []
    deadline = time.monotonic() + timeout_s
    while len(events) < max_events and time.monotonic() < deadline:
        msg = ws.receive_json()
        events.append(msg)
        if msg.get("stage") == "done":
            return events
    return events


def _post_stream(client: TestClient, sql: str) -> str:
    resp = client.post("/api/query/stream", json={"query": sql})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "query_id" in body and isinstance(body["query_id"], str)
    return body["query_id"]


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_stream_event_order(client: TestClient) -> None:
    """The WebSocket must deliver events in the documented order and end
    with a ``done`` event."""

    query_id = _post_stream(
        client, "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 5"
    )

    with client.websocket_connect(f"/ws/query/{query_id}") as ws:
        events = _drain_ws(ws)

    stages = [e["stage"] for e in events]

    # Starts with plan_ready, ends with done (the route's own done).
    assert stages[0] == "plan_ready"
    assert stages[-1] == "done"

    # aggregation_start immediately precedes done.
    assert stages[-2] == "aggregation_start"

    # Every partition must report a started-then-complete pair (in that
    # relative order), though they may interleave across partitions.
    for pid in _PARTITION_IDS:
        started = f"partition_{pid}_started"
        complete = f"partition_{pid}_complete"
        assert started in stages
        assert complete in stages
        assert stages.index(started) < stages.index(complete)

    # Aggregation must come after every partition_*_complete.
    agg_idx = stages.index("aggregation_start")
    for pid in _PARTITION_IDS:
        assert stages.index(f"partition_{pid}_complete") < agg_idx


def test_stream_done_payload_has_query_response(client: TestClient) -> None:
    """The final ``done`` event's payload must be the full QueryResponse."""

    query_id = _post_stream(
        client, "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 5"
    )

    with client.websocket_connect(f"/ws/query/{query_id}") as ws:
        events = _drain_ws(ws)

    done_events = [e for e in events if e["stage"] == "done"]
    assert done_events, "expected at least one `done` event"

    # The FINAL done event is the one that carries the QueryResponse.
    payload = done_events[-1]["payload"]

    for key in (
        "query_id",
        "results",
        "records_processed",
        "optimizations_applied",
        "plan",
        "partial_results",
        "failed_partitions",
    ):
        assert key in payload, f"missing key in done payload: {key}"

    assert payload["query_id"] == query_id
    assert isinstance(payload["results"], list)
    assert isinstance(payload["plan"], dict)
    assert "steps" in payload["plan"]
    assert payload["partial_results"] is False
    assert payload["failed_partitions"] == []


def test_stream_unknown_query_id_closes_websocket(client: TestClient) -> None:
    """Opening a WS for an unknown ``query_id`` must close with code 4404.

    Depending on timing the TestClient may raise either on entering the
    ``websocket_connect`` context (if the server's close frame is in the
    buffer already) or on the first ``receive_*`` call — both paths mean
    "the server rejected this subscription"; we accept either shape.
    """

    from starlette.websockets import WebSocketDisconnect

    caught: WebSocketDisconnect | None = None
    try:
        with client.websocket_connect("/ws/query/does-not-exist") as ws:
            try:
                ws.receive_json()
            except WebSocketDisconnect as exc:
                caught = exc
    except WebSocketDisconnect as exc:
        caught = exc

    assert caught is not None, "expected a WebSocketDisconnect"
    assert caught.code == 4404


def test_get_root_returns_html(client: TestClient) -> None:
    """GET / must return HTML that contains the textarea + Run/Explain."""

    resp = client.get("/")
    assert resp.status_code == 200
    content_type = resp.headers.get("content-type", "")
    assert "text/html" in content_type

    body = resp.text
    assert '<textarea id="sql"' in body
    assert "Run" in body
    assert "Explain" in body


def test_stream_invalid_sql_returns_400(client: TestClient) -> None:
    """Invalid SQL must surface as an HTTP 400 on the POST — parse errors
    should never propagate through the WebSocket."""

    resp = client.post("/api/query/stream", json={"query": "SELECT FROM logs"})
    assert resp.status_code == 400
    assert "detail" in resp.json()
