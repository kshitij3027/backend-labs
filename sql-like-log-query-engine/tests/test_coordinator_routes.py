"""Tests for the coordinator's HTTP routes.

We build the real ``APIRouter`` on top of a stub ``app.state`` so the whole
pipeline — parse → plan → executor → aggregator — runs against an
``httpx.MockTransport`` without needing a real partition container. The
app's own lifespan is bypassed; we install ``client/registry/executor`` on
``app.state`` by hand so there's no race with the deprecated
``on_event("startup")`` hook.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from src.coordinator.executor import QueryExecutor
from src.coordinator.progress import ProgressRegistry
from src.coordinator.registry import PartitionRegistry
from src.coordinator.routes import router
from src.shared.config import CoordinatorSettings


# ---------------------------------------------------------------------------
# mock partition transport
# ---------------------------------------------------------------------------


def _metadata_payload(pid: str, port: int) -> dict:
    return {
        "id": pid,
        "url": f"http://{pid}:{port}",
        "time_range": {
            "start": "2026-04-01T00:00:00",
            "end": "2026-04-30T23:59:59",
        },
        "indexed_fields": ["level", "service", "timestamp"],
        "healthy": True,
    }


def _mock_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        host = request.url.host or ""

        if path == "/health":
            return httpx.Response(200, json={"status": "ok"})

        if path == "/metadata":
            port = {"partition-1": 8101, "partition-2": 8102}.get(host, 8100)
            return httpx.Response(200, json=_metadata_payload(host, port))

        if path == "/execute":
            body = json.loads(request.content.decode() or "{}")
            if body.get("aggregation"):
                return httpx.Response(
                    200,
                    json={
                        "rows": [],
                        "partial_aggregate": {
                            "groups": None,
                            "aggregates": {"COUNT(*)": 5},
                            "record_count": 5,
                            "count": 5,
                            "sums": {},
                            "mins": {},
                            "maxs": {},
                            "functions": [["COUNT", "*"]],
                            "group_by": [],
                        },
                        "records_scanned": 5,
                    },
                )
            return httpx.Response(
                200,
                json={
                    "rows": [
                        {
                            "level": "ERROR",
                            "service": "api",
                            "message": "boom",
                            "duration_ms": 42,
                        }
                    ],
                    "partial_aggregate": None,
                    "records_scanned": 1,
                },
            )

        return httpx.Response(404, json={"detail": "not found"})

    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# test-only app factory
# ---------------------------------------------------------------------------


def _build_test_app(settings: CoordinatorSettings) -> FastAPI:
    """Build a FastAPI instance wired against the mock transport."""

    transport = _mock_transport()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(transport=transport)
        registry = PartitionRegistry(settings.partition_urls_dict())
        executor = QueryExecutor(
            client=client, request_timeout=settings.request_timeout
        )

        app.state.settings = settings
        app.state.client = client
        app.state.registry = registry
        app.state.executor = executor
        app.state.progress = ProgressRegistry()

        # Prime the registry so the very first request sees live partitions.
        await registry.refresh(client)

        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)
    return app


@pytest.fixture
def client(coordinator_settings: CoordinatorSettings):
    # Narrow the registry to two partitions for simpler assertions.
    settings = coordinator_settings.model_copy(
        update={
            "partition_urls": (
                "partition-1=http://partition-1:8101,"
                "partition-2=http://partition-2:8102"
            )
        }
    )
    app = _build_test_app(settings)
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# health / partitions
# ---------------------------------------------------------------------------


def test_health_returns_partitions_list(client: TestClient) -> None:
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert isinstance(body["partitions"], list)
    assert {p["id"] for p in body["partitions"]} == {"partition-1", "partition-2"}
    for p in body["partitions"]:
        assert p["healthy"] is True
        assert "url" in p


def test_partitions_endpoint_returns_list(client: TestClient) -> None:
    resp = client.get("/api/partitions")
    assert resp.status_code == 200
    partitions = resp.json()
    assert isinstance(partitions, list)
    assert len(partitions) == 2
    assert partitions[0]["id"] == "partition-1"


# ---------------------------------------------------------------------------
# /api/query
# ---------------------------------------------------------------------------


def test_query_returns_full_envelope(client: TestClient) -> None:
    resp = client.post(
        "/api/query",
        json={"query": "SELECT * FROM logs WHERE level = 'ERROR'"},
    )
    assert resp.status_code == 200
    body = resp.json()

    for key in (
        "query_id",
        "results",
        "records_processed",
        "execution_time_ms",
        "optimizations_applied",
        "plan",
        "partial_results",
        "failed_partitions",
    ):
        assert key in body, f"missing key: {key}"

    assert body["query_id"]
    assert isinstance(body["results"], list)
    # 2 partitions × 1 row each.
    assert len(body["results"]) == 2
    assert body["records_processed"] == 2
    assert body["partial_results"] is False
    assert body["failed_partitions"] == []
    assert body["execution_time_ms"] >= 0.0
    notes = body["optimizations_applied"]
    assert any("Partition pruning" in note for note in notes)
    assert any("Predicate pushdown" in note for note in notes)
    plan = body["plan"]
    assert "steps" in plan
    assert plan["parallelism"] >= 1


def test_query_with_aggregation(client: TestClient) -> None:
    resp = client.post(
        "/api/query",
        json={"query": "SELECT COUNT(*) AS n FROM logs"},
    )
    assert resp.status_code == 200
    body = resp.json()
    # 5 rows per partition × 2 partitions.
    assert body["results"] == [{"n": 10}]
    assert body["records_processed"] == 10


def test_query_invalid_sql_returns_400(client: TestClient) -> None:
    resp = client.post("/api/query", json={"query": "SELECT FROM logs"})
    assert resp.status_code == 400
    assert "detail" in resp.json()


def test_query_empty_string_rejected(client: TestClient) -> None:
    # Pydantic rejects empty query (min_length=1) → 422.
    resp = client.post("/api/query", json={"query": ""})
    assert resp.status_code in (400, 422)


# ---------------------------------------------------------------------------
# /api/explain
# ---------------------------------------------------------------------------


def test_explain_returns_plan_text(client: TestClient) -> None:
    resp = client.post(
        "/api/explain",
        json={"query": "SELECT COUNT(*) FROM logs GROUP BY service"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "plan_text" in body
    assert isinstance(body["plan_text"], str)
    assert "Execution plan" in body["plan_text"]
    assert "plan" in body
    assert "steps" in body["plan"]


def test_explain_invalid_sql_returns_400(client: TestClient) -> None:
    resp = client.post("/api/explain", json={"query": "WHERE x"})
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# root UI
# ---------------------------------------------------------------------------


def test_root_serves_ui_html(client: TestClient) -> None:
    """GET / returns the single-page UI — or, if the template directory
    didn't get mounted onto ``app.state.templates`` (which happens in the
    bare ``_build_test_app`` factory used by these tests), the fallback
    HTML stub. Either way the response is HTML and contains the sentinel
    elements the Chrome smoke test looks for.
    """

    resp = client.get("/")
    assert resp.status_code == 200
    content_type = resp.headers.get("content-type", "")
    assert "text/html" in content_type
    body = resp.text
    assert '<textarea id="sql"' in body
    # Button labels — either from the real template or the fallback stub.
    assert "Run" in body
    assert "Explain" in body


# ---------------------------------------------------------------------------
# no healthy partitions → still a valid response
# ---------------------------------------------------------------------------


def test_query_with_no_healthy_partitions_returns_empty_results() -> None:
    """If every partition is unreachable the coordinator must still return
    a valid ``QueryResponse`` — empty results, zero records, all partitions
    listed in ``failed_partitions`` is not required (they never ran) but
    ``results`` must be empty and the status is 200.
    """

    # Build settings pointing to URLs that always 500.
    settings = CoordinatorSettings(
        coordinator_port=8000,
        partition_urls="partition-dead=http://partition-dead:9999",
        request_timeout=0.5,
    )

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "always down"})

    transport = httpx.MockTransport(handler)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(transport=transport)
        registry = PartitionRegistry(settings.partition_urls_dict())
        executor = QueryExecutor(
            client=client, request_timeout=settings.request_timeout
        )
        app.state.settings = settings
        app.state.client = client
        app.state.registry = registry
        app.state.executor = executor
        app.state.progress = ProgressRegistry()
        await registry.refresh(client)  # partition will be marked unhealthy
        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)

    with TestClient(app) as c:
        resp = c.post("/api/query", json={"query": "SELECT * FROM logs"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["results"] == []
        assert body["records_processed"] == 0
