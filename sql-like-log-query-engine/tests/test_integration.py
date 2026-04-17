"""Full-stack integration tests.

This module boots the real coordinator FastAPI app together with three real
partition FastAPI apps entirely in-process. No sockets are opened.

How the wiring works:

* Each partition is a regular ``FastAPI`` built by
  :func:`src.partition.app.create_partition_app`. We bypass its lifespan and
  populate ``app.state`` ourselves with the same resources the lifespan
  would build — ``storage``, ``executor``, ``metadata`` — so routes see
  fully-initialised state without needing the lifespan thread.
* Each partition is then wrapped in an ``httpx.ASGITransport``. The
  transport speaks HTTP directly to the ASGI app without opening a socket.
* The coordinator's scatter-gather logic expects a single
  ``httpx.AsyncClient`` able to reach any of ``http://partition-{N}:{port}``.
  We build a single :class:`httpx.MockTransport` whose handler inspects the
  request URL, looks up the matching partition's ``ASGITransport``, and
  forwards the request via ``handle_async_request``. The coordinator code
  itself runs completely unchanged.
* We bypass the coordinator's own ``lifespan`` (which would build a fresh
  client and poll in the background) and attach ``client / registry /
  executor / progress`` to ``app.state`` by hand — exactly the pattern
  already used by ``tests/test_coordinator_routes.py``.

The result: a single ``TestClient`` talking to the coordinator exercises
``parse → plan → scatter-gather → aggregate`` end to end, with real
partition data.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.coordinator.executor import QueryExecutor
from src.coordinator.progress import ProgressRegistry
from src.coordinator.registry import PartitionRegistry
from src.coordinator.routes import router as coordinator_router
from src.partition.app import create_partition_app
from src.partition.data_generator import generate_logs
from src.partition.executor import LocalExecutor
from src.partition.storage import LogStorage
from src.shared.config import CoordinatorSettings, PartitionSettings
from src.shared.models import PartitionMetadata, TimeRange


# Small deterministic dataset — 200 rows/partition keeps the suite under 5s
# while still producing plenty of per-level / per-service variety.
_ROWS_PER_PARTITION = 200


# --- partition spec --------------------------------------------------------


_PARTITION_SPECS: list[dict[str, Any]] = [
    {
        "id": "partition-1",
        "port": 8101,
        "time_start": "2026-04-01T00:00:00",
        "time_end": "2026-04-07T23:59:59",
    },
    {
        "id": "partition-2",
        "port": 8102,
        "time_start": "2026-04-08T00:00:00",
        "time_end": "2026-04-14T23:59:59",
    },
    {
        "id": "partition-3",
        "port": 8103,
        "time_start": "2026-04-15T00:00:00",
        "time_end": "2026-04-21T23:59:59",
    },
]


def _build_partition_app(spec: dict[str, Any]) -> FastAPI:
    """Build a partition FastAPI app and populate ``app.state`` manually.

    We skip the app's real lifespan and replicate exactly what it does so
    the state is guaranteed to be present by the time the first request
    lands — no thread-boundary races with ``TestClient``.
    """

    settings = PartitionSettings(
        partition_id=spec["id"],
        partition_port=spec["port"],
        partition_time_start=spec["time_start"],
        partition_time_end=spec["time_end"],
        indexed_fields="level,service,timestamp",
        log_sample_count=_ROWS_PER_PARTITION,
        log_level="INFO",
    )

    app = create_partition_app(settings)

    # Replicate the lifespan's work so the routes see ready state without
    # needing to run the lifespan (ASGITransport does not run lifespan).
    time_range = TimeRange(
        start=datetime.fromisoformat(settings.partition_time_start),
        end=datetime.fromisoformat(settings.partition_time_end),
    )
    indexed_fields = settings.indexed_fields_list()
    records = generate_logs(
        partition_id=settings.partition_id,
        time_range=time_range,
        count=settings.log_sample_count,
    )
    storage = LogStorage(records=records, indexed_fields=indexed_fields)
    executor = LocalExecutor(storage=storage)
    metadata = PartitionMetadata(
        id=settings.partition_id,
        url=f"http://{settings.partition_id}:{settings.partition_port}",
        time_range=time_range,
        indexed_fields=indexed_fields,
        healthy=True,
    )

    app.state.settings = settings
    app.state.storage = storage
    app.state.executor = executor
    app.state.metadata = metadata

    return app


# --- multiplexed transport -------------------------------------------------


def _build_multiplexed_transport(
    partition_transports: dict[str, httpx.ASGITransport],
) -> httpx.MockTransport:
    """Return a mock transport that dispatches each request by hostname.

    Partition URLs in ``CoordinatorSettings`` are of the form
    ``http://{partition_id}:{port}`` — so the host portion of an incoming
    request is exactly the partition id we keyed the ASGI transports by.
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host or ""
        transport = partition_transports.get(host)
        if transport is None:
            return httpx.Response(
                404, json={"detail": f"unknown partition host: {host}"}
            )
        # ASGITransport.handle_async_request returns an httpx.Response
        # whose body is already buffered into memory — exactly what
        # MockTransport wants to return.
        return await transport.handle_async_request(request)

    return httpx.MockTransport(handler)


# --- coordinator app factory for tests -------------------------------------


def _build_coordinator_app(
    partition_transports: dict[str, httpx.ASGITransport],
    partition_urls: dict[str, str],
) -> FastAPI:
    """Build a coordinator FastAPI wired to the in-process partitions."""

    settings = CoordinatorSettings(
        coordinator_port=8000,
        partition_urls=",".join(
            f"{pid}={url}" for pid, url in partition_urls.items()
        ),
        request_timeout=5.0,
        default_limit=1000,
        max_concurrent_queries=100,
        query_timeout=30.0,
        log_level="INFO",
    )

    mux_transport = _build_multiplexed_transport(partition_transports)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(transport=mux_transport)
        registry = PartitionRegistry(settings.partition_urls_dict())
        executor = QueryExecutor(
            client=client, request_timeout=settings.request_timeout
        )

        app.state.settings = settings
        app.state.client = client
        app.state.registry = registry
        app.state.executor = executor
        app.state.progress = ProgressRegistry()

        # Prime the registry so the first query sees live partitions.
        await registry.refresh(client)

        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(coordinator_router)
    return app


# --- fixtures --------------------------------------------------------------


@pytest.fixture(scope="module")
def integration_client():
    """Yield a ``TestClient`` for a fully-wired in-process coordinator."""

    partition_apps: list[FastAPI] = [
        _build_partition_app(spec) for spec in _PARTITION_SPECS
    ]

    partition_urls: dict[str, str] = {
        spec["id"]: f"http://{spec['id']}:{spec['port']}"
        for spec in _PARTITION_SPECS
    }

    partition_transports: dict[str, httpx.ASGITransport] = {
        spec["id"]: httpx.ASGITransport(app=app)
        for spec, app in zip(_PARTITION_SPECS, partition_apps)
    }

    app = _build_coordinator_app(partition_transports, partition_urls)
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _run_query(client: TestClient, sql: str) -> dict[str, Any]:
    resp = client.post("/api/query", json={"query": sql})
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_select_star_returns_rows(integration_client: TestClient) -> None:
    body = _run_query(integration_client, "SELECT * FROM logs LIMIT 10")
    assert isinstance(body["results"], list)
    assert len(body["results"]) == 10
    assert body["records_processed"] > 0
    assert body["partial_results"] is False
    assert body["failed_partitions"] == []
    # Every row should carry the core synthetic schema.
    for row in body["results"]:
        assert "level" in row
        assert "service" in row
        assert "message" in row


def test_level_filter(integration_client: TestClient) -> None:
    body = _run_query(
        integration_client,
        "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 20",
    )
    assert len(body["results"]) > 0
    for row in body["results"]:
        assert row["level"] == "ERROR"


def test_count_star(integration_client: TestClient) -> None:
    body = _run_query(integration_client, "SELECT COUNT(*) AS total FROM logs")
    assert len(body["results"]) == 1
    total = body["results"][0]["total"]
    # COUNT(*) across 3 partitions of 200 rows each.
    assert isinstance(total, (int, float))
    assert int(total) == _ROWS_PER_PARTITION * 3


def test_group_by_service_counts(integration_client: TestClient) -> None:
    body = _run_query(
        integration_client,
        "SELECT service, COUNT(*) AS cnt FROM logs GROUP BY service",
    )
    results = body["results"]
    # Data generator draws from 5 services; we may see all 5.
    assert 1 <= len(results) <= 5
    for row in results:
        assert "service" in row
        assert "cnt" in row
        assert int(row["cnt"]) > 0

    total_from_groups = sum(int(row["cnt"]) for row in results)
    assert total_from_groups == _ROWS_PER_PARTITION * 3


def test_temporal_pruning(integration_client: TestClient) -> None:
    body = _run_query(
        integration_client,
        "SELECT * FROM logs WHERE timestamp > '2026-04-15' LIMIT 5",
    )
    # Every returned row must satisfy the predicate.
    for row in body["results"]:
        assert row["timestamp"] > "2026-04-15"

    # Planner should prune to just partition-3.
    notes = body["plan"]["optimization_notes"]
    pruning_notes = [n for n in notes if "Partition pruning" in n]
    assert pruning_notes, f"expected a pruning note, got: {notes}"
    assert "1/3" in pruning_notes[0]


def test_between_query(integration_client: TestClient) -> None:
    body = _run_query(
        integration_client,
        "SELECT * FROM logs WHERE duration_ms BETWEEN 100 AND 200 LIMIT 10",
    )
    # With 600 rows and expovariate(1/50), ~12% land in [100, 200] — the
    # LIMIT 10 should always be satisfied.
    assert len(body["results"]) > 0
    for row in body["results"]:
        dur = row["duration_ms"]
        assert isinstance(dur, (int, float))
        assert 100 <= float(dur) <= 200


def test_contains_query(integration_client: TestClient) -> None:
    body = _run_query(
        integration_client,
        "SELECT * FROM logs WHERE message CONTAINS 'timeout' LIMIT 5",
    )
    assert len(body["results"]) > 0
    for row in body["results"]:
        assert "timeout" in row["message"].lower()


def test_aggregation_with_where(integration_client: TestClient) -> None:
    body = _run_query(
        integration_client,
        "SELECT COUNT(*) AS cnt FROM logs WHERE level = 'ERROR'",
    )
    assert len(body["results"]) == 1
    row = body["results"][0]
    assert "cnt" in row
    assert int(row["cnt"]) >= 0
    # ERROR is weighted at ~10 %, so with 600 total rows we expect ~60
    # errors — keep a loose bound rather than an exact number so the test
    # is stable across seeds.
    assert int(row["cnt"]) < _ROWS_PER_PARTITION * 3


def test_group_by_with_having(integration_client: TestClient) -> None:
    # Using the aliased column name ``cnt`` in HAVING is the shape the
    # aggregator's post-group evaluator supports (aggregate functions
    # re-materialise under their alias in the grouped result rows).
    body = _run_query(
        integration_client,
        "SELECT service, COUNT(*) AS cnt FROM logs GROUP BY service "
        "HAVING cnt > 1",
    )
    assert len(body["results"]) > 0
    for row in body["results"]:
        assert int(row["cnt"]) > 1


def test_explain_endpoint(integration_client: TestClient) -> None:
    resp = integration_client.post(
        "/api/explain",
        json={
            "query": (
                "SELECT service, COUNT(*) AS cnt FROM logs "
                "WHERE level = 'ERROR' GROUP BY service ORDER BY cnt DESC"
            )
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "plan_text" in body
    assert "Execution plan" in body["plan_text"]
    # The rendered text should reference at least one of the three
    # optimization bullets the planner emits.
    assert any(
        marker in body["plan_text"]
        for marker in (
            "Partition pruning",
            "Predicate pushdown",
            "Aggregation distribution",
        )
    )
