"""Unit tests for C13 — FastAPI router surface.

Covers the five routers wired in :mod:`src.main`:

* ``/health`` — liveness/readiness.
* ``/experiments`` — CRUD over experiment definitions.
* ``/experiments/{id}/run`` + ``/runs/{run_id}`` — start/inspect runs.
* ``/experiments/{id}/abort`` — abort active runs for an experiment.
* ``/targets`` — list allowlisted Docker targets.
* ``/admin/*`` — dry-run toggle + abort-all kill switch.

Tests build a fresh ``app = create_app()`` for each case and override the
FastAPI dependency providers in :mod:`src.api.dependencies` so we never
trigger the real lifespan (no Docker socket, no SystemMonitor, no
SQLite-on-disk). Persistence uses an in-memory aiosqlite engine seeded per
test via the same ``make_engine`` / ``create_all_tables`` helpers used in
``test_persistence.py``. The ``RunManager`` is replaced with a
``MagicMock`` / ``AsyncMock`` to keep the API layer isolated from the
engine's lifecycle complexity.

httpx + ASGITransport drives the app without invoking lifespan, which is
what we want: each test wires the bits of ``app.state`` the routers
actually touch and exercises one endpoint at a time.
"""

from __future__ import annotations

from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api import dependencies as deps
from src.main import create_app
from src.models.experiments import (
    ExperimentDefinition,
    ExperimentRun,
    Hypothesis,
    RunStatus,
)
from src.models.scenarios import FailureType
from src.persistence import (
    ExperimentDefinitionRepo,
    ExperimentRunRepo,
    create_all_tables,
    make_engine,
    make_sessionmaker,
)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def sessionmaker() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Fresh in-memory aiosqlite DB per test."""
    engine = make_engine("sqlite+aiosqlite:///:memory:")
    await create_all_tables(engine)
    sm = make_sessionmaker(engine)
    try:
        yield sm
    finally:
        await engine.dispose()


def _make_app(
    *,
    sm: async_sessionmaker[AsyncSession] | None = None,
    run_manager: object | None = None,
    docker_client: object | None = None,
):
    """Build a fresh app and wire dependency overrides + app.state.

    We never enter the lifespan; ASGITransport + AsyncClient does not
    trigger it automatically. The state slots populated here are exactly
    what the routers / dependency providers read.
    """
    app = create_app()

    if sm is not None:
        app.state.db_sessionmaker = sm

        async def _override_db():
            async with sm() as session:
                yield session

        app.dependency_overrides[deps.get_db] = _override_db

    if run_manager is not None:
        app.state.run_manager = run_manager
        app.dependency_overrides[deps.get_run_manager] = lambda: run_manager

    if docker_client is not None:
        app.state.docker_client = docker_client
        app.dependency_overrides[deps.get_docker_client] = lambda: docker_client

    return app


def _client(app) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _valid_payload(**overrides) -> dict:
    payload = {
        "name": "latency-smoke",
        "description": "lat-injection smoke",
        "type": FailureType.LATENCY_INJECTION.value,
        "target": "log-consumer",
        "parameters": {"latency_ms": 200},
        "duration": 60,
        "severity": 2,
    }
    payload.update(overrides)
    return payload


# --------------------------------------------------------------------------- #
# /health
# --------------------------------------------------------------------------- #


class TestHealthRoute:
    async def test_health_returns_expected_shape(self, sessionmaker) -> None:
        # Even without a real lifespan, the route should return 200 and the
        # documented keys; values reflect whatever is on app.state.
        app = _make_app(sm=sessionmaker)
        async with _client(app) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        for key in (
            "status",
            "version",
            "monitor_running",
            "docker_connected",
            "db_connected",
        ):
            assert key in body, f"missing key {key!r} in health body"
        # status string is fixed; the booleans we just sanity-check as bools.
        assert body["status"] == "ok"
        assert body["version"] == "0.1.0"
        assert isinstance(body["monitor_running"], bool)
        assert isinstance(body["docker_connected"], bool)
        assert isinstance(body["db_connected"], bool)


# --------------------------------------------------------------------------- #
# /experiments CRUD
# --------------------------------------------------------------------------- #


class TestExperimentsCrud:
    async def test_post_then_get_roundtrips_basic_payload(self, sessionmaker) -> None:
        app = _make_app(sm=sessionmaker)
        payload = _valid_payload()
        async with _client(app) as client:
            create_resp = await client.post("/experiments", json=payload)
            assert create_resp.status_code == 201, create_resp.text
            created = create_resp.json()
            assert "id" in created and len(created["id"]) > 0
            assert created["name"] == payload["name"]
            assert created["type"] == payload["type"]
            assert created["target"] == payload["target"]
            assert created["parameters"] == payload["parameters"]
            assert created["duration"] == payload["duration"]
            assert created["severity"] == payload["severity"]

            list_resp = await client.get("/experiments")
            assert list_resp.status_code == 200
            rows = list_resp.json()
            assert isinstance(rows, list)
            assert any(r["id"] == created["id"] for r in rows)

    async def test_post_with_hypothesis_roundtrips_via_get_by_id(
        self, sessionmaker
    ) -> None:
        app = _make_app(sm=sessionmaker)
        hypothesis = {
            "statement": "If 200ms latency on log-consumer then RTT < 30s",
            "recovery_time_budget_s": 30,
            "expected_invariants": ["no data loss"],
        }
        payload = _valid_payload(hypothesis=hypothesis)
        async with _client(app) as client:
            create_resp = await client.post("/experiments", json=payload)
            assert create_resp.status_code == 201, create_resp.text
            exp_id = create_resp.json()["id"]

            get_resp = await client.get(f"/experiments/{exp_id}")
            assert get_resp.status_code == 200
            body = get_resp.json()
            assert body["id"] == exp_id
            assert body["hypothesis"] is not None
            assert body["hypothesis"]["statement"] == hypothesis["statement"]
            assert (
                body["hypothesis"]["recovery_time_budget_s"]
                == hypothesis["recovery_time_budget_s"]
            )
            assert (
                body["hypothesis"]["expected_invariants"]
                == hypothesis["expected_invariants"]
            )

    async def test_post_missing_required_field_returns_422(
        self, sessionmaker
    ) -> None:
        app = _make_app(sm=sessionmaker)
        bad = _valid_payload()
        bad.pop("type")  # FailureType is required.
        async with _client(app) as client:
            resp = await client.post("/experiments", json=bad)
        assert resp.status_code == 422, resp.text

    async def test_get_unknown_id_returns_404(self, sessionmaker) -> None:
        app = _make_app(sm=sessionmaker)
        async with _client(app) as client:
            resp = await client.get("/experiments/does-not-exist-id")
        assert resp.status_code == 404
        assert "experiment not found" in resp.json()["detail"]

    async def test_list_filtered_by_target_only_returns_matching(
        self, sessionmaker
    ) -> None:
        app = _make_app(sm=sessionmaker)
        async with _client(app) as client:
            # Seed two on log-consumer, one on log-producer.
            r1 = await client.post(
                "/experiments",
                json=_valid_payload(name="lc-1", target="log-consumer"),
            )
            r2 = await client.post(
                "/experiments",
                json=_valid_payload(name="lc-2", target="log-consumer"),
            )
            r3 = await client.post(
                "/experiments",
                json=_valid_payload(name="lp-1", target="log-producer"),
            )
            assert all(r.status_code == 201 for r in (r1, r2, r3))

            filt = await client.get("/experiments", params={"target": "log-consumer"})
            assert filt.status_code == 200
            rows = filt.json()
            names = {r["name"] for r in rows}
            assert names == {"lc-1", "lc-2"}
            assert all(r["target"] == "log-consumer" for r in rows)


# --------------------------------------------------------------------------- #
# /experiments/{id}/run + /runs/{run_id}
# --------------------------------------------------------------------------- #


class TestRunRoutes:
    async def test_start_run_unknown_experiment_returns_404(
        self, sessionmaker
    ) -> None:
        run_manager = MagicMock()
        run_manager.dry_run = False
        run_manager.start = AsyncMock()
        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.post("/experiments/no-such-id/run")
        assert resp.status_code == 404
        # The mock must NOT have been called — we 404'd before reaching it.
        run_manager.start.assert_not_called()

    async def test_start_run_happy_path_returns_run_id_and_calls_manager(
        self, sessionmaker
    ) -> None:
        # Seed a definition in the DB.
        defn = ExperimentDefinition(
            name="happy-path",
            type=FailureType.LATENCY_INJECTION,
            target="log-consumer",
            parameters={"latency_ms": 150},
            duration=10,
            severity=2,
        )
        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)

        seeded_run = ExperimentRun(
            experiment_id=defn.id,
            status=RunStatus.PENDING,
        )
        run_manager = MagicMock()
        run_manager.dry_run = False
        run_manager.start = AsyncMock(return_value=seeded_run)

        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.post(f"/experiments/{defn.id}/run")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["run_id"] == seeded_run.run_id
        assert body["experiment_id"] == defn.id
        assert body["status"] == RunStatus.PENDING.value
        assert body["dry_run"] is False

        # start() called once, with the *persisted* definition (matched by id).
        run_manager.start.assert_awaited_once()
        called_arg = run_manager.start.await_args.args[0]
        assert isinstance(called_arg, ExperimentDefinition)
        assert called_arg.id == defn.id

    async def test_get_run_falls_back_to_db_when_not_in_memory(
        self, sessionmaker
    ) -> None:
        defn = ExperimentDefinition(
            name="db-fallback",
            type=FailureType.LATENCY_INJECTION,
            target="log-consumer",
            parameters={"latency_ms": 100},
            duration=5,
            severity=1,
        )
        persisted_run = ExperimentRun(
            experiment_id=defn.id,
            status=RunStatus.COMPLETED,
        )
        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)
            await ExperimentRunRepo(session).create(persisted_run)

        run_manager = MagicMock()
        run_manager.dry_run = False
        run_manager.get_run = MagicMock(return_value=None)

        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.get(f"/runs/{persisted_run.run_id}")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["run_id"] == persisted_run.run_id
        assert body["experiment_id"] == defn.id
        assert body["status"] == RunStatus.COMPLETED.value

    async def test_get_run_unknown_id_returns_404(self, sessionmaker) -> None:
        run_manager = MagicMock()
        run_manager.dry_run = False
        run_manager.get_run = MagicMock(return_value=None)
        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.get("/runs/no-such-run-id")
        assert resp.status_code == 404


# --------------------------------------------------------------------------- #
# /experiments/{id}/abort
# --------------------------------------------------------------------------- #


class TestAbortRoute:
    async def test_abort_no_active_run_returns_false(self, sessionmaker) -> None:
        defn = ExperimentDefinition(
            name="abort-none",
            type=FailureType.LATENCY_INJECTION,
            target="log-consumer",
            parameters={"latency_ms": 100},
            duration=5,
            severity=1,
        )
        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)

        run_manager = MagicMock()
        run_manager.dry_run = False
        # No active runs anywhere.
        run_manager.active_run_ids = MagicMock(return_value=[])
        run_manager.get_run = MagicMock(return_value=None)
        run_manager.abort_run = AsyncMock(return_value=False)

        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.post(f"/experiments/{defn.id}/abort")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["aborted"] is False
        assert body["run_id"] == defn.id

    async def test_abort_in_memory_run_returns_true(self, sessionmaker) -> None:
        defn = ExperimentDefinition(
            name="abort-yes",
            type=FailureType.LATENCY_INJECTION,
            target="log-consumer",
            parameters={"latency_ms": 100},
            duration=5,
            severity=1,
        )
        async with sessionmaker() as session:
            await ExperimentDefinitionRepo(session).create(defn)

        active_run = ExperimentRun(
            experiment_id=defn.id,
            status=RunStatus.RUNNING,
        )
        run_manager = MagicMock()
        run_manager.dry_run = False
        # DB returns nothing (no rows for this experiment), so the route
        # falls back to the in-memory map — which we model here.
        run_manager.active_run_ids = MagicMock(return_value=[active_run.run_id])
        run_manager.get_run = MagicMock(return_value=active_run)
        run_manager.abort_run = AsyncMock(return_value=True)

        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.post(f"/experiments/{defn.id}/abort")

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["aborted"] is True
        run_manager.abort_run.assert_awaited_once_with(active_run.run_id)


# --------------------------------------------------------------------------- #
# /targets
# --------------------------------------------------------------------------- #


def _fake_container(
    *,
    name: str,
    cid: str,
    image: str = "log-consumer:dev",
    status: str = "running",
    labels: dict | None = None,
):
    """Build a MagicMock that quacks like a docker SDK Container."""
    c = MagicMock()
    c.name = name
    c.id = cid
    c.image = image
    c.status = status
    c.labels = labels if labels is not None else {"chaos.target": "true"}
    return c


class TestTargetsRoute:
    async def test_list_targets_maps_container_attrs(self, sessionmaker) -> None:
        docker_client = MagicMock()
        docker_client.list_chaos_targets = MagicMock(
            return_value=[
                _fake_container(
                    name="log-consumer",
                    cid="cid-consumer-1234",
                    image="log-consumer:dev",
                    status="running",
                    labels={"chaos.target": "true", "service": "consumer"},
                ),
                _fake_container(
                    name="log-producer",
                    cid="cid-producer-5678",
                    image="log-producer:dev",
                    status="running",
                    labels={"chaos.target": "true"},
                ),
            ]
        )

        app = _make_app(sm=sessionmaker, docker_client=docker_client)
        async with _client(app) as client:
            resp = await client.get("/targets")

        assert resp.status_code == 200, resp.text
        rows = resp.json()
        assert isinstance(rows, list)
        assert len(rows) == 2
        names = {r["name"] for r in rows}
        assert names == {"log-consumer", "log-producer"}
        consumer = next(r for r in rows if r["name"] == "log-consumer")
        assert consumer["id"] == "cid-consumer-1234"
        assert consumer["status"] == "running"
        assert consumer["labels"].get("service") == "consumer"
        docker_client.list_chaos_targets.assert_called_once()

    async def test_list_targets_empty_returns_empty_list(self, sessionmaker) -> None:
        docker_client = MagicMock()
        docker_client.list_chaos_targets = MagicMock(return_value=[])
        app = _make_app(sm=sessionmaker, docker_client=docker_client)
        async with _client(app) as client:
            resp = await client.get("/targets")
        assert resp.status_code == 200
        assert resp.json() == []


# --------------------------------------------------------------------------- #
# /admin/*
# --------------------------------------------------------------------------- #


class TestAdminRoutes:
    async def test_get_dry_run_reflects_current_state(self, sessionmaker) -> None:
        run_manager = MagicMock()
        run_manager.dry_run = True
        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.get("/admin/dry-run")
        assert resp.status_code == 200
        assert resp.json() == {"dry_run": True}

    async def test_post_dry_run_enables_via_set_dry_run(self, sessionmaker) -> None:
        # Model a real toggle: set_dry_run returns the new state.
        run_manager = MagicMock()
        run_manager.dry_run = False
        run_manager.set_dry_run = MagicMock(return_value=True)
        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.post("/admin/dry-run", params={"enabled": True})
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"dry_run": True}
        run_manager.set_dry_run.assert_called_once_with(True)

    async def test_post_admin_abort_returns_aborted_count(self, sessionmaker) -> None:
        run_manager = MagicMock()
        run_manager.abort_all = AsyncMock(return_value=2)
        app = _make_app(sm=sessionmaker, run_manager=run_manager)
        async with _client(app) as client:
            resp = await client.post("/admin/abort")
        assert resp.status_code == 200
        assert resp.json() == {"aborted_count": 2}
        run_manager.abort_all.assert_awaited_once()
