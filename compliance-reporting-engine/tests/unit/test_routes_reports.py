"""Unit tests for :mod:`src.api.routes_reports`.

Strategy: each test builds a fresh FastAPI app with an in-memory
SQLite engine + session factory + a no-op coordinator stub attached
to ``app.state``. The router under test is mounted and exercised via
``httpx.AsyncClient + ASGITransport`` (same pattern the sibling
``gdpr-log-erasure-system`` integration tests use — no thread, no
extra process). The coordinator stub records each ``generate()``
call so we can assert dispatch happened without actually running the
pipeline.

Why a minimal per-test app instead of importing :mod:`src.main`? The
real lifespan needs Postgres + a real Fernet key + a real signing key
+ a real storage path; the unit tests want to exercise the route + DB
layer in isolation. The fixture below stays under ~40 lines and gives
us full control over ``app.state`` for failure-injection.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import AsyncIterator
from uuid import UUID, uuid4

import pytest_asyncio
from cryptography.fernet import Fernet
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.api import routes_reports
from src.persistence.db import init_db
from src.persistence.models import Report


class _StubCoordinator:
    """Record-only stub for ``ReportCoordinator``.

    Captures every ``generate(report_id)`` call so tests can assert
    the background task was dispatched with the expected id.
    """

    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def generate(self, report_id: UUID) -> None:
        self.calls.append(report_id)


@pytest_asyncio.fixture
async def test_app() -> AsyncIterator[tuple[FastAPI, _StubCoordinator, async_sessionmaker, AsyncEngine]]:
    """Build a fresh FastAPI app + in-memory DB per test.

    Yields a tuple of ``(app, coordinator_stub, session_factory, engine)``
    so tests can hit the API via the client AND open a sibling session
    to assert DB-side effects independently.
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    await init_db(engine)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    stub = _StubCoordinator()

    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.coordinator = stub
    app.state.signing_key = b"a" * 32
    app.state.secondary_signing_key = None
    app.state.fernet = Fernet(Fernet.generate_key())
    app.include_router(routes_reports.router)

    try:
        yield app, stub, session_factory, engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(test_app) -> AsyncIterator[AsyncClient]:
    app, _stub, _factory, _engine = test_app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


def _valid_body() -> dict:
    """A request body that should always validate."""
    period_end = datetime(2026, 5, 25, 0, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=7)
    return {
        "framework": "SOX",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "export_format": "JSON",
        "title": "Q2 SOX evidence",
        "description": "Unit test body",
    }


async def test_post_generate_returns_202_with_report_id(test_app, client) -> None:
    """Happy path: 202 + report_id, Report row inserted with state=PENDING."""
    _app, stub, session_factory, _engine = test_app

    response = await client.post("/reports/generate", json=_valid_body())

    assert response.status_code == 202
    body = response.json()
    assert "report_id" in body
    assert body["state"] == "PENDING"

    # Verify the row landed in the DB.
    async with session_factory() as session:
        report = await session.get(Report, UUID(body["report_id"]))
        assert report is not None
        assert report.framework == "SOX"
        assert report.state == "PENDING"
        assert report.export_format == "JSON"
        assert report.title == "Q2 SOX evidence"

    # The route dispatched the coordinator via BackgroundTasks; the
    # AsyncClient awaits them as part of the request lifecycle, so by
    # the time we're here the stub should have recorded the call.
    assert stub.calls == [UUID(body["report_id"])]


async def test_post_generate_unknown_framework_returns_422(client) -> None:
    body = _valid_body()
    body["framework"] = "MADEUP"
    response = await client.post("/reports/generate", json=body)
    assert response.status_code == 422
    assert "MADEUP" in response.json()["detail"]


async def test_post_generate_invalid_format_returns_422(client) -> None:
    body = _valid_body()
    body["export_format"] = "DOCX"
    response = await client.post("/reports/generate", json=body)
    # Pydantic Literal rejection -> 422 from FastAPI itself.
    assert response.status_code == 422


async def test_post_generate_period_inverted_returns_422(client) -> None:
    body = _valid_body()
    # Swap start / end so period_start > period_end.
    body["period_start"], body["period_end"] = body["period_end"], body["period_start"]
    response = await client.post("/reports/generate", json=body)
    assert response.status_code == 422
    assert "period_start" in response.json()["detail"]


async def test_get_status_404_when_missing(client) -> None:
    response = await client.get(f"/reports/{uuid4()}")
    assert response.status_code == 404
    assert "report not found" in response.json()["detail"]


async def test_get_status_returns_report_state(test_app, client) -> None:
    """Pre-insert a Report row, GET its status, assert fields round-trip."""
    _app, _stub, session_factory, _engine = test_app

    period_end = datetime(2026, 5, 25, 0, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=14)
    new_id = uuid4()

    async with session_factory() as session:
        report = Report(
            id=new_id,
            framework="HIPAA",
            period_start=period_start,
            period_end=period_end,
            export_format="PDF",
            state="AGGREGATING",
            title="In-flight",
        )
        session.add(report)
        await session.commit()

    response = await client.get(f"/reports/{new_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["report_id"] == str(new_id)
    assert body["framework"] == "HIPAA"
    assert body["state"] == "AGGREGATING"
    assert body["export_format"] == "PDF"
    # Non-COMPLETED -> no download URL surfaced.
    assert body["download_url"] is None
    assert body["verify_url"] == f"/reports/{new_id}/verify"


async def test_download_404_when_not_completed(test_app, client) -> None:
    """Download on a PENDING/non-existent report is a clean 404."""
    _app, _stub, session_factory, _engine = test_app

    period_end = datetime(2026, 5, 25, 0, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=14)
    new_id = uuid4()

    async with session_factory() as session:
        session.add(
            Report(
                id=new_id,
                framework="SOX",
                period_start=period_start,
                period_end=period_end,
                export_format="JSON",
                state="PENDING",
            )
        )
        await session.commit()

    response = await client.get(f"/reports/{new_id}/download")
    assert response.status_code == 404
    # And a totally unknown id is also 404.
    response2 = await client.get(f"/reports/{uuid4()}/download")
    assert response2.status_code == 404
