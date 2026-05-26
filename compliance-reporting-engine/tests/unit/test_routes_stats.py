"""Unit tests for :mod:`src.api.routes_stats` and the underlying stats service.

Strategy: build a minimal FastAPI app with an in-memory SQLite engine,
seed five ``Report`` rows in a mix of terminal and in-flight states,
hit ``GET /dashboard/stats``, and assert the rolled-up counters match
what we put in. The same approach as the routes_reports test, but
without a coordinator stub (this route is pure read).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import AsyncIterator
from uuid import uuid4

import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.api import routes_stats
from src.persistence.db import init_db
from src.persistence.models import Report


@pytest_asyncio.fixture
async def test_app() -> AsyncIterator[tuple[FastAPI, async_sessionmaker, AsyncEngine]]:
    """Fresh FastAPI app + in-memory DB per test."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    await init_db(engine)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(routes_stats.router)

    try:
        yield app, session_factory, engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def client(test_app) -> AsyncIterator[AsyncClient]:
    app, _factory, _engine = test_app
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def _seed_five_reports(session_factory: async_sessionmaker) -> None:
    """Seed a deterministic mix: 2 COMPLETED, 1 FAILED, 2 in-flight.

    Mix of frameworks (SOX x2, HIPAA x2, GDPR) and formats
    (JSON x3, PDF x2) so the breakdown dicts have multiple keys to
    aggregate over.
    """
    period_end = datetime(2026, 5, 25, 0, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=30)

    rows = [
        # 2 COMPLETED
        Report(
            id=uuid4(),
            framework="SOX",
            period_start=period_start,
            period_end=period_end,
            export_format="JSON",
            state="COMPLETED",
            completed_at=period_end,
        ),
        Report(
            id=uuid4(),
            framework="HIPAA",
            period_start=period_start,
            period_end=period_end,
            export_format="PDF",
            state="COMPLETED",
            completed_at=period_end,
        ),
        # 1 FAILED
        Report(
            id=uuid4(),
            framework="GDPR",
            period_start=period_start,
            period_end=period_end,
            export_format="JSON",
            state="FAILED",
            error_message="boom",
        ),
        # 2 in-flight (one PENDING, one EXPORTING)
        Report(
            id=uuid4(),
            framework="SOX",
            period_start=period_start,
            period_end=period_end,
            export_format="PDF",
            state="PENDING",
        ),
        Report(
            id=uuid4(),
            framework="HIPAA",
            period_start=period_start,
            period_end=period_end,
            export_format="JSON",
            state="EXPORTING",
        ),
    ]
    async with session_factory() as session:
        session.add_all(rows)
        await session.commit()


async def test_dashboard_stats_aggregates_5_rows(test_app, client) -> None:
    """5 mixed rows -> verified totals, breakdowns, success rate, in-flight."""
    _app, session_factory, _engine = test_app
    await _seed_five_reports(session_factory)

    response = await client.get("/dashboard/stats")
    assert response.status_code == 200
    body = response.json()

    # --- Totals ---
    assert body["total_reports"] == 5

    # --- Breakdowns ---
    # SOX: 2 (one COMPLETED, one PENDING); HIPAA: 2 (one COMPLETED,
    # one EXPORTING); GDPR: 1 (FAILED). Order is alphabetical by
    # framework name as set by the GROUP BY ORDER BY in the service.
    assert body["framework_breakdown"] == {
        "GDPR": 1,
        "HIPAA": 2,
        "SOX": 2,
    }
    # JSON: 3 (SOX/JSON, GDPR/JSON, HIPAA/JSON); PDF: 2 (HIPAA/PDF, SOX/PDF).
    assert body["format_breakdown"] == {
        "JSON": 3,
        "PDF": 2,
    }

    # --- Success rate ---
    # 2 COMPLETED + 1 FAILED = 3 terminal. 2/3 ≈ 0.6667.
    assert body["success_rate"] == 0.6667

    # --- In-flight ---
    # PENDING + EXPORTING = 2.
    assert body["in_flight"] == 2

    # --- Recent ---
    # All 5 rows fit within the default limit of 10; ordered by
    # created_at DESC. Just check the count + shape, not the precise
    # ordering (created_at defaults to "now" so all five are within a
    # microsecond of each other under the in-memory test fixture).
    assert isinstance(body["recent"], list)
    assert len(body["recent"]) == 5
    for entry in body["recent"]:
        assert {"report_id", "framework", "export_format", "state", "created_at"}.issubset(
            entry.keys()
        )


async def test_dashboard_stats_empty_db(test_app, client) -> None:
    """No reports at all -> zeroes everywhere, success_rate=0.0, empty recent."""
    response = await client.get("/dashboard/stats")
    assert response.status_code == 200
    body = response.json()
    assert body["total_reports"] == 0
    assert body["framework_breakdown"] == {}
    assert body["format_breakdown"] == {}
    assert body["success_rate"] == 0.0
    assert body["in_flight"] == 0
    assert body["recent"] == []


async def test_dashboard_stats_in_flight_excluded_from_success_rate(
    test_app, client
) -> None:
    """A run with no terminal reports yet returns 0.0 success rate (not NaN/error)."""
    _app, session_factory, _engine = test_app

    period_end = datetime(2026, 5, 25, 0, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=14)

    async with session_factory() as session:
        # Only in-flight rows — no COMPLETED, no FAILED.
        session.add_all(
            [
                Report(
                    id=uuid4(),
                    framework="SOX",
                    period_start=period_start,
                    period_end=period_end,
                    export_format="JSON",
                    state="PENDING",
                ),
                Report(
                    id=uuid4(),
                    framework="SOX",
                    period_start=period_start,
                    period_end=period_end,
                    export_format="JSON",
                    state="AGGREGATING",
                ),
            ]
        )
        await session.commit()

    response = await client.get("/dashboard/stats")
    body = response.json()
    assert body["total_reports"] == 2
    assert body["in_flight"] == 2
    # No terminal rows -> divide-by-zero guard returns 0.0.
    assert body["success_rate"] == 0.0
