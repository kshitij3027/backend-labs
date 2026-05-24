"""Integration tests for the HTMX dashboard."""
from __future__ import annotations

from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.dependencies import get_session
from src.api.routes_dashboard import router as dashboard_router
from src.settings import Settings


@pytest_asyncio.fixture
async def app(session_factory: async_sessionmaker[AsyncSession]) -> FastAPI:
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        anonymization_hash_salt="t",
        dashboard_refresh_ms=3000,
    )
    app.mount("/static", StaticFiles(directory="static"), name="static")
    app.include_router(dashboard_router)

    async def _override_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as s:
            try:
                yield s
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_session] = _override_session
    return app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_get_dashboard_returns_html_with_all_four_cards(client):
    r = await client.get("/")
    assert r.status_code == 200
    body = r.text
    assert "GDPR Log Erasure" in body
    # All four card sections present (skeleton; 3 are placeholders until commit 10)
    assert 'id="card-stats"' in body
    assert 'id="card-requests"' in body
    assert 'id="card-completed"' in body
    assert 'id="card-audit"' in body
    # HTMX wired on stats card
    assert 'hx-get="/partials/stats"' in body
    assert 'hx-trigger="load, every 3000ms"' in body


@pytest.mark.asyncio
async def test_get_partial_stats_returns_kv_table(client):
    r = await client.get("/partials/stats")
    assert r.status_code == 200
    body = r.text
    assert "Statistics" in body
    assert "total mappings" in body
    assert "unique users" in body
    assert "completion rate" in body
    assert "0" in body  # fresh DB → total_mappings 0


@pytest.mark.asyncio
async def test_static_dashboard_css_served(client):
    r = await client.get("/static/dashboard.css")
    assert r.status_code == 200
    assert "card" in r.text


@pytest.mark.asyncio
async def test_static_htmx_min_js_served(client):
    r = await client.get("/static/htmx.min.js")
    assert r.status_code == 200
    # HTMX runtime is at least 40 KB
    assert len(r.content) > 40000
