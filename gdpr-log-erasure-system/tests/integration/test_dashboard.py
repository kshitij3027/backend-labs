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


# ── added in commit 10 ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_partial_requests_empty(client):
    r = await client.get("/partials/requests")
    assert r.status_code == 200
    assert "no in-flight requests" in r.text


@pytest.mark.asyncio
async def test_partial_completed_empty(client):
    r = await client.get("/partials/completed")
    assert r.status_code == 200
    assert "no completed requests yet" in r.text


@pytest.mark.asyncio
async def test_partial_audit_has_genesis_row(client):
    r = await client.get("/partials/audit")
    assert r.status_code == 200
    # init_db has already seeded a GENESIS row visible via session_factory.
    assert "Audit Feed" in r.text
    assert "GENESIS" in r.text


@pytest.mark.asyncio
async def test_partial_requests_shows_inflight(client, session_factory):
    from src.persistence.models import ErasureRequest, RequestType, RequestState
    async with session_factory() as s:
        s.add(ErasureRequest(user_id="dash-u", request_type=RequestType.DELETE, state=RequestState.PENDING))
        await s.commit()
    r = await client.get("/partials/requests")
    assert r.status_code == 200
    assert "dash-u" in r.text
    assert 'class="pill PENDING"' in r.text


@pytest.mark.asyncio
async def test_partial_completed_shows_terminal(client, session_factory):
    import datetime as dt
    from src.persistence.models import ErasureRequest, RequestType, RequestState
    async with session_factory() as s:
        now = dt.datetime.utcnow().replace(microsecond=0)
        s.add(ErasureRequest(
            user_id="dash-c", request_type=RequestType.DELETE,
            state=RequestState.COMPLETED,
            started_at=now - dt.timedelta(seconds=2),
            completed_at=now,
        ))
        await s.commit()
    r = await client.get("/partials/completed")
    assert r.status_code == 200
    assert "dash-c" in r.text
    assert 'class="pill COMPLETED"' in r.text


@pytest.mark.asyncio
async def test_dashboard_wires_all_four_cards(client):
    """All four cards now have hx-get/hx-trigger."""
    r = await client.get("/")
    body = r.text
    for path in ("/partials/stats", "/partials/requests", "/partials/completed", "/partials/audit"):
        assert f'hx-get="{path}"' in body
