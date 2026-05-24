from __future__ import annotations

from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.dependencies import get_session
from src.api.routes_stats import router as stats_router
from src.api.routes_tracking import router as tracking_router


@pytest_asyncio.fixture
async def app(session_factory: async_sessionmaker[AsyncSession]) -> FastAPI:
    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(tracking_router)
    app.include_router(stats_router)

    async def _override_get_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as s:
            try:
                yield s
            except Exception:
                await s.rollback()
                raise

    app.dependency_overrides[get_session] = _override_get_session
    return app


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_statistics_fresh_system(client):
    r = await client.get("/api/statistics")
    assert r.status_code == 200
    body = r.json()
    assert body["total_mappings"] == 0
    assert body["unique_users"] == 0
    assert body["completion_rate"] == 0.0
    assert body["data_type_counts"] == {}


@pytest.mark.asyncio
async def test_statistics_after_tracking(client):
    for uid, dtype in [("a", "system_logs"), ("a", "analytics_events"), ("b", "system_logs")]:
        await client.post(
            "/api/user-data-tracking",
            json={"user_id": uid, "data_type": dtype, "storage_location": "loc"},
        )
    r = await client.get("/api/statistics")
    body = r.json()
    assert body["total_mappings"] == 3
    assert body["unique_users"] == 2
    assert body["data_type_counts"] == {"analytics_events": 1, "system_logs": 2}
    assert body["completion_rate"] == 0.0  # no erasure requests yet
