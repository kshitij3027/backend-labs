"""Integration tests for tracking endpoints via httpx + ASGITransport."""
from __future__ import annotations

from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.dependencies import get_session
from src.api.routes_tracking import router as tracking_router


@pytest_asyncio.fixture
async def app(session_factory: async_sessionmaker[AsyncSession]) -> FastAPI:
    app = FastAPI()
    app.state.session_factory = session_factory
    app.include_router(tracking_router)

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
async def test_post_tracking_returns_201(client: AsyncClient):
    r = await client.post(
        "/api/user-data-tracking",
        json={"user_id": "u-1", "data_type": "system_logs", "storage_location": "loc-a"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["user_id"] == "u-1"
    assert body["id"] >= 1
    assert "created_at" in body


@pytest.mark.asyncio
async def test_post_tracking_is_idempotent(client: AsyncClient):
    payload = {
        "user_id": "u-2", "data_type": "system_logs",
        "storage_location": "loc", "data_path": "/var/log/x",
    }
    r1 = await client.post("/api/user-data-tracking", json=payload)
    r2 = await client.post("/api/user-data-tracking", json=payload)
    assert r1.status_code == 201
    assert r2.status_code == 201  # spec: idempotent → return existing, not 409
    assert r1.json()["id"] == r2.json()["id"]


@pytest.mark.asyncio
async def test_get_data_locations_returns_list(client: AsyncClient):
    user = "u-3"
    for dtype in ("system_logs", "analytics_events", "personal_profile"):
        await client.post(
            "/api/user-data-tracking",
            json={"user_id": user, "data_type": dtype, "storage_location": "loc-a"},
        )
    r = await client.get(f"/api/data-locations/{user}")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list)
    assert len(body) == 3
    types = {row["data_type"] for row in body}
    assert types == {"system_logs", "analytics_events", "personal_profile"}


@pytest.mark.asyncio
async def test_get_data_locations_empty_user(client: AsyncClient):
    r = await client.get("/api/data-locations/nobody")
    assert r.status_code == 200
    assert r.json() == []


@pytest.mark.asyncio
async def test_post_tracking_validation_error(client: AsyncClient):
    r = await client.post(
        "/api/user-data-tracking",
        json={"user_id": "", "data_type": "x", "storage_location": "y"},
    )
    assert r.status_code == 422
