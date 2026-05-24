"""Integration test: full erasure workflow on in-memory SQLite."""
from __future__ import annotations

import asyncio
from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.dependencies import get_session
from src.api.routes_erasure import router as erasure_router
from src.api.routes_tracking import router as tracking_router
from src.audit.verifier import verify_chain
from src.erasure.anonymization import is_anonymized
from src.erasure.coordinator import ErasureCoordinator
from src.persistence.models import UserDataMapping
from src.settings import Settings
from sqlalchemy import select


@pytest_asyncio.fixture
async def app(session_factory: async_sessionmaker[AsyncSession]) -> FastAPI:
    app = FastAPI()
    settings = Settings(
        database_url="sqlite+aiosqlite:///:memory:",
        anonymization_hash_salt="test-salt",
        verification_enabled=True,
        max_parallel_location_erasures=4,
        erasure_retry_count=1,
        erasure_retry_backoff_seconds=0,
    )
    app.state.session_factory = session_factory
    app.state.coordinator = ErasureCoordinator(session_factory, settings)
    app.include_router(tracking_router)
    app.include_router(erasure_router)

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


async def _wait_terminal(client: AsyncClient, request_id: str, timeout_s: float = 5.0) -> dict:
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        r = await client.get(f"/api/erasure-requests/{request_id}")
        assert r.status_code == 200
        body = r.json()
        if body["state"] in ("COMPLETED", "FAILED"):
            return body
        await asyncio.sleep(0.1)
    pytest.fail(f"erasure request {request_id} did not terminate within {timeout_s}s")


@pytest.mark.asyncio
async def test_full_anonymize_flow(client, session_factory):
    # 1. Register 3 mappings: 2 anonymisable + 1 PII (will fallback to DELETE)
    user = "u-flow"
    for dtype in ("system_logs", "analytics_events", "personal_profile"):
        r = await client.post(
            "/api/user-data-tracking",
            json={"user_id": user, "data_type": dtype, "storage_location": f"loc-{dtype}",
                  "metadata": {"user_id": user, "ip": "10.0.0.1", "msg": "x"}},
        )
        assert r.status_code == 201

    # 2. Submit ANONYMIZE
    r = await client.post(
        "/api/erasure-requests",
        json={"user_id": user, "request_type": "ANONYMIZE"},
    )
    assert r.status_code == 202
    body = r.json()
    rid = body["id"]
    assert body["state"] == "PENDING"
    assert len(body["audit_entries"]) >= 1
    assert body["audit_entries"][0]["event_type"] == "REQUEST_CREATED"

    # 3. Poll for terminal state
    final = await _wait_terminal(client, rid)
    assert final["state"] == "COMPLETED", final
    assert final["completed_at"] is not None

    # 4. Assert: anonymisable rows survive with _anonymized marker; PII row deleted
    async with session_factory() as s:
        rows = (await s.execute(
            select(UserDataMapping).where(UserDataMapping.user_id == user)
        )).scalars().all()
        by_type = {r.data_type: r for r in rows}
        assert "system_logs" in by_type
        assert "analytics_events" in by_type
        assert "personal_profile" not in by_type
        assert is_anonymized(by_type["system_logs"].metadata_json)

        # 5. Audit chain remains valid
        ok, bad = await verify_chain(s)
        assert ok is True, f"chain broken at {bad}"

    # 6. Final GET shows the full audit timeline + correct terminal state
    r = await client.get(f"/api/erasure-requests/{rid}")
    assert r.status_code == 200
    final = r.json()
    event_types = [e["event_type"] for e in final["audit_entries"]]
    assert "REQUEST_CREATED" in event_types
    assert "STATE_TRANSITION" in event_types
    assert "DISCOVERY_COMPLETE" in event_types
    assert "LOCATION_ERASED" in event_types
    assert final["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_delete_flow_removes_all_locations(client, session_factory):
    user = "u-del"
    for i in range(3):
        await client.post(
            "/api/user-data-tracking",
            json={"user_id": user, "data_type": "system_logs", "storage_location": f"loc-{i}"},
        )
    r = await client.post(
        "/api/erasure-requests",
        json={"user_id": user, "request_type": "DELETE"},
    )
    rid = r.json()["id"]
    final = await _wait_terminal(client, rid)
    assert final["state"] == "COMPLETED"

    async with session_factory() as s:
        rows = (await s.execute(
            select(UserDataMapping).where(UserDataMapping.user_id == user)
        )).scalars().all()
        assert rows == []
