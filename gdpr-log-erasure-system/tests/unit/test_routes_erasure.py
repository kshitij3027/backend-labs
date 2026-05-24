"""Unit tests for erasure request endpoints (schema validation + 404 paths)."""
from __future__ import annotations

from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.dependencies import get_session
from src.api.routes_erasure import router as erasure_router
from src.api.schemas import ErasureRequestCreate
from src.erasure.coordinator import ErasureCoordinator
from src.settings import Settings


@pytest_asyncio.fixture
async def app(session_factory: async_sessionmaker[AsyncSession]) -> FastAPI:
    app = FastAPI()
    app.state.session_factory = session_factory
    app.state.coordinator = ErasureCoordinator(
        session_factory,
        Settings(
            database_url="sqlite+aiosqlite:///:memory:",
            anonymization_hash_salt="t",
            verification_enabled=False,
            erasure_retry_count=1,
            erasure_retry_backoff_seconds=0,
        ),
    )
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


def test_schema_validation_request_type_required():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ErasureRequestCreate(user_id="u-1")  # type: ignore[call-arg]


def test_schema_validation_request_type_must_be_valid():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ErasureRequestCreate(user_id="u-1", request_type="PURGE")  # type: ignore[arg-type]


def test_schema_accepts_delete_and_anonymize():
    p1 = ErasureRequestCreate(user_id="u-1", request_type="DELETE")
    p2 = ErasureRequestCreate(user_id="u-1", request_type="ANONYMIZE")
    assert p1.request_type.value == "DELETE"
    assert p2.request_type.value == "ANONYMIZE"


@pytest.mark.asyncio
async def test_get_unknown_request_returns_404(client):
    r = await client.get("/api/erasure-requests/non-existent-uuid")
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()
