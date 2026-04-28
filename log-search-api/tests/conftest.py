from __future__ import annotations

import os
from typing import AsyncIterator
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="session")
def app_instance() -> FastAPI:
    os.environ.setdefault("SECRET_KEY", "test-secret-key-please-change")
    from src.clients.elasticsearch import get_es
    from src.clients.redis import get_redis
    from src.main import build_app

    app = build_app()

    fake_es = AsyncMock()
    fake_es.cluster = AsyncMock()
    fake_es.cluster.health = AsyncMock(return_value={})
    fake_es.close = AsyncMock(return_value=None)

    fake_redis = AsyncMock()
    fake_redis.ping = AsyncMock(return_value=True)
    fake_redis.aclose = AsyncMock(return_value=None)

    app.dependency_overrides[get_es] = lambda: fake_es
    app.dependency_overrides[get_redis] = lambda: fake_redis

    return app


@pytest_asyncio.fixture
async def async_client(app_instance: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture(scope="session")
def live_url() -> str:
    return os.getenv("API_URL", "http://api:8000")


@pytest.fixture
def api_token() -> str:
    os.environ.setdefault("SECRET_KEY", "test-secret-key-please-change")
    from src.auth.security import create_access_token
    from src.config import Settings

    settings = Settings()
    token, _ = create_access_token("testuser", settings)
    return token
