"""Shared pytest fixtures for the real-time analytics dashboard test suite."""

from __future__ import annotations

import pytest
import fakeredis.aioredis
from httpx import ASGITransport, AsyncClient

from src.main import app
from src.storage import RedisStorage


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit code 0 instead of 5).

    Useful for intermediate commits with no collected tests so that
    ``docker compose run --rm test pytest`` still returns green.
    """
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
async def fake_redis():
    """Provide a fakeredis async client for tests that need Redis."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
async def storage(fake_redis):
    """Provide a RedisStorage instance backed by fakeredis."""
    s = RedisStorage(client=fake_redis, metric_ttl_seconds=3600)
    yield s
    await s.close()


@pytest.fixture
async def client(storage):
    """Provide an async HTTP client wired to the FastAPI app with fake storage."""
    app.state.storage = storage
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
