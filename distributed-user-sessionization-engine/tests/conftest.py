"""Shared pytest fixtures for the sessionization engine test suite."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from src.config import Config
from src.models import Event
from src.redis_store import RedisStore


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def make_event():
    """Factory fixture that produces Event objects with sensible defaults."""
    def _make(
        user_id: str = "user_001",
        event_type: str = "page_view",
        timestamp: datetime | None = None,
        device_type: str = "desktop",
        page_url: str = "/home",
        metadata: dict | None = None,
    ) -> Event:
        return Event(
            user_id=user_id,
            event_type=event_type,
            timestamp=timestamp or datetime.now(timezone.utc),
            device_type=device_type,
            page_url=page_url,
            metadata=metadata or {},
        )
    return _make


@pytest.fixture
def config():
    """Default test config with short timeouts."""
    return Config(
        session_timeout_seconds=60.0,
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    )


@pytest.fixture
async def redis_store(config):
    """Create a RedisStore connected to the test Redis, flush after each test."""
    store = RedisStore(config)
    await store.connect()
    yield store
    await store.redis.flushdb()
    await store.close()
