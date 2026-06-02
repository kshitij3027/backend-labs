"""Shared pytest fixtures for the multi-tier caching layer test suite.

Provides the empty-run -> exit-0 hook, a fresh ``settings`` fixture, and (from
C8) an async ``redis_l2`` fixture yielding a connected, per-test-isolated
:class:`~src.l2_redis.L2Redis` against the real Redis wired by the compose
``test`` service. Later commits extend this with Postgres + ASGI ``client``
fixtures.
"""
from __future__ import annotations

import os

import pytest

from src.l2_redis import L2Redis
from src.settings import Settings

# The compose ``test`` service injects this; default keeps host-side collection
# from crashing on import even when Redis isn't reachable.
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Treat an empty test run as success (exit 0 instead of 5)."""
    if exitstatus == 5:
        session.exitstatus = 0


@pytest.fixture
def settings() -> Settings:
    """Return a fresh, non-cached Settings instance for tests."""
    return Settings()


@pytest.fixture
async def redis_l2():
    """Yield a connected L2Redis against the real test Redis.

    Flushes the DB before and after each test for isolation, then closes the
    pool. Requires a reachable Redis (compose ``test`` service / ``REDIS_URL``).
    """
    tier = L2Redis(REDIS_URL)
    await tier.connect()
    try:
        await tier.raw.flushdb()
        yield tier
    finally:
        try:
            await tier.raw.flushdb()
        finally:
            await tier.close()
