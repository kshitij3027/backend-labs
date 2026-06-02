"""Shared pytest fixtures for the multi-tier caching layer test suite.

Provides the empty-run -> exit-0 hook, a fresh ``settings`` fixture, an async
``redis_l2`` fixture (from C8) yielding a connected, per-test-isolated
:class:`~src.l2_redis.L2Redis` against the real Redis wired by the compose
``test`` service, and (from C9) an async ``pg_pool`` fixture yielding a
schema-applied, per-test-truncated asyncpg pool against the real Postgres.
Later commits extend this with an ASGI ``client`` fixture.
"""
from __future__ import annotations

import os

import pytest

from src.db.pool import apply_schema, create_pool
from src.l2_redis import L2Redis
from src.settings import Settings

# The compose ``test`` service injects these; defaults keep host-side
# collection from crashing on import even when the services aren't reachable.
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://cache:cache@postgres:5432/cache"
)


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


@pytest.fixture
async def pg_pool():
    """Yield a schema-applied asyncpg pool against the real test Postgres.

    Applies the schema, then TRUNCATEs ``raw_logs`` and ``precomputed_aggregates``
    for per-test isolation before yielding. Closes the pool afterward. Requires a
    reachable Postgres (compose ``test`` service / ``DATABASE_URL``).
    """
    pool = await create_pool(DATABASE_URL)
    try:
        await apply_schema(pool)
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE raw_logs, precomputed_aggregates")
        yield pool
    finally:
        await pool.close()
