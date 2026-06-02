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

from src.cache_manager import CacheManager
from src.db.pool import apply_schema, create_pool
from src.db.seed import seed_raw_logs
from src.l1_cache import L1Cache
from src.l2_redis import L2Redis
from src.metrics import Metrics
from src.patterns import PatternEngine
from src.settings import Settings
from src.singleflight import SingleFlight

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


@pytest.fixture
async def cache_manager(redis_l2, pg_pool):
    """Yield a fully-wired :class:`~src.cache_manager.CacheManager`.

    Builds the keystone read-through manager over the REAL per-test Redis
    (``redis_l2``) and Postgres (``pg_pool``) fixtures, with fresh in-process
    collaborators (L1, metrics, patterns, single-flight). A small deterministic
    ``raw_logs`` dataset is seeded so backend aggregations return real data.

    A 100ms ``backend_delay_ms`` makes the slow path measurably slower than a
    cache hit (so timing assertions hold) and guarantees concurrent cold gets
    overlap (so the single-flight test can observe coalescing). The manager's
    ``l1``/``l2``/``pg_pool`` are exposed as attributes for assertions.
    """
    # Seed a deterministic corpus so backend GROUP BY queries return data.
    await seed_raw_logs(pg_pool, 500, seed=99, end_ts=1_780_000_000)

    manager = CacheManager(
        l1=L1Cache(max_size=1000, ttl=300),
        l2=redis_l2,
        pg_pool=pg_pool,
        metrics=Metrics(),
        patterns=PatternEngine(),
        singleflight=SingleFlight(),
        time_bucket_seconds=300,
        backend_delay_ms=100,
        l2_ttl_seconds=600,
        l2_compress=False,
    )
    yield manager
