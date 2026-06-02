"""Integration tests for the FastAPI app surface.

The ``/health`` and lifespan-wiring tests drive the app **through its lifespan**
(via ``app.router.lifespan_context``) so ``app.state`` is populated exactly as in
production. From C15 the lifespan builds the *full* object graph (L1 + L2 Redis +
Postgres pool + cache manager + warmer), so the endpoint tests below run against
the **real** compose Redis + Postgres wired by the ``test`` service.

Before exercising ``/query`` we seed a small deterministic ``raw_logs`` corpus
into the same database (via a short-lived pool to ``DATABASE_URL``) so the slow
backend returns real data, and flush Redis so no stale L2 entry leaks a hit.

``pytest.ini`` sets ``asyncio_mode = auto``, so plain ``async def test_*``
functions run without an explicit ``@pytest.mark.asyncio`` decorator.
"""

from __future__ import annotations

import os

import httpx
from httpx import ASGITransport, AsyncClient

import redis.asyncio
from src.db.pool import apply_schema, create_pool
from src.db.seed import seed_raw_logs
from src.main import app

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://cache:cache@postgres:5432/cache"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

# A query window that brackets the seeded corpus (end_ts = 1_780_000_000).
_QUERY_BODY = {
    "query": "error_rate",
    "params": {"source": "api", "start": 1_779_000_000, "end": 1_781_000_000},
}


async def _seed_dataset() -> None:
    """Seed a small deterministic dataset and flush Redis for a clean run.

    Opens a short-lived pool to the same ``DATABASE_URL`` the app's lifespan
    uses, applies the (idempotent) schema, truncates the tables, and seeds 400
    rows. Then flushes the shared Redis DB so a stale L2 entry from a prior test
    cannot make the first ``/query`` an unexpected hit.
    """
    pool = await create_pool(DATABASE_URL)
    try:
        await apply_schema(pool)
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE raw_logs, precomputed_aggregates")
        await seed_raw_logs(pool, 400, seed=5, end_ts=1_780_000_000)
    finally:
        await pool.close()

    client = redis.asyncio.from_url(REDIS_URL)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


async def test_health_returns_healthy() -> None:
    """GET /health returns 200 with the exact ``{"status": "healthy"}`` body."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


async def test_lifespan_populates_settings() -> None:
    """The lifespan attaches the full object graph (incl. settings) to state."""
    async with app.router.lifespan_context(app):
        assert app.state.settings is not None
        # api_port is a core Settings field; confirm the wiring is real.
        assert isinstance(app.state.settings.api_port, int)
        # C15 also wires the rest of the graph onto app.state.
        assert app.state.cache_manager is not None
        assert app.state.metrics is not None
        assert app.state.warmer is not None


async def test_query_repeat_hits_l1() -> None:
    """First /query misses to backend/l3; an identical repeat is served from L1."""
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            first = await client.post("/query", json=_QUERY_BODY)
            assert first.status_code == 200
            body = first.json()
            assert "result" in body
            assert body["meta"]["tier"] in {"backend", "l3"}

            # Immediate identical repeat must short-circuit to L1.
            second = await client.post("/query", json=_QUERY_BODY)
            assert second.status_code == 200
            assert second.json()["meta"]["tier"] == "l1"


async def test_cache_stats_shape() -> None:
    """GET /cache/stats exposes the §8 performance fields, memory, and tiers."""
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Two queries so total_requests >= 2 (a miss then an L1 hit).
            await client.post("/query", json=_QUERY_BODY)
            await client.post("/query", json=_QUERY_BODY)

            resp = await client.get("/cache/stats")
            assert resp.status_code == 200
            data = resp.json()

            perf = data["performance"]
            assert isinstance(perf["overall_hit_rate"], float)
            assert perf["total_requests"] >= 2

            assert "memory" in data
            assert "l1" in data["tiers"]


async def test_invalidate_pattern_forces_recompute() -> None:
    """After invalidating ``q:*`` the same query is re-fetched (not from L1)."""
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/query", json=_QUERY_BODY)  # populate tiers
            hot_again = await client.post("/query", json=_QUERY_BODY)
            assert hot_again.json()["meta"]["tier"] == "l1"

            inv = await client.post("/cache/invalidate", json={"pattern": "q:*"})
            assert inv.status_code == 200
            counts = inv.json()
            assert set(counts) == {"l1", "l2", "l3"}

            # With all tiers purged for this key, the query must recompute.
            after = await client.post("/query", json=_QUERY_BODY)
            assert after.status_code == 200
            assert after.json()["meta"]["tier"] in {"backend", "l3"}


async def test_invalidate_requires_pattern_or_tags() -> None:
    """POST /cache/invalidate with an empty body is rejected (400 or 422)."""
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/cache/invalidate", json={})
            assert resp.status_code in {400, 422}


async def test_query_missing_query_is_422() -> None:
    """POST /query without a ``query`` field is a validation error (422)."""
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/query", json={})
            assert resp.status_code == 422


async def test_cache_hot_returns_list() -> None:
    """GET /cache/hot returns 200 with a ``hot`` list (populated after queries)."""
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/query", json=_QUERY_BODY)

            resp = await client.get("/cache/hot")
            assert resp.status_code == 200
            assert isinstance(resp.json()["hot"], list)
