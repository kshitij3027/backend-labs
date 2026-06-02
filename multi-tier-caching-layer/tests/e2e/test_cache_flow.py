"""In-process end-to-end flow test for the multi-tier caching layer.

This is the deterministic, single-process mirror of the cross-container
``scripts/verify_e2e.py``. It drives the **real** FastAPI app through its
production lifespan (via ``app.router.lifespan_context`` + an
``httpx.ASGITransport`` client), so the full object graph — L1 + L2 Redis +
Postgres pool + ``CacheManager`` + warmer — is built exactly as in production
against the compose ``test`` service's real Redis + Postgres.

It exercises the §5 success criteria end to end over HTTP:

* the **tier walk** — a cold query misses to ``backend``/``l3``, an identical
  repeat is an ``l1`` hit, and a cosmetically-different timestamp in the same
  300-second bucket also hits (semantic cache key);
* **graceful fallback** — with L1 + L2 cleared but L3 still populated, the next
  query is served from ``l3`` with no error; and with the L2 client closed
  (a simulated Redis outage, all L2 ops fail-soft) plus L1 cleared, a query
  still returns 200 (from L3 or the backend) and never a 5xx.

``pytest.ini`` sets ``asyncio_mode = auto`` so plain ``async def test_*`` run
without an explicit marker. As with ``tests/integration/test_api.py``, the
dataset is seeded via a short-lived pool to ``DATABASE_URL`` *before* the
lifespan is entered, and Redis is flushed so no stale L2 entry leaks a hit.
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
_BASE_PARAMS = {"source": "api", "start": 1_779_000_000, "end": 1_781_000_000}
_QUERY_BODY = {"query": "error_rate", "params": dict(_BASE_PARAMS)}


async def _seed_dataset() -> None:
    """Seed ~400 deterministic ``raw_logs`` and flush Redis for a clean run.

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
        await seed_raw_logs(pool, 400, seed=17, end_ts=1_780_000_000)
    finally:
        await pool.close()

    client = redis.asyncio.from_url(REDIS_URL)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


async def test_full_tier_walk_and_semantic_key() -> None:
    """Cold miss -> repeat L1 hit -> same-bucket cosmetic timestamp also hits."""
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1) Cold: a full miss computed by the slow backend (or read from a
            #    pre-materialized L3 row) — never a fast cache tier.
            first = await client.post("/query", json=_QUERY_BODY)
            assert first.status_code == 200
            body = first.json()
            assert "result" in body
            assert body["meta"]["tier"] in {"backend", "l3"}

            # 2) Immediate identical repeat short-circuits to the in-process L1.
            second = await client.post("/query", json=_QUERY_BODY)
            assert second.status_code == 200
            assert second.json()["meta"]["tier"] == "l1"

            # 3) Semantic key: 1_779_000_123 floors to the SAME 300s bucket as
            #    1_779_000_000, so this cosmetically-different query hits.
            semantic_body = {
                "query": "error_rate",
                "params": {**_BASE_PARAMS, "start": 1_779_000_123},
            }
            third = await client.post("/query", json=semantic_body)
            assert third.status_code == 200
            assert third.json()["meta"]["tier"] in {"l1", "l2"}


async def test_graceful_fallback_to_l3_then_l2_outage() -> None:
    """Clearing L1+L2 falls through to L3; a true L2 outage still serves 200.

    Stage 1 (fall-through to L3): warm the query across every tier, then clear
    L1 and flush L2 while leaving the L3 Postgres row intact. The next query
    must be served from ``l3`` with no error.

    Stage 2 (L2-outage resilience): close the L2 client (so *every* L2 op
    fail-softs and flips ``degraded``) and clear L1. A subsequent query must
    still return 200 — served from L3 or recomputed by the backend — and never
    a 5xx; ``meta.degraded`` may be ``True``. Reconnect is not required since
    the test ends here.
    """
    await _seed_dataset()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Warm the query into all tiers (cold compute populates L1 + L2 + L3).
            warm = await client.post("/query", json=_QUERY_BODY)
            assert warm.status_code == 200
            assert warm.json()["meta"]["tier"] in {"backend", "l3"}

            # --- Stage 1: clear L1 + L2, keep L3 -> next query hits L3 -------- #
            app.state.l1.clear()
            await app.state.l2.raw.flushdb()

            from_l3 = await client.post("/query", json=_QUERY_BODY)
            assert from_l3.status_code == 200
            assert from_l3.json()["meta"]["tier"] == "l3"

            # --- Stage 2: simulate a Redis outage (L2 ops fail-soft) --------- #
            # Closing the client sets the internal handle to None so every L2
            # operation degrades to a miss/no-op instead of raising.
            await app.state.l2.close()
            app.state.l1.clear()

            degraded = await client.post("/query", json=_QUERY_BODY)
            # The cache must keep serving despite L2 being down: 200, not 5xx,
            # served from L3 (still populated) or recomputed by the backend.
            assert degraded.status_code == 200
            meta = degraded.json()["meta"]
            assert meta["tier"] in {"l3", "backend"}
            assert degraded.status_code < 500
