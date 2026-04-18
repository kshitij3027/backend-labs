"""HTTP-level tests for ``GET /api/stats`` (C4).

These drive the full FastAPI app through the ``async_client`` fixture
so the stats endpoint reads live numbers from the same ``app.state.db``
and ``app.state.redis`` handles the rest of the API uses. No mocking.

Covered:

* Shape + types of the response (``total_logs``, ``facet_cardinality``,
  ``cache``, ``redis_reachable``).
* Cache counter reflection — after one miss + one hit on ``/api/search``
  the stats should report ``hits=1, misses=1, hit_rate=0.5``.
* Facet cardinality roughly matches the seeded generator distribution.
"""

from __future__ import annotations

import uuid

from httpx import AsyncClient

from src.config import settings
from src.search.query_builder import FACET_DIMS
from src.storage import redis_cache


async def _flush_redis() -> None:
    """Flush the shared Redis DB so stale keys from prior tests don't
    turn a fresh miss into a spurious hit (tests share one Redis server)."""
    client = await redis_cache.connect(settings.redis_url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


async def test_stats_shape(async_client: AsyncClient):
    """GET /api/stats returns the full StatsResponse shape with expected keys."""
    gen = await async_client.post("/api/logs/generate?count=500&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # --- top-level keys present ---
    expected_keys = {"total_logs", "facet_cardinality", "cache", "redis_reachable"}
    assert expected_keys.issubset(body.keys()), (
        f"missing keys: {expected_keys - set(body.keys())}"
    )

    # --- total_logs ---
    assert isinstance(body["total_logs"], int)
    assert body["total_logs"] == 500

    # --- facet_cardinality is a dict with every FACET_DIM -> int ---
    card = body["facet_cardinality"]
    assert isinstance(card, dict)
    assert set(card.keys()) == set(FACET_DIMS)
    for dim, n in card.items():
        assert isinstance(n, int), f"cardinality for {dim} not int: {n!r}"

    # --- cache block ---
    cache = body["cache"]
    assert set(cache.keys()) == {"hits", "misses", "errors", "hit_rate"}
    assert isinstance(cache["hits"], int)
    assert isinstance(cache["misses"], int)
    assert isinstance(cache["errors"], int)
    # hit_rate is either a float in [0, 1] or None when denominator==0.
    assert cache["hit_rate"] is None or 0.0 <= cache["hit_rate"] <= 1.0

    # --- redis_reachable is a bool (None-safe JSON check) ---
    assert isinstance(body["redis_reachable"], bool)


async def test_stats_reflects_cache_activity(async_client: AsyncClient):
    """After 1 miss + 1 hit on /api/search, /api/stats must report hits=1, misses=1.

    We reset the shared counter first so the absolute numbers are
    deterministic regardless of whether other tests ran before us
    inside the same process.
    """
    await _flush_redis()

    gen = await async_client.post("/api/logs/generate?count=200&seed=42")
    assert gen.status_code == 201, gen.text

    redis_cache.stats.reset()

    # Unique nonce in ``query`` so the cache key is guaranteed fresh
    # even if FLUSHDB missed a key (e.g. a parallel test is running).
    body = {
        "filters": {"service": ["payments"]},
        "query": f"__nonce_{uuid.uuid4().hex}__",
    }
    r1 = await async_client.post("/api/search", json=body)
    assert r1.status_code == 200, r1.text
    r2 = await async_client.post("/api/search", json=body)
    assert r2.status_code == 200, r2.text

    # One miss, one hit so far. GET /api/stats will itself call
    # ``redis_cache.ping`` but that does not bump the hit/miss counters.
    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    cache = resp.json()["cache"]

    assert cache["hits"] == 1
    assert cache["misses"] == 1
    assert cache["hit_rate"] == 0.5


async def test_stats_facet_cardinality_matches_seeded(async_client: AsyncClient):
    """A 500-row seed should light up every facet dim with >0 distinct values.

    Upper bounds come from the generator: 5 services, 5 levels, 4 regions,
    4 latency buckets, up to 24 hours. All must be >0 for a seed this size
    (timestamps span the last 24h so multiple hour buckets will appear).
    """
    gen = await async_client.post("/api/logs/generate?count=500&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    card = resp.json()["facet_cardinality"]

    # Upper bounds from the generator design.
    assert 0 < card["service"] <= 5
    assert 0 < card["level"] <= 5
    assert 0 < card["region"] <= 4
    assert 0 < card["latency_bucket"] <= 4
    assert 0 < card["hour_bucket"] <= 24
