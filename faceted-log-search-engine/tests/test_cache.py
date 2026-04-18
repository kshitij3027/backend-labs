"""Unit + HTTP-level tests for the Redis cache-aside layer (C4).

These tests run **inside the Docker test container** where a real
``redis:7-alpine`` service is reachable at ``redis://redis:6379`` via
compose networking (``REDIS_URL`` env in ``docker-compose.yml``). We
**do not** mock Redis for the happy path — the whole point of the
cache layer is to verify round-trips against a real server.

What we cover:

* ``make_key`` determinism + prefix shape.
* ``connect`` / ``ping`` lifecycle incl. graceful false on bad host.
* ``get_or_compute`` miss-then-hit behaviour, no-client fallback,
  and single-compute-per-miss invariant.
* HTTP-level miss → hit on ``POST /api/search`` and ``GET /api/facets``.
* Key independence when filters differ.
* Simulated Redis outage via a monkeypatched flaky client on
  ``app.state.redis`` — must still return a valid 200 response.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError as RedisConnectionError

from src.config import settings
from src.storage import redis_cache


# ---------------------------------------------------------------------------
# make_key
# ---------------------------------------------------------------------------

def test_make_key_deterministic():
    """Two logically-equal payloads (dict order varied) hash the same.

    The implementation serializes with ``sort_keys=True`` so Python's
    dict iteration order cannot leak into the digest. If this ever
    regresses, search result caching would partition by insertion order
    and hit-rates would collapse.
    """
    a = redis_cache.make_key("search", {"a": 1, "b": 2})
    b = redis_cache.make_key("search", {"b": 2, "a": 1})
    assert a == b


def test_make_key_includes_prefix():
    """Keys are ``"<prefix>:<digest>"`` so we can scope per-endpoint."""
    key = redis_cache.make_key("search", {})
    assert key.startswith("search:")
    # After the prefix + ':' we expect a 40-char sha1 hexdigest.
    _, _, digest = key.partition(":")
    assert len(digest) == 40
    assert all(c in "0123456789abcdef" for c in digest)


# ---------------------------------------------------------------------------
# connect + ping lifecycle
# ---------------------------------------------------------------------------

async def test_ping_returns_true_when_connected():
    """Real Redis reachable -> ping returns True."""
    client = await redis_cache.connect(settings.redis_url)
    try:
        assert await redis_cache.ping(client) is True
    finally:
        await client.aclose()


async def test_ping_returns_false_on_bad_host():
    """Unresolvable host -> ping swallows the error and returns False.

    The short ``socket_connect_timeout`` set inside ``connect`` means
    this resolves in well under a second, so the test stays quick
    even on a misbehaving DNS.
    """
    client = await redis_cache.connect("redis://nonexistent-host-xyz:6379")
    try:
        # Guard with a generous-but-finite timeout so we fail loud if
        # the connect timeout was ever accidentally removed.
        result = await asyncio.wait_for(redis_cache.ping(client), timeout=3.0)
        assert result is False
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# get_or_compute core behaviour
# ---------------------------------------------------------------------------

async def test_get_or_compute_miss_then_hit():
    """First call misses + computes, second call hits the cache.

    Uses a fresh unique key so parallel/prior test runs don't pollute
    this one. Resets the shared ``stats`` dataclass at the top so the
    counter assertions are absolute.
    """
    client = await redis_cache.connect(settings.redis_url)
    try:
        redis_cache.stats.reset()
        key = f"test:miss_then_hit:{uuid.uuid4().hex}"

        async def _compute() -> dict:
            return {"value": 42, "note": "computed"}

        # --- Miss ---
        value1, hit1 = await redis_cache.get_or_compute(client, key, _compute, ttl=30)
        assert hit1 is False
        assert value1 == {"value": 42, "note": "computed"}
        assert redis_cache.stats.misses == 1
        assert redis_cache.stats.hits == 0

        # --- Hit ---
        value2, hit2 = await redis_cache.get_or_compute(client, key, _compute, ttl=30)
        assert hit2 is True
        assert value2 == value1
        assert redis_cache.stats.hits == 1
        assert redis_cache.stats.misses == 1
    finally:
        await client.aclose()


async def test_get_or_compute_no_client_falls_through():
    """``client=None`` -> compute runs, ``errors`` bumped, no raise."""
    redis_cache.stats.reset()

    ran = {"n": 0}

    async def _compute() -> dict:
        ran["n"] += 1
        return {"ok": True}

    value, was_hit = await redis_cache.get_or_compute(
        client=None,
        key="k",
        compute=_compute,
        ttl=5,
    )
    assert was_hit is False
    assert value == {"ok": True}
    assert ran["n"] == 1
    # errors bumped because we treat "no client" as a cache outage
    # from the caller's perspective.
    assert redis_cache.stats.errors == 1


async def test_get_or_compute_compute_called_once_per_miss():
    """Compute runs exactly once on a miss, zero times on a hit."""
    client = await redis_cache.connect(settings.redis_url)
    try:
        redis_cache.stats.reset()
        key = f"test:once_per_miss:{uuid.uuid4().hex}"

        calls = {"n": 0}

        async def _compute() -> dict:
            calls["n"] += 1
            return {"payload": "v"}

        # Miss -> compute called exactly once.
        await redis_cache.get_or_compute(client, key, _compute, ttl=30)
        assert calls["n"] == 1

        # Hit -> compute must NOT run again. Counter stays at 1.
        await redis_cache.get_or_compute(client, key, _compute, ttl=30)
        assert calls["n"] == 1
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# HTTP-level cache integration (via async_client fixture)
# ---------------------------------------------------------------------------

async def _flush_redis() -> None:
    """Flush the shared Redis so previous tests can't leak cache hits.

    Tests that assert on cache miss/hit counts call this before
    seeding so the exact numbers are deterministic regardless of
    which other tests ran in the same pytest process.
    """
    client = await redis_cache.connect(settings.redis_url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


async def test_http_search_miss_then_hit(async_client: AsyncClient):
    """Two identical POST /api/search calls: first cached=false, second cached=true.

    We also assert the second call's ``query_time_ms`` is smaller than
    the first — on a hit we overwrite ``query_time_ms`` with the true
    request-level elapsed time in ``api/search.py`` so it reflects
    cache speed instead of the cached miss cost.
    """
    await _flush_redis()

    # Seed some data so filters have rows to match.
    gen = await async_client.post("/api/logs/generate?count=300&seed=42")
    assert gen.status_code == 201, gen.text

    redis_cache.stats.reset()

    # Include a unique nonce so this test's cache key is distinct
    # from any other test's, even if the flush above races. The
    # nonce lives under ``filters.service`` is not ideal — instead
    # we stick it in ``query`` which the generator never matches.
    body = {"filters": {"service": ["payments"]}, "query": f"__nonce_{uuid.uuid4().hex}__"}

    # --- first call: miss ---
    r1 = await async_client.post("/api/search", json=body)
    assert r1.status_code == 200, r1.text
    j1 = r1.json()
    assert j1["cached"] is False
    assert redis_cache.stats.misses >= 1

    # --- second call: hit ---
    r2 = await async_client.post("/api/search", json=body)
    assert r2.status_code == 200, r2.text
    j2 = r2.json()
    assert j2["cached"] is True
    assert j2["query_time_ms"] < j1["query_time_ms"], (
        f"expected cached response to be faster: "
        f"miss={j1['query_time_ms']}ms hit={j2['query_time_ms']}ms"
    )


async def test_http_facets_miss_then_hit(async_client: AsyncClient):
    """GET /api/facets: same miss -> hit pattern as /api/search."""
    await _flush_redis()

    gen = await async_client.post("/api/logs/generate?count=300&seed=42")
    assert gen.status_code == 201, gen.text

    redis_cache.stats.reset()

    # Unique free-text query so this test's cache key doesn't collide
    # with other tests that hit /api/facets with the same filters.
    params = {"service": "payments", "query": f"__nonce_{uuid.uuid4().hex}__"}

    r1 = await async_client.get("/api/facets", params=params)
    assert r1.status_code == 200, r1.text
    j1 = r1.json()
    assert j1["cached"] is False
    assert redis_cache.stats.misses >= 1

    r2 = await async_client.get("/api/facets", params=params)
    assert r2.status_code == 200, r2.text
    j2 = r2.json()
    assert j2["cached"] is True


async def test_http_different_filters_different_keys(async_client: AsyncClient):
    """Two distinct filter sets -> two cache misses (independent keys)."""
    await _flush_redis()

    gen = await async_client.post("/api/logs/generate?count=300&seed=42")
    assert gen.status_code == 201, gen.text

    redis_cache.stats.reset()

    # Use a shared nonce across the two bodies so neither collides with
    # any prior test, but the ``filters`` difference is what we assert on.
    nonce = f"__nonce_{uuid.uuid4().hex}__"
    body_a = {"filters": {"service": ["payments"]}, "query": nonce}
    body_b = {"filters": {"service": ["auth"]}, "query": nonce}

    r_a = await async_client.post("/api/search", json=body_a)
    assert r_a.status_code == 200, r_a.text
    assert r_a.json()["cached"] is False

    r_b = await async_client.post("/api/search", json=body_b)
    assert r_b.status_code == 200, r_b.text
    # DIFFERENT filters -> must also be a miss; otherwise cache keys
    # would be colliding across logically-distinct requests.
    assert r_b.json()["cached"] is False
    assert redis_cache.stats.misses >= 2


# ---------------------------------------------------------------------------
# Simulated Redis outage via a flaky monkeypatched client.
# ---------------------------------------------------------------------------

# The "stop the real redis container" form of this test would require
# Docker-in-Docker inside the test container, which we don't have. We
# emulate the failure mode with a fake Redis client instead — the
# fallback path in ``get_or_compute`` doesn't distinguish between
# "real connection error" and "client raised ConnectionError", so the
# contract we care about (request still succeeds) is covered.

class _FlakyRedis:
    """A minimal async stub that always raises on the hot-path methods."""

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("simulated down")

    async def setex(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("simulated down")

    async def ping(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("simulated down")

    async def aclose(self) -> None:
        return None


@pytest.mark.skipif(
    os.getenv("SKIP_REDIS_DOWN_TEST") == "1",
    reason="SKIP_REDIS_DOWN_TEST=1 -> skip simulated Redis-outage test",
)
async def test_redis_down_search_still_succeeds(
    tmp_db_path,
    monkeypatch: pytest.MonkeyPatch,
):
    """When Redis connection errors surface, search still returns 200.

    We manually drive the lifespan so the normal ``app.state.redis``
    is set up, then swap it out for a ``_FlakyRedis`` instance that
    always raises ``RedisConnectionError``. The handler must catch
    this in ``get_or_compute`` and still return a valid response with
    ``cached=False``. The ``errors`` counter must bump.
    """
    from src.main import app

    async with app.router.lifespan_context(app):
        # Swap in the flaky client AFTER startup so generate can run
        # through the normal happy path (we just care about the search
        # call below).
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            gen = await client.post("/api/logs/generate?count=100&seed=42")
            assert gen.status_code == 201, gen.text

            # Monkeypatch the Redis client for the duration of the test.
            real_redis = app.state.redis
            try:
                app.state.redis = _FlakyRedis()
                redis_cache.stats.reset()

                resp = await client.post(
                    "/api/search", json={"filters": {"service": ["payments"]}}
                )
                assert resp.status_code == 200, resp.text
                body = resp.json()
                # Redis fell over -> fallback compute path -> cached=False.
                assert body["cached"] is False
                # errors must bump (at least the GET raised).
                assert redis_cache.stats.errors >= 1
                # Response is otherwise well-formed — facets + logs keys.
                assert "facets" in body
                assert "logs" in body
            finally:
                app.state.redis = real_redis
