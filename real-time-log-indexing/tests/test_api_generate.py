"""Tests for ``POST /api/generate-sample``.

The endpoint closes the full ingest loop:

    sample_data → XADD → Redis stream → RedisStreamConsumer → InvertedIndex → /api/search

so most of the real value here is end-to-end: push N synthetic logs,
wait for the consumer (running as a background task inside the
fixture's lifespan) to drain the stream, and confirm the index
counters have moved and the data is searchable.

The tests use the shared ``async_client`` + ``app_instance`` fixtures
from :mod:`tests.conftest` — that runs the real FastAPI lifespan and
therefore starts a real consumer task against real Redis. If Redis
isn't reachable in the test environment the push endpoint returns 503
and the roundtrip tests skip, keeping the suite green on the host.
"""

from __future__ import annotations

import asyncio
import os

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import AsyncClient


@pytest_asyncio.fixture(autouse=True)
async def _flush_redis_between_tests():
    """Flush Redis state before each test so stream offsets, consumer group
    cursors, and leftover XADD entries from earlier tests cannot bleed in."""
    import redis.asyncio as redis_async
    url = os.environ.get("REDIS_URL", "redis://redis:6379")
    try:
        client = redis_async.from_url(url, decode_responses=False)
        await client.flushall()
        await client.aclose()
    except Exception:
        pass
    yield


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

async def test_generate_requires_positive_count(async_client: AsyncClient) -> None:
    """count=0 must be rejected by Pydantic's ge=1 constraint → 422."""
    r = await async_client.post("/api/generate-sample", json={"count": 0})
    assert r.status_code == 422


async def test_generate_rejects_count_too_large(async_client: AsyncClient) -> None:
    """count above 100_000 must be rejected by le=100_000 → 422."""
    r = await async_client.post("/api/generate-sample", json={"count": 200_000})
    assert r.status_code == 422


async def test_generate_negative_rate_rejected(async_client: AsyncClient) -> None:
    """rate must be strictly positive per gt=0 → negative rate → 422."""
    r = await async_client.post(
        "/api/generate-sample", json={"count": 10, "rate": -1}
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Happy path — default count + response shape
# ---------------------------------------------------------------------------

async def test_generate_default_count_is_500(async_client: AsyncClient) -> None:
    """With no count in the body, the default (500) kicks in."""
    r = await async_client.post("/api/generate-sample", json={})
    if r.status_code == 503:
        pytest.skip("Redis not connected in test env")
    assert r.status_code == 200

    body = r.json()
    assert body["ingested"] == 500
    assert body["stream"] == "logs"
    assert body["took_ms"] >= 0


# ---------------------------------------------------------------------------
# End-to-end: ingest + index counter moves
# ---------------------------------------------------------------------------

async def test_generate_small_batch_indexed(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """Pushing 20 logs must be observed by the consumer within ~3 s.

    We record the docs_indexed baseline first (the suite seeds into
    the same index in other tests — the fixture guarantees a clean
    segment dir per test but not a clean process between them in
    parametrised runs).
    """
    initial = app_instance.state.index.stats()["docs_indexed"]

    r = await async_client.post("/api/generate-sample", json={"count": 20})
    if r.status_code == 503:
        pytest.skip("Redis not connected")
    assert r.status_code == 200

    # Wait up to 3 s for the consumer to drain the 20 messages.
    deadline = asyncio.get_event_loop().time() + 3.0
    while asyncio.get_event_loop().time() < deadline:
        if app_instance.state.index.stats()["docs_indexed"] >= initial + 20:
            break
        await asyncio.sleep(0.1)

    assert app_instance.state.index.stats()["docs_indexed"] >= initial + 20


async def test_generate_then_search_roundtrip(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """Push 100 logs, wait for the index to catch up, then search.

    The templates in :mod:`src.sample_data` include plenty of
    ERROR-level templates that contain the word ``error`` or
    ``failed``. We fall back to a very common token (``service``)
    if ``error`` happens to be absent for this seed so the test is
    robust to template-pool shifts.
    """
    r = await async_client.post("/api/generate-sample", json={"count": 100})
    if r.status_code == 503:
        pytest.skip("Redis not connected")
    assert r.status_code == 200

    # Wait up to ~5 s for all 100 to land. 0.1 s * 50 = 5 s.
    for _ in range(50):
        if app_instance.state.index.stats()["docs_indexed"] >= 100:
            break
        await asyncio.sleep(0.1)

    sr = await async_client.get("/api/search?q=error&limit=50")
    assert sr.status_code == 200
    results = sr.json()["results"]
    if not results:
        # Fallback: "service" appears in message templates and in the
        # service names ("auth-service" etc.), so some docs will have
        # it on any seed. If this also comes back empty, something is
        # genuinely wrong with the ingest loop.
        sr2 = await async_client.get("/api/search?q=service&limit=50")
        assert sr2.status_code == 200
        results = sr2.json()["results"]

    assert len(results) > 0


# ---------------------------------------------------------------------------
# Rate-limited push — throttled path
# ---------------------------------------------------------------------------

async def test_generate_rate_slow(async_client: AsyncClient) -> None:
    """rate=200 throttles to ~5 ms between each XADD.

    Four messages therefore need at least 3 * (1/200) s = 15 ms of
    sleep, and should complete well under 2 s. The upper bound is
    loose because the Redis round-trip on a slow test host can add
    a few hundred ms.
    """
    r = await async_client.post(
        "/api/generate-sample", json={"count": 4, "rate": 200}
    )
    if r.status_code == 503:
        pytest.skip("Redis not connected")
    assert r.status_code == 200

    body = r.json()
    assert body["ingested"] == 4
    assert 10 <= body["took_ms"] <= 2000
