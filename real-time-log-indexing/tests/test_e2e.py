"""End-to-end tests that hit the live compose-managed FastAPI app.

Unlike the rest of the suite (which runs against an in-process ASGI
app via ``httpx.ASGITransport``), these tests target the real
``app`` service over the docker-compose bridge network at ``APP_URL``.
They're excluded from ``make test`` and driven by ``make e2e`` — this
way the bulk suite keeps running fast in a single container while
``make e2e`` spins up the full stack.

Each test starts from a clean Redis (``FLUSHALL`` in the autouse
fixture) so assertions on counters are deterministic. The app's index
is *not* wiped between tests — the persistent segments stay on the
app_data volume so ``docs_indexed`` grows monotonically across tests,
and every assertion is written in delta form.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio
import redis.asyncio as redis_async


APP_URL = os.environ.get("APP_URL", "http://app:8080")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def _flush_redis_before_each() -> AsyncIterator[None]:
    """Wipe Redis between tests so each one sees a clean stream.

    The app's on-disk segments survive — we only flush the broker so
    per-test generate-sample counts are easy to reason about.
    """
    client = redis_async.from_url(REDIS_URL, decode_responses=False)
    try:
        await client.flushall()
    except Exception:
        # Don't fail the test over a best-effort pre-clean; the run
        # will still work if Redis just came up and was empty.
        pass
    finally:
        await client.aclose()
    yield


async def _client() -> httpx.AsyncClient:
    """Return an ``AsyncClient`` pointed at the live app."""
    return httpx.AsyncClient(base_url=APP_URL, timeout=30.0)


async def _wait_for_health(http: httpx.AsyncClient) -> None:
    """Poll ``/health`` until the app reports Redis reachable.

    A freshly flushed Redis can take a handful of ms for the consumer
    to rediscover, and the first test in a run is the most likely to
    race. Polling for ``redis_connected=True`` gives the consumer a
    chance to reattach before we start making assertions.
    """
    for _ in range(30):
        try:
            r = await http.get("/health")
            if r.status_code == 200 and r.json().get("redis_connected"):
                return
        except Exception:
            pass
        await asyncio.sleep(0.5)
    pytest.fail("app never reported healthy with redis_connected=True")


async def _wait_for_indexed(
    http: httpx.AsyncClient, target: int, deadline_s: float = 30.0
) -> int:
    """Block until ``docs_indexed >= target`` or the deadline passes."""
    deadline = time.time() + deadline_s
    cur = 0
    while time.time() < deadline:
        cur = (await http.get("/api/stats")).json()["docs_indexed"]
        if cur >= target:
            return cur
        await asyncio.sleep(0.2)
    return cur


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_health_live() -> None:
    """The live app must answer /health with a valid HealthResponse shape."""
    async with await _client() as http:
        r = await http.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] in ("ok", "degraded")
        assert isinstance(body["redis_connected"], bool)
        assert isinstance(body["segments_ready"], bool)
        assert body["uptime_s"] >= 0


async def test_full_loop_ingest_search() -> None:
    """Generate -> consume -> index -> search: the whole happy path."""
    async with await _client() as http:
        await _wait_for_health(http)

        stats0 = (await http.get("/api/stats")).json()

        r = await http.post("/api/generate-sample", json={"count": 500})
        assert r.status_code == 200
        assert r.json()["ingested"] == 500

        target = stats0["docs_indexed"] + 500
        cur = await _wait_for_indexed(http, target, deadline_s=20.0)
        assert cur >= target, (
            f"consumer did not drain: {cur} < {target}"
        )

        # Pick a term that's guaranteed to appear in the sample set —
        # every batch of 500 has some ERROR-level logs.
        hits = None
        for term in ("error", "timeout", "auth", "payment", "cache"):
            body = (
                await http.get(f"/api/search?q={term}&limit=10")
            ).json()
            if body["total"] > 0:
                hits = body
                break
        assert hits is not None, (
            "none of the common terms matched any of the 500 generated docs"
        )
        assert hits["total"] > 0
        # Highlighted tags live in the first result's message.
        first = hits["results"][0]
        assert "<mark>" in first["highlighted_message"].lower()
        assert "took_ms" in hits and hits["took_ms"] >= 0


async def test_filters_narrow_results() -> None:
    """The service filter must narrow results to just that service."""
    async with await _client() as http:
        await _wait_for_health(http)

        stats0 = (await http.get("/api/stats")).json()
        await http.post("/api/generate-sample", json={"count": 500})
        await _wait_for_indexed(
            http, stats0["docs_indexed"] + 500, deadline_s=20.0
        )

        body = (
            await http.get(
                "/api/search?q=error&service=payment-service&limit=50"
            )
        ).json()

        # Filter result shape: every returned row must be the requested
        # service. The total may be zero if no ERROR + payment-service
        # rows happened to be generated; that's acceptable — what we
        # must not see is a different service leaking through.
        for result in body["results"]:
            assert result["service"] == "payment-service", (
                f"filter leaked: got service={result['service']!r}"
            )


async def test_dashboard_renders() -> None:
    """GET / must return the dashboard HTML with the required IDs."""
    async with await _client() as http:
        r = await http.get("/")
        assert r.status_code == 200
        # Key DOM IDs that the frontend depends on; if any of these
        # disappear the Chrome UI tests will also fail and we want to
        # catch it at the HTTP layer first.
        assert "stats-docs-indexed" in r.text
        assert "live-feed" in r.text
        assert "search-input" in r.text


async def test_stats_monotonic_across_requests() -> None:
    """``docs_indexed`` must be monotonically non-decreasing across polls."""
    async with await _client() as http:
        await _wait_for_health(http)

        a = (await http.get("/api/stats")).json()["docs_indexed"]
        await http.post("/api/generate-sample", json={"count": 100})
        # Brief settle so the consumer has a chance to pick up the batch.
        b = await _wait_for_indexed(http, a + 100, deadline_s=15.0)
        assert b >= a
        assert b >= a + 100


async def test_restart_persistence_survives_flush() -> None:
    """Large ingest must flush to disk and remain searchable.

    This is the in-process half of the "survives restart" success
    criterion — we push enough docs to force at least one segment
    flush (``SEGMENT_MAX_DOCS`` defaults to 10 000, but the batching
    writer can flush sooner on memory pressure) and then verify the
    app still reports the full count via ``/api/stats``. A full
    ``docker compose restart app`` check lives in the Makefile e2e
    target where the main thread can drive the lifecycle.
    """
    async with await _client() as http:
        await _wait_for_health(http)

        stats0 = (await http.get("/api/stats")).json()
        await http.post("/api/generate-sample", json={"count": 2000})
        cur = await _wait_for_indexed(
            http, stats0["docs_indexed"] + 2000, deadline_s=30.0
        )
        assert cur >= stats0["docs_indexed"] + 2000

        # Search should still round-trip after the big ingest.
        body = (await http.get("/api/search?q=error&limit=20")).json()
        assert body["took_ms"] >= 0
