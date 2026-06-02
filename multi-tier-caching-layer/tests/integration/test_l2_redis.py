"""Integration tests for the Redis-backed L2 tier (:mod:`src.l2_redis`).

These run against a **real Redis** (the compose ``test`` service injects
``REDIS_URL=redis://redis:6379/0`` and depends on the ``redis`` service being
healthy). ``pytest.ini`` sets ``asyncio_mode = auto``, so plain
``async def test_*`` functions run without an explicit decorator.

Each test gets the module-local ``l2`` fixture, which connects an
:class:`~src.l2_redis.L2Redis`, ``flushdb``s + closes afterwards for isolation.
The graceful-degradation test deliberately builds its own client pointed at an
unreachable host (so it must NOT share the live fixture).
"""
from __future__ import annotations

import os

import pytest

from src.l2_redis import L2Redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")


@pytest.fixture
async def l2():
    """Yield a connected L2Redis against the real test Redis; flush+close after."""
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


# ---------------------------------------------------------------------------
# Round-trip: set -> get (compressed and uncompressed)
# ---------------------------------------------------------------------------
async def test_set_get_roundtrip_compressed(l2: L2Redis) -> None:
    """A dict survives a compressed set -> get round-trip and counts as a hit."""
    payload = {"rows": [{"source": "api", "count": 42}], "total": 42}
    assert await l2.set("q:abc", payload, compress=True) is True

    got = await l2.get("q:abc")
    assert got == payload
    assert l2.hits == 1
    assert l2.misses == 0


async def test_set_get_roundtrip_uncompressed(l2: L2Redis) -> None:
    """The same round-trip works with compression disabled."""
    payload = {"rows": [{"source": "web", "count": 7}], "total": 7}
    assert await l2.set("q:plain", payload, compress=False) is True

    got = await l2.get("q:plain")
    assert got == payload
    assert l2.hits == 1


async def test_get_missing_returns_none_and_counts_miss(l2: L2Redis) -> None:
    """A missing key yields None and increments the miss counter."""
    assert await l2.get("q:does-not-exist") is None
    assert l2.misses == 1
    assert l2.hits == 0


# ---------------------------------------------------------------------------
# Pattern invalidation (SCAN-based, never KEYS)
# ---------------------------------------------------------------------------
async def test_invalidate_pattern_only_matching(l2: L2Redis) -> None:
    """invalidate_pattern('q:*') deletes only q:* keys, leaving others intact."""
    await l2.set("q:1", {"v": 1})
    await l2.set("q:2", {"v": 2})
    await l2.set("other:1", {"v": 99})

    deleted = await l2.invalidate_pattern("q:*")
    assert deleted == 2

    assert await l2.get("q:1") is None
    assert await l2.get("q:2") is None
    # The non-matching key must survive.
    assert await l2.get("other:1") == {"v": 99}


# ---------------------------------------------------------------------------
# Tag invalidation
# ---------------------------------------------------------------------------
async def test_invalidate_tag_removes_key_and_tag_set(l2: L2Redis) -> None:
    """Setting with a tag then invalidating that tag removes the key + tag set."""
    await l2.set("q:tagged", {"v": 1}, tags=["source:api"])

    # Sanity: the tag set exists and references the key.
    assert await l2.raw.exists(b"tag:source:api") == 1

    deleted = await l2.invalidate_tag("source:api")
    assert deleted == 1

    assert await l2.get("q:tagged") is None
    # The bookkeeping set must be gone too.
    assert await l2.raw.exists(b"tag:source:api") == 0


# ---------------------------------------------------------------------------
# Graceful degradation — the §5 success criterion: never raise.
# ---------------------------------------------------------------------------
async def test_graceful_degradation_get(l2: L2Redis) -> None:
    """A bad host -> get returns None, degraded flips True, and nothing raises.

    Note: the ``l2`` fixture is requested only so its teardown still runs; the
    degraded client below is built independently against an unreachable host.
    """
    bad = L2Redis("redis://nonexistent-host:6379/0", timeout=0.5)
    await bad.connect()
    try:
        result = await bad.get("x")  # must NOT raise
        assert result is None
        assert bad.degraded is True
        assert bad.errors >= 1
    finally:
        await bad.close()


async def test_graceful_degradation_set(l2: L2Redis) -> None:
    """A bad host -> set returns False and degraded flips True, no exception."""
    bad = L2Redis("redis://nonexistent-host:6379/0", timeout=0.5)
    await bad.connect()
    try:
        ok = await bad.set("x", {"v": 1})  # must NOT raise
        assert ok is False
        assert bad.degraded is True
    finally:
        await bad.close()
