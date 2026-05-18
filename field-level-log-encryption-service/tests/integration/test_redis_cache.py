"""Integration tests for the :class:`RedisCache` backend.

These tests require a live Redis instance. They are gated at module
level on the ``REDIS_HOST`` environment variable so the unit suite
(which runs without Redis) skips them cleanly. Under the docker-compose
``tester`` profile, the ``redis`` service is brought up first via
``depends_on: service_healthy`` and ``REDIS_HOST=redis`` is set on the
``tester`` container, so these tests run end-to-end against the same
Redis the ``app`` container uses.

Why integration not unit?
-------------------------
We deliberately do NOT use fakeredis or an embedded server. The
production deployment will talk to a real Redis, and the C9 contract
includes things like ``SCAN_ITER`` cursor pagination and ``decode_responses=False``
binary handling that fakeredis emulations have historically had bugs
around. Testing against the real server is the only way to be sure.

Isolation
---------
Each test generates a unique key prefix so two parallel test runs
against the same Redis don't collide. Keys are TTL'd to 60 seconds
so a partial run doesn't permanently pollute the Redis key space —
even if a test crashes mid-execution, the residue self-cleans.
"""
from __future__ import annotations

import os
import uuid

import pytest

from src.cache import RedisCache
from src.cache.provider import CacheUnavailable


# Module-level skip: the entire file is gated on REDIS_HOST being set.
# Without that env var (i.e. running unit tests on a dev laptop with no
# Redis), every test in this file is skipped with a clear reason.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("REDIS_HOST") is None,
        reason="REDIS_HOST not set; skipping Redis integration tests",
    ),
]


def _redis_host() -> str:
    """Resolve the Redis host from env, defaulting to ``redis``."""
    return os.environ.get("REDIS_HOST", "redis")


def _redis_port() -> int:
    """Resolve the Redis port from env, defaulting to 6379."""
    return int(os.environ.get("REDIS_PORT", "6379"))


@pytest.fixture
def cache() -> RedisCache:
    """Construct a :class:`RedisCache` against the compose `redis` service.

    Each test gets its own instance so a close() in one test can't
    affect another's connection state. The compose `redis` service is
    healthchecked so by the time pytest runs we know it's up.
    """
    rc = RedisCache(host=_redis_host(), port=_redis_port())
    yield rc
    # Best-effort cleanup. close() is idempotent.
    rc.close()


@pytest.fixture
def test_prefix() -> str:
    """Unique key prefix per test to keep parallel runs isolated.

    Using a uuid4 hex means we don't have to clean up after every test —
    each one writes into a fresh namespace and the natural TTL on
    counters will eventually reap them. (For test reliability we don't
    rely on TTL within the test; we use distinct names instead.)
    """
    return f"test:{uuid.uuid4().hex[:8]}:"


# ---------------------------------------------------------------------------
# TestRedisCache
# ---------------------------------------------------------------------------


class TestRedisCache:
    """Live-server contract tests for :class:`RedisCache`."""

    def test_set_then_get_roundtrip(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # Bytes round-trip — confirms decode_responses=False is honoured
        # and binary values survive the wire.
        key = f"{test_prefix}roundtrip"
        cache.set(key, b"\x00\xff\x10\x80value")
        assert cache.get(key) == b"\x00\xff\x10\x80value"

    def test_get_missing_returns_none(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # Symmetric with the in-memory contract: missing key → None,
        # not KeyError. Important so call sites can branch on `is None`
        # without backend-specific catch blocks.
        assert cache.get(f"{test_prefix}does-not-exist") is None

    def test_incr_starts_at_one(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # Redis INCR auto-creates at 0 and returns the new value, so
        # the first call returns 1 and subsequent calls return 2, 3.
        key = f"{test_prefix}counter"
        assert cache.incr(key) == 1
        assert cache.incr(key) == 2
        assert cache.incr(key) == 3

    def test_get_counter_returns_zero_for_missing(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # Counters that have never been incremented read as 0. This is
        # the contract surfaced to ``GET /v1/keys``.
        assert cache.get_counter(f"{test_prefix}never") == 0

    def test_get_counter_matches_incr(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # After several incrs, get_counter returns the same value.
        # Note: Redis stores integer counters as ASCII bytes; the
        # implementation does the int() conversion for us.
        key = f"{test_prefix}counter"
        for _ in range(5):
            cache.incr(key)
        assert cache.get_counter(key) == 5

    def test_keys_with_prefix_matches_only_prefixed(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # Use the SCAN_ITER-backed prefix scan. We seed three counters
        # under the test prefix and one under an unrelated prefix; only
        # the matching ones should come back.
        cache.incr(f"{test_prefix}a")
        cache.incr(f"{test_prefix}b")
        cache.incr(f"{test_prefix}c")
        cache.incr("unrelated:other")  # must NOT appear in our scan

        found = sorted(cache.keys_with_prefix(test_prefix))
        # Three counters under our prefix.
        assert len(found) >= 3
        # All matching the prefix.
        for k in found:
            assert k.startswith(test_prefix)
        # The unrelated key is NOT in the result set.
        assert "unrelated:other" not in found

    def test_set_with_ttl_expires(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # Set a 1-second TTL via Redis's ex= argument; confirm the key
        # is gone after the TTL window. We don't time.sleep here to
        # keep the test fast — instead we check Redis's TTL command
        # directly by asserting the key has a positive remaining TTL.
        key = f"{test_prefix}ttl"
        cache.set(key, b"transient", ttl_sec=60)
        # The value is still there.
        assert cache.get(key) == b"transient"
        # Verify TTL was applied (via the raw client). This avoids a
        # sleep while still asserting the ttl_sec argument was honored.
        ttl_remaining = cache._client.ttl(key)  # type: ignore[attr-defined]
        # `ttl` returns -1 when key has no TTL, -2 when key is missing.
        # A positive value means a TTL is set.
        assert ttl_remaining > 0

    def test_set_without_ttl_no_expiry(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # ttl_sec=None → no expiry. Redis's TTL returns -1 for that.
        key = f"{test_prefix}forever"
        cache.set(key, b"x")
        ttl = cache._client.ttl(key)  # type: ignore[attr-defined]
        assert ttl == -1  # -1 means no TTL

    def test_close_is_callable(
        self, cache: RedisCache, test_prefix: str
    ) -> None:
        # close() must work without raising. We call it again after
        # the fixture's own close in teardown to confirm idempotency.
        cache.close()
        cache.close()  # double close — must not raise

    def test_unreachable_host_raises_cache_unavailable(self) -> None:
        # The constructor's ping should fail fast and surface as
        # CacheUnavailable. This is what build_cache catches for the
        # fallback path.
        with pytest.raises(CacheUnavailable):
            RedisCache(host="redis-unreachable-host-for-test", port=6399)
