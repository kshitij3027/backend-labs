"""Integration tests for the :class:`RedisBackend` cache backend.

These tests require a live Redis. They are gated at module level by an
explicit ping attempt — if Redis is not reachable from the test runner
(unit suite on a dev laptop with no Redis, for example), the whole
module is skipped cleanly.

Under the docker-compose ``test`` profile, the ``redis`` service is
brought up first via ``depends_on: service_healthy`` and the tester
container talks to it on hostname ``redis`` (compose's service-name DNS).

Why integration not unit?
-------------------------
We deliberately do NOT use fakeredis or an embedded server. The
production deployment will talk to a real Redis, and the C10 contract
includes ``SCAN_ITER`` cursor pagination which fakeredis emulations
have historically had bugs around. Testing against the real server is
the only way to be sure.

Isolation
---------
Each test generates a unique key prefix so two parallel test runs
against the same Redis don't collide. We don't TTL the keys (the
backend's contract is "no TTL by default") so the test sweeps its
prefix at fixture teardown.
"""
from __future__ import annotations

import os
import uuid

import pytest
import redis

from src.cache.redis_backend import RedisBackend


# ---------------------------------------------------------------------------
# Module-level skip: bail out cleanly if Redis isn't reachable from here.
# ---------------------------------------------------------------------------
#
# We do an actual ``ping`` (not just a check on the env var) because the
# compose tester container always has the env var but a dev laptop might
# set REDIS_HOST and still have no Redis running. The ping is the truthful
# signal — if it succeeds, Redis is live; if it fails, we skip without
# misleading the operator about why.

_REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
_REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def _probe_redis() -> bool:
    """Return True iff Redis is reachable at the configured host:port.

    Short ``socket_connect_timeout`` so the skip decision happens
    quickly — without it, a misconfigured env would block the test
    runner for 60 s on every invocation.
    """
    try:
        client = redis.Redis(
            host=_REDIS_HOST,
            port=_REDIS_PORT,
            socket_connect_timeout=1,
            decode_responses=True,
        )
        client.ping()
        client.close()
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _probe_redis(),
        reason=f"Redis not reachable at {_REDIS_HOST}:{_REDIS_PORT}; skipping",
    ),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def backend() -> RedisBackend:
    """Construct a fresh :class:`RedisBackend` per test.

    A per-test instance means a ``close()`` in one test can't affect
    another's connection state. The compose ``redis`` service is
    healthchecked so by the time pytest runs we know it's up.
    """
    rb = RedisBackend(host=_REDIS_HOST, port=_REDIS_PORT)
    yield rb
    # Best-effort cleanup — close() is idempotent in redis-py.
    try:
        rb.close()
    except Exception:
        pass


@pytest.fixture
def test_prefix() -> str:
    """Unique key prefix per test to keep parallel runs isolated.

    Using a uuid4 hex means we don't have to clean up between every
    test — each one writes into a fresh namespace. The test for
    ``keys()`` does its own targeted cleanup to keep the test reliable
    on a long-lived Redis.
    """
    return f"test_c10:{uuid.uuid4().hex[:8]}:"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRedisBackend:
    """Live-server contract tests for :class:`RedisBackend`."""

    def test_set_then_get_roundtrip(
        self, backend: RedisBackend, test_prefix: str
    ) -> None:
        # String round-trip — confirms decode_responses=True is honoured
        # and the str values survive the wire as str (not bytes).
        key = f"{test_prefix}roundtrip"
        backend.set(key, "hello-world")
        assert backend.get(key) == "hello-world"

    def test_get_missing_returns_none(
        self, backend: RedisBackend, test_prefix: str
    ) -> None:
        # Symmetric with the InMemoryBackend contract: missing → None,
        # not KeyError. Important so call sites can branch on `is None`
        # without backend-specific catch blocks.
        assert backend.get(f"{test_prefix}does-not-exist") is None

    def test_incr_starts_at_one_then_two(
        self, backend: RedisBackend, test_prefix: str
    ) -> None:
        # Redis INCR auto-creates at 0 and returns the new value, so
        # the first call returns 1 and subsequent calls return 2, 3.
        key = f"{test_prefix}counter"
        assert backend.incr(key) == 1
        assert backend.incr(key) == 2
        assert backend.incr(key) == 3

    def test_keys_returns_prefix_matches_only(
        self, backend: RedisBackend, test_prefix: str
    ) -> None:
        # Seed three keys under the test prefix and one under an
        # unrelated prefix; only the matching three should come back.
        # The unrelated key is sentinel-named so we can clean it up.
        backend.set(f"{test_prefix}a", "1")
        backend.set(f"{test_prefix}b", "2")
        backend.set(f"{test_prefix}c", "3")
        sentinel = f"unrelated:{uuid.uuid4().hex[:8]}"
        backend.set(sentinel, "x")

        try:
            found = sorted(backend.keys(prefix=test_prefix))
            # Exactly the three keys we seeded under our prefix.
            assert len(found) == 3
            # All matching the prefix.
            for k in found:
                assert k.startswith(test_prefix)
            # The unrelated key is NOT in the result set.
            assert sentinel not in found
        finally:
            # Targeted cleanup of the sentinel key so a long-lived Redis
            # doesn't accumulate stale entries across test runs.
            backend._client.delete(sentinel)  # type: ignore[attr-defined]

    def test_set_with_ttl_applies_expiry(
        self, backend: RedisBackend, test_prefix: str
    ) -> None:
        # Set a 60-second TTL via the ttl_sec= argument; confirm Redis
        # reports a positive remaining TTL via its TTL command. We
        # don't sleep here — that'd make the test slow — instead we
        # assert the TTL was actually applied.
        key = f"{test_prefix}ttl"
        backend.set(key, "transient", ttl_sec=60)
        # The value is still there.
        assert backend.get(key) == "transient"
        # Verify TTL was applied via the raw client. -1 means no TTL,
        # -2 means key missing; a positive value means a TTL is set.
        ttl_remaining = backend._client.ttl(key)  # type: ignore[attr-defined]
        assert ttl_remaining > 0

    def test_close_is_idempotent(
        self, backend: RedisBackend, test_prefix: str
    ) -> None:
        # close() must work without raising. We call it explicitly here
        # AND it's called again by the fixture teardown; the redis-py
        # close() is idempotent so the double-call must not blow up.
        backend.close()
        # A second close() through the fixture's finally block also
        # exercises the idempotency contract; this test just proves a
        # direct double-close in user code works too.
        backend.close()
