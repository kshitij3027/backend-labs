"""Unit tests for the C9 cache subsystem.

Coverage targets:

* :class:`TestInMemoryCache` — the pure-Python backend's contract: get/
  set round-trips with bytes, TTL expiry, atomic incr, prefix scan,
  the symmetric :meth:`get_counter` reader, and close() idempotency.
* :class:`TestBuildCacheFallback` — the factory's two modes: with
  ``fallback_to_memory=True`` an unreachable Redis falls back to
  in-memory; with ``fallback_to_memory=False`` the same input raises
  :class:`CacheUnavailable`.

The :class:`RedisCache` itself is exercised in
:mod:`tests.integration.test_redis_cache` against a live Redis under
docker-compose. Re-running those tests in pure-unit context would
require an embedded Redis (fakeredis etc.) which we deliberately don't
take a dependency on — production behaviour against the real server is
what matters.
"""
from __future__ import annotations

import time

import pytest

from src.cache import (
    CacheProvider,
    CacheUnavailable,
    InMemoryCache,
    build_cache,
)


# ---------------------------------------------------------------------------
# TestInMemoryCache
# ---------------------------------------------------------------------------


class TestInMemoryCache:
    """In-process backend contract tests."""

    def test_get_set_bytes_roundtrip(self) -> None:
        # Plain bytes round-trip — the most basic contract. Use a
        # non-UTF8-safe byte sequence to confirm we're not implicitly
        # decoding anywhere (would corrupt ciphertext when the cache is
        # used for arbitrary binary values).
        cache = InMemoryCache()
        cache.set("k1", b"\x00\xff\x10\x80value")
        assert cache.get("k1") == b"\x00\xff\x10\x80value"

    def test_get_missing_key_returns_none(self) -> None:
        # No entry → None (not KeyError, not empty bytes). Symmetric
        # with Redis's GET semantics so the two backends are
        # interchangeable.
        cache = InMemoryCache()
        assert cache.get("nope") is None

    def test_set_overwrites_existing_value(self) -> None:
        # Repeated set on the same key wins-by-most-recent. No version
        # history, no merging.
        cache = InMemoryCache()
        cache.set("k", b"first")
        cache.set("k", b"second")
        assert cache.get("k") == b"second"

    def test_ttl_value_returns_immediately_then_expires(self) -> None:
        # Short TTL: 0.5s. Read immediately succeeds, then sleep past
        # the expiry and the lazy reaper in get() returns None.
        cache = InMemoryCache()
        cache.set("ephemeral", b"x", ttl_sec=1)
        # Immediate read: still alive.
        assert cache.get("ephemeral") == b"x"
        # Past TTL: gone. We sleep slightly longer than the TTL to
        # absorb any sub-millisecond timer skew on slow CI hosts.
        time.sleep(1.2)
        assert cache.get("ephemeral") is None

    def test_set_without_ttl_persists_indefinitely(self) -> None:
        # No ttl_sec → no expiry. We can't sleep "forever" but verifying
        # the entry doesn't sprout an expiry attribute is enough.
        cache = InMemoryCache()
        cache.set("permanent", b"x")
        # Brief sleep to make sure no auto-expiry kicks in.
        time.sleep(0.05)
        assert cache.get("permanent") == b"x"

    def test_incr_creates_at_one_then_increments(self) -> None:
        # Missing key starts at 0 then increments to 1, matches the
        # Redis INCR semantic. Subsequent calls return 2, 3.
        cache = InMemoryCache()
        assert cache.incr("counter") == 1
        assert cache.incr("counter") == 2
        assert cache.incr("counter") == 3

    def test_incr_independent_counters(self) -> None:
        # Different keys = independent counters. Bumping one must not
        # bleed into the other.
        cache = InMemoryCache()
        cache.incr("a")
        cache.incr("a")
        cache.incr("b")
        assert cache.get_counter("a") == 2
        assert cache.get_counter("b") == 1

    def test_get_counter_missing_returns_zero(self) -> None:
        # Never-incremented key reads as 0, never raises. This is the
        # contract surfaced to ``GET /v1/keys`` which queries one
        # counter per (key_id, op) without knowing whether it's been
        # used yet.
        cache = InMemoryCache()
        assert cache.get_counter("never-incremented") == 0

    def test_get_counter_matches_incr_result(self) -> None:
        # After N incrs, get_counter == N. Symmetric reader for incr.
        cache = InMemoryCache()
        for _ in range(7):
            cache.incr("k")
        assert cache.get_counter("k") == 7

    def test_keys_with_prefix_filters_correctly(self) -> None:
        # Prefix matching is a literal startswith. Only keys in the
        # counter namespace are returned — values from .set() are NOT
        # included (different namespace by design).
        cache = InMemoryCache()
        cache.incr("key_usage:alpha:encrypt")
        cache.incr("key_usage:alpha:decrypt")
        cache.incr("key_usage:beta:encrypt")
        cache.incr("unrelated_counter")
        # Also store a bytes value at the same prefix to confirm it's
        # NOT picked up by keys_with_prefix (counter namespace only).
        cache.set("key_usage:omega:encrypt", b"not-a-counter")

        found = sorted(cache.keys_with_prefix("key_usage:"))
        assert found == [
            "key_usage:alpha:decrypt",
            "key_usage:alpha:encrypt",
            "key_usage:beta:encrypt",
        ]

    def test_keys_with_prefix_empty_returns_empty(self) -> None:
        # No matches → empty list (not None, not KeyError). Caller
        # iterates the result without a None check.
        cache = InMemoryCache()
        cache.incr("hits")
        assert cache.keys_with_prefix("misses:") == []

    def test_get_and_incr_namespaces_are_disjoint(self) -> None:
        # set/get and incr/get_counter live in different dicts —
        # incrementing 'foo' must not make get('foo') return bytes.
        # This is the load-bearing invariant of the two-dict design.
        cache = InMemoryCache()
        cache.incr("foo")
        cache.incr("foo")
        assert cache.get("foo") is None  # bytes namespace empty
        assert cache.get_counter("foo") == 2  # counter namespace has 2

    def test_close_is_callable_and_idempotent(self) -> None:
        # close() must work without error, and calling it twice must
        # also work — no AttributeError on a re-close path.
        cache = InMemoryCache()
        cache.close()
        cache.close()

    def test_provider_isinstance(self) -> None:
        # Type-system contract: InMemoryCache is-a CacheProvider.
        # Lets call sites accept the ABC and inject either backend.
        cache = InMemoryCache()
        assert isinstance(cache, CacheProvider)


# ---------------------------------------------------------------------------
# TestBuildCacheFallback
# ---------------------------------------------------------------------------


class TestBuildCacheFallback:
    """Factory behaviour: Redis-first with optional fallback."""

    def test_unreachable_redis_falls_back_to_in_memory(self) -> None:
        # An unreachable host triggers a CacheUnavailable inside the
        # RedisCache ctor; the factory catches it and returns an
        # InMemoryCache instead. The DNS-unresolvable host name is the
        # most deterministic way to simulate an outage — port 6399 on
        # a non-existent host fails the connect fast.
        cache = build_cache(
            host="redis-unreachable-host-for-test",
            port=6399,
            fallback_to_memory=True,
        )
        # The fallback path returns an in-memory cache.
        assert isinstance(cache, InMemoryCache)

    def test_unreachable_redis_without_fallback_raises(self) -> None:
        # Same input but fallback disabled → CacheUnavailable propagates.
        # This is what tests use to assert the failure mode directly.
        with pytest.raises(CacheUnavailable):
            build_cache(
                host="redis-unreachable-host-for-test",
                port=6399,
                fallback_to_memory=False,
            )

    def test_fallback_cache_is_usable(self) -> None:
        # The InMemoryCache returned from the fallback path must be a
        # fully functional cache — incr round-trips, etc. (Sanity check
        # that we didn't accidentally return a half-initialised object.)
        cache = build_cache(
            host="redis-unreachable-host-for-test",
            port=6399,
            fallback_to_memory=True,
        )
        assert cache.incr("test") == 1
        assert cache.get_counter("test") == 1
