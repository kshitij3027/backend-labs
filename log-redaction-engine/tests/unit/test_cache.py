"""Unit tests for the C10 cache backends.

Coverage layout:

* :class:`~src.cache.in_memory.InMemoryBackend` — round-trip, TTL
  expiry, ``incr`` semantics, prefix scan, close as no-op.
* :class:`~src.cache.backend.Backend` Protocol — ``runtime_checkable``
  isinstance check confirms InMemoryBackend conforms to the structural
  contract.

The RedisBackend integration tests live in
``tests/integration/test_redis_backend.py`` and are gated on Redis
being reachable. This unit module never touches the network.
"""
from __future__ import annotations

import time

import pytest

from src.cache.backend import Backend
from src.cache.in_memory import InMemoryBackend


# ---------------------------------------------------------------------------
# InMemoryBackend — get / set
# ---------------------------------------------------------------------------


class TestInMemoryGetSet:
    """Round-trip semantics for the simplest get/set pair."""

    def test_set_then_get_returns_stored_value(self) -> None:
        # Trivial round-trip. The set must persist the value verbatim
        # and the get must return the same string.
        backend = InMemoryBackend()
        backend.set("alpha", "one")
        assert backend.get("alpha") == "one"

    def test_get_unknown_key_returns_none(self) -> None:
        # Symmetric with the Protocol contract: missing key → None,
        # not KeyError. Caller can branch on ``is None`` uniformly.
        backend = InMemoryBackend()
        assert backend.get("never-set") is None

    def test_set_overwrites_existing_value(self) -> None:
        # Re-setting must overwrite — there's no append/list semantics
        # on a Backend. Important for the C10 token store mirror where
        # re-tokenizing the same plaintext re-writes the mapping.
        backend = InMemoryBackend()
        backend.set("k", "first")
        backend.set("k", "second")
        assert backend.get("k") == "second"


# ---------------------------------------------------------------------------
# InMemoryBackend — TTL expiry
# ---------------------------------------------------------------------------


class TestInMemoryTtl:
    """Lazy expiry-on-access semantics."""

    def test_set_with_ttl_expires_after_window(self) -> None:
        # TTL=1 second. After sleeping past the window, the key reads
        # as None and the eviction must have happened during the get
        # (we don't expose a way to verify that directly, but the
        # ``keys()`` sweep in the next test class corroborates it).
        backend = InMemoryBackend()
        backend.set("ephemeral", "value", ttl_sec=1)
        # The value is present before the TTL elapses.
        assert backend.get("ephemeral") == "value"
        # Sleep long enough that ``time.time() > expiry`` is true. A
        # small margin (1.1 s) covers clock granularity on slow CI.
        time.sleep(1.1)
        assert backend.get("ephemeral") is None

    def test_set_without_ttl_persists(self) -> None:
        # Default ttl_sec=None means no expiry. The key must remain
        # readable across a short sleep.
        backend = InMemoryBackend()
        backend.set("permanent", "forever")
        time.sleep(0.1)
        assert backend.get("permanent") == "forever"

    def test_resetting_without_ttl_clears_prior_ttl(self) -> None:
        # Mirrors the Redis SET-clears-TTL contract: re-setting a
        # previously TTL'd key without a TTL clears the expiry rather
        # than silently inheriting the old one.
        backend = InMemoryBackend()
        backend.set("k", "ttl'd", ttl_sec=1)
        # Re-set with no TTL — the prior expiry should be discarded.
        backend.set("k", "permanent")
        time.sleep(1.1)
        # If the TTL hadn't been cleared, this would return None.
        assert backend.get("k") == "permanent"


# ---------------------------------------------------------------------------
# InMemoryBackend — incr
# ---------------------------------------------------------------------------


class TestInMemoryIncr:
    """Counter semantics: auto-create at 0, monotonic increment."""

    def test_incr_starts_at_one(self) -> None:
        # First incr of a missing key returns 1 (auto-create at 0
        # then +1). Subsequent incrs return 2, 3, ...
        backend = InMemoryBackend()
        assert backend.incr("hits") == 1
        assert backend.incr("hits") == 2
        assert backend.incr("hits") == 3

    def test_incr_independent_counters(self) -> None:
        # Two different keys must maintain separate counters — incring
        # one must not bleed into the other.
        backend = InMemoryBackend()
        backend.incr("a")
        backend.incr("a")
        backend.incr("b")
        # ``get`` returns the stringified counter (the backend stores
        # counters in the same dict as bytes values).
        assert backend.get("a") == "2"
        assert backend.get("b") == "1"


# ---------------------------------------------------------------------------
# InMemoryBackend — keys prefix scan
# ---------------------------------------------------------------------------


class TestInMemoryKeys:
    """Prefix-based key enumeration."""

    def test_keys_returns_only_prefix_matches(self) -> None:
        # Seed two namespaces and confirm the prefix filter is exact.
        backend = InMemoryBackend()
        backend.set("foo:a", "1")
        backend.set("foo:b", "2")
        backend.set("bar:c", "3")
        found = sorted(backend.keys(prefix="foo:"))
        assert found == ["foo:a", "foo:b"]

    def test_keys_empty_prefix_returns_all(self) -> None:
        # ``prefix=""`` is the documented "all keys" sentinel — every
        # string starts with the empty string, so the filter is a
        # no-op and we get the full key set.
        backend = InMemoryBackend()
        backend.set("a", "1")
        backend.set("b", "2")
        backend.set("c", "3")
        assert sorted(backend.keys()) == ["a", "b", "c"]

    def test_keys_excludes_expired(self) -> None:
        # ``keys()`` sweeps expired entries before returning so callers
        # never see a key that ``get`` would refuse. We seed one
        # short-TTL key and one permanent key, wait past the TTL, and
        # confirm only the permanent one survives the scan.
        backend = InMemoryBackend()
        backend.set("short", "x", ttl_sec=1)
        backend.set("long", "y")
        time.sleep(1.1)
        assert backend.keys() == ["long"]


# ---------------------------------------------------------------------------
# InMemoryBackend — close
# ---------------------------------------------------------------------------


class TestInMemoryClose:
    """Lifecycle methods on the in-memory backend."""

    def test_close_is_noop(self) -> None:
        # close() must work without raising and must not invalidate
        # the underlying dicts. Calling it twice (idempotency) must
        # also succeed — the lifespan's shutdown path relies on this.
        backend = InMemoryBackend()
        backend.set("a", "1")
        backend.close()
        backend.close()
        # The backend remains functional after close() — the in-memory
        # implementation has no real resources to release.
        assert backend.get("a") == "1"


# ---------------------------------------------------------------------------
# Backend Protocol — runtime conformance
# ---------------------------------------------------------------------------


class TestBackendProtocol:
    """Verify the Protocol's ``runtime_checkable`` contract."""

    def test_in_memory_backend_isinstance_backend(self) -> None:
        # ``@runtime_checkable`` lets us assert structural conformance
        # at runtime. The InMemoryBackend has all five required
        # methods plus the ``name`` attribute, so ``isinstance`` must
        # return True. This catches accidental signature changes on
        # InMemoryBackend that would break the Protocol contract.
        backend = InMemoryBackend()
        assert isinstance(backend, Backend)

    def test_in_memory_backend_exposes_name_attribute(self) -> None:
        # The Protocol declares ``name: str`` as an attribute. Verify
        # the concrete class exposes it with the documented value so
        # the lifespan's log line ("backend=in_memory") stays stable.
        assert InMemoryBackend.name == "in_memory"
        assert InMemoryBackend().name == "in_memory"

    def test_non_backend_object_not_isinstance(self) -> None:
        # Sanity check the negative case: an arbitrary object that
        # doesn't implement the Protocol must NOT pass isinstance.
        # Without this we couldn't trust the positive test above.
        class NotABackend:
            pass

        assert not isinstance(NotABackend(), Backend)


# ---------------------------------------------------------------------------
# Module-level pytest discovery sanity
# ---------------------------------------------------------------------------


def test_module_imports_cleanly() -> None:
    # If this test file imports without error, every transitive
    # symbol used above (Backend, InMemoryBackend) is reachable. A
    # smoke check that costs nothing and catches packaging regressions.
    assert Backend is not None
    assert InMemoryBackend is not None
