"""Cache abstraction — the :class:`CacheProvider` ABC.

The interface is deliberately small. We only support the operations the
service actually needs in C9:

* ``get(key)`` / ``set(key, value, ttl_sec=None)`` — generic bytes
  key/value access.
* ``incr(key)`` — atomic counter increment, the load-bearing operation
  for the per-key-id usage tracking in
  :class:`~src.processor.log_processor.LogProcessor`.
* ``keys_with_prefix(prefix)`` — enumerate keys with a given prefix.
  Used by ``GET /v1/keys`` to discover all per-key counters without
  having to remember every key id we've ever seen.
* ``get_counter(key)`` — convenience reader for counter values. Kept
  separate from :meth:`get` because the in-memory backend stores
  counters in a dedicated ``int`` dict (not the bytes dict), so the
  symmetric ``get`` would return ``None`` for an incremented key —
  ``get_counter`` papers over that asymmetry uniformly across backends.
* ``close()`` — release any open resources (Redis socket, etc.).

We do NOT support:

* Pub/sub — out of scope; we'd reach for a dedicated broker.
* Distributed locks — same reasoning.
* Bulk get/set — premature; the call sites are all single-key today.

Each method docstring documents the contract; concrete backends in
:mod:`.in_memory` and :mod:`.redis_cache` must honour it.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class CacheUnavailable(Exception):
    """Raised when a networked cache backend is unreachable.

    Today only :class:`~src.cache.redis_cache.RedisCache` raises this —
    its constructor does a ping with a short timeout and re-raises the
    underlying ``redis.ConnectionError`` as :class:`CacheUnavailable`.
    The :func:`~src.cache.factory.build_cache` helper catches it and
    falls back to :class:`~src.cache.in_memory.InMemoryCache` so the
    rest of the app stays oblivious to backend health.

    Kept as a generic exception rather than a redis-specific one so a
    future Memcached / Hazelcast backend can use the same fallback path
    without leaking implementation detail.
    """


class CacheProvider(ABC):
    """Abstract base class for cache backends.

    All methods are synchronous. The Redis client we use
    (``redis-py >= 5``) is blocking by default; the
    :class:`~src.processor.log_processor.LogProcessor` calls cache
    methods from inside its already-running encrypt/decrypt path which
    is itself driven by ``ThreadPoolExecutor`` workers, so blocking on
    a network round-trip there is acceptable. If we ever push cache
    calls onto the request thread we'll wrap them in
    ``asyncio.to_thread`` rather than making the interface async — the
    cost of an async interface (every callable becomes a coroutine) is
    higher than the cost of one ``to_thread`` per cache hit.
    """

    @abstractmethod
    def get(self, key: str) -> bytes | None:
        """Return the value previously stored at ``key``, or ``None``.

        TTL-aware: if the value was set with a ``ttl_sec`` and the TTL
        has elapsed, ``get`` returns ``None`` (the backend is free to
        evict lazily or eagerly).

        Important asymmetry with :meth:`incr`: the in-memory backend
        stores counters in a separate dict, so ``get("key_usage:foo")``
        returns ``None`` even after several ``incr`` calls. Use
        :meth:`get_counter` instead.
        """

    @abstractmethod
    def set(self, key: str, value: bytes, ttl_sec: int | None = None) -> None:
        """Store ``value`` at ``key``.

        ``ttl_sec=None`` means no expiry. Overwrites any existing value.
        ``value`` is bytes — callers are responsible for serialisation
        (we never JSON-encode for the caller; that would lock in a
        format that some future use case might not want).
        """

    @abstractmethod
    def incr(self, key: str) -> int:
        """Atomically increment the integer counter at ``key`` by 1.

        If the key is missing, treat it as 0 and increment to 1.
        Returns the new value. The atomicity guarantee matters: two
        encrypt threads incrementing the same counter must never see
        the same intermediate value.

        Implementations must NOT store the result in the same key
        space as :meth:`set` — that would let a malformed counter
        (non-integer bytes) corrupt the bytes store. The in-memory
        backend uses a dedicated ``_counters`` dict; Redis uses its
        native ``INCR`` which has the same semantics.
        """

    @abstractmethod
    def get_counter(self, key: str) -> int:
        """Return the current counter value at ``key``, or 0 if missing.

        Symmetric reader for :meth:`incr`. Never raises on a missing
        key — operators querying ``GET /v1/keys`` shouldn't have to
        handle "no counter yet" specially.
        """

    @abstractmethod
    def keys_with_prefix(self, prefix: str) -> list[str]:
        """Return every counter key whose name starts with ``prefix``.

        Order is unspecified — Redis ``SCAN`` returns keys in
        scan-cursor order; the in-memory backend returns dict insertion
        order. Callers that need ordering must sort the result.

        Searches ONLY the counter namespace (the ``incr`` keys), not
        the bytes namespace (the ``set`` keys). This matches the C9
        usage pattern in ``GET /v1/keys`` — we only ever want to
        discover usage counters this way.
        """

    @abstractmethod
    def close(self) -> None:
        """Release any open resources (sockets, threads).

        Idempotent — calling ``close`` twice must not raise. The
        in-memory backend is a no-op; Redis closes its connection pool.
        """
