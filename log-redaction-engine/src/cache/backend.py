"""Cache backend Protocol shared by ``InMemoryBackend`` and ``RedisBackend``.

The :class:`Backend` Protocol is the minimal surface area that the rest of
the application depends on for cache-like storage. By keeping the interface
small and Protocol-based, we get:

* **Structural typing** — any object with the right shape satisfies the
  contract; no inheritance required. Useful for tests that want to swap
  in a stub without subclassing.
* **Runtime checkability** — ``@runtime_checkable`` lets us write
  ``isinstance(obj, Backend)`` for the rare branches that need to
  verify a backend was actually passed in (the C10 lifespan asserts
  this once at startup).

The five operations cover the two cross-process workloads C10 cares about:

* ``get`` / ``set`` / ``keys`` — the token store mirror (plaintext_hash →
  token and token → plaintext, no TTL — tokens persist for the deployment
  lifetime).
* ``incr`` — the pattern-counter mirror (per-pattern hit counts for
  cross-process aggregation).
* ``close`` — graceful shutdown hook.

What we deliberately leave out
------------------------------
* Pub/sub, transactions, distributed locks — out of scope; we'd reach
  for a dedicated tool.
* Bulk get/set — premature optimisation; every C10 call site is single-key.
* Async — the redis-py client is blocking and our lifespan runs cache
  setup once at startup. If we ever push cache calls onto a request
  thread we'll wrap them in ``asyncio.to_thread`` rather than making
  the entire interface async.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Backend(Protocol):
    """Protocol describing a key/value cache backend.

    All values are str — the higher-level code (TokenStore, PatternCounters)
    only needs to store strings and integers (the latter is read back as
    a str then ``int()``-ed). Keeping the type uniform avoids a bytes-vs-str
    asymmetry between the two concrete implementations.

    Attributes
    ----------
    name : str
        Short identifier (``"in_memory"`` or ``"redis"``) used in startup
        logs so an operator can tell which backend the app picked.
    """

    name: str

    def get(self, key: str) -> str | None:
        """Return the value previously stored at ``key``, or ``None``.

        TTL-aware: if the value was set with a ``ttl_sec`` and the TTL
        has elapsed, ``get`` returns ``None``. Backends are free to
        evict lazily (in-memory) or eagerly (Redis); the caller cannot
        distinguish.
        """
        ...

    def set(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        """Store ``value`` at ``key``.

        ``ttl_sec=None`` means no expiry. Overwrites any existing value.
        """
        ...

    def incr(self, key: str) -> int:
        """Atomically increment the integer counter at ``key`` by 1.

        If the key is missing, treat it as 0 and increment to 1.
        Returns the new value. The atomicity guarantee matters: two
        threads incrementing the same counter must never see the same
        intermediate value.
        """
        ...

    def keys(self, prefix: str = "") -> list[str]:
        """Return every key whose name starts with ``prefix``.

        ``prefix=""`` returns every key. Order is unspecified — Redis
        ``SCAN`` returns keys in scan-cursor order; the in-memory backend
        returns dict insertion order. Callers that need ordering must
        sort the result.
        """
        ...

    def close(self) -> None:
        """Release any open resources (sockets, threads).

        Idempotent — calling ``close`` twice must not raise. The
        in-memory backend is a no-op; Redis closes its connection pool.
        """
        ...
