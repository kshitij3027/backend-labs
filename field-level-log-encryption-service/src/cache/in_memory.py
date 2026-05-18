"""In-process dictionary-backed :class:`CacheProvider`.

Used in two contexts:

1. **Unit tests** — the integration test suite that hits real Redis is
   gated behind the ``REDIS_HOST`` env var, so the unit suite always
   uses :class:`InMemoryCache` directly. Stays dependency-free.
2. **Runtime fallback** — :func:`~src.cache.factory.build_cache` returns
   an :class:`InMemoryCache` when Redis is unreachable so the app
   continues serving traffic; the only behavioural change is that
   counter state is lost on restart and not shared across processes.

Implementation notes
--------------------
We keep two separate dicts:

* ``_data`` — ``dict[str, tuple[bytes, datetime | None]]`` for
  :meth:`set` / :meth:`get`. The optional datetime is the absolute
  expiry instant; ``None`` means no TTL.
* ``_counters`` — ``dict[str, int]`` for :meth:`incr` / :meth:`get_counter`.

Why two dicts? Counter values are integers, not bytes. We *could*
str/bytes-encode them and stuff them into ``_data``, but then
``incr("foo")`` followed by ``get("foo")`` would return ``b"1"`` —
caller would have to decode + int() to use it. Splitting the
namespace keeps each accessor's contract clean and avoids the
"decode every time" tax in the hot path. The provider's
:meth:`get_counter` is the symmetric reader.

Thread safety
-------------
A single :class:`threading.Lock` guards both dicts. The hot path
(``incr``) holds the lock for one ``dict.get`` + one ``dict.__setitem__``
which is microseconds — contention is not a concern at our scale
(the lock would have to be held for hundreds of nanoseconds across
thousands of concurrent threads before throughput suffered).
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

from .provider import CacheProvider


class InMemoryCache(CacheProvider):
    """Dict-backed, lock-guarded :class:`CacheProvider` implementation."""

    def __init__(self) -> None:
        # Bytes namespace: value + optional absolute expiry.
        self._data: dict[str, tuple[bytes, datetime | None]] = {}
        # Counter namespace: plain integers.
        self._counters: dict[str, int] = {}
        # Single lock guarding both dicts. Per the module docstring,
        # contention is not a concern at our request rates.
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # CacheProvider implementation
    # ------------------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        """Return the bytes stored at ``key`` or ``None``.

        TTL check is lazy: if a value has expired we discover that on
        access and evict it then. This keeps the cost of ``set`` flat
        (no background reaper thread) at the cost of a slightly larger
        memory footprint for expired-but-untouched keys.
        """
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if expires_at is not None and datetime.now(timezone.utc) >= expires_at:
                # Expired — evict eagerly so subsequent gets are fast.
                del self._data[key]
                return None
            return value

    def set(self, key: str, value: bytes, ttl_sec: int | None = None) -> None:
        """Store ``value`` at ``key`` with optional TTL in seconds.

        We compute the absolute expiry once at set time so subsequent
        ``get`` calls don't have to re-derive it on every access.
        """
        expires_at: datetime | None = None
        if ttl_sec is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_sec)
        with self._lock:
            self._data[key] = (value, expires_at)

    def incr(self, key: str) -> int:
        """Atomically increment the counter at ``key``.

        Counters live in a dedicated ``_counters`` dict (see module
        docstring). The lock guarantees the read-modify-write is atomic
        even if two threads call ``incr`` concurrently — without the
        lock they would race on the ``+= 1`` step.
        """
        with self._lock:
            new_value = self._counters.get(key, 0) + 1
            self._counters[key] = new_value
            return new_value

    def get_counter(self, key: str) -> int:
        """Return the current counter at ``key`` or 0 if missing.

        Never raises — the lookup is unconditional. Useful for the
        ``GET /v1/keys`` aggregation where most key ids have no
        counter yet (e.g. retired keys that have never been queried).
        """
        with self._lock:
            return self._counters.get(key, 0)

    def keys_with_prefix(self, prefix: str) -> list[str]:
        """Return every counter key whose name starts with ``prefix``.

        We snapshot the keys under the lock then filter without it —
        the list() copy is O(n) but cheap, and we don't want to hold
        the lock during the comprehension (which would block ``incr``).
        """
        with self._lock:
            all_keys = list(self._counters.keys())
        return [k for k in all_keys if k.startswith(prefix)]

    def close(self) -> None:
        """No-op for the in-memory backend.

        Defined explicitly (rather than relying on the ABC default,
        which would raise) so callers can use the same shutdown path
        for either backend.
        """
        # Nothing to release. Kept for interface symmetry.
        return None
