"""L1 in-process cache tier.

A thin, thread-safe wrapper around :class:`cachetools.TTLCache` that provides
the operations the cache manager (C11) needs: LRU+TTL semantics, hit/miss
accounting, pattern-style key deletion, near-expiry detection (for the proactive
warmer in C14), and an approximate memory footprint for the metrics/dashboard.

Design notes
------------
* ``cachetools`` is *not* thread-safe on its own. Every public method takes a
  :class:`threading.RLock` so concurrent FastAPI request handlers (running in
  the default threadpool / multiple event-loop tasks touching shared state)
  cannot corrupt the underlying dict or the parallel bookkeeping structures.
* The ``timer`` is injectable. ``TTLCache`` evaluates entry expiry against the
  value returned by ``timer()``, so tests pass a deterministic fake clock and
  advance it manually — no ``time.sleep`` required. Production uses
  ``time.monotonic`` so wall-clock jumps don't affect TTLs.
* We keep a parallel ``self._set_times`` map (key -> insertion time) so we can
  compute each entry's *age* for :meth:`near_expiry_keys`. ``TTLCache`` tracks
  expiry internally but does not expose per-key age, hence the side table.
"""
from __future__ import annotations

import fnmatch
import json
import threading
import time
from typing import Any, Callable

import cachetools


class L1Cache:
    """Thread-safe, in-process LRU + TTL cache with per-tier statistics."""

    def __init__(
        self,
        max_size: int,
        ttl: float,
        *,
        timer: Callable[[], float] = time.monotonic,
    ) -> None:
        self._cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=max_size, ttl=ttl, timer=timer
        )
        self._lock = threading.RLock()
        # Parallel insertion-time table used purely for near-expiry detection;
        # kept in lock-step with ``self._cache`` on every mutation.
        self._set_times: dict[str, float] = {}
        self._ttl: float = ttl
        self._timer: Callable[[], float] = timer
        self.hits: int = 0
        self.misses: int = 0

    # -- core operations -------------------------------------------------

    def get(self, key: str) -> Any | None:
        """Return the cached value or ``None``.

        Accessing ``self._cache[key]`` drives ``TTLCache`` expiry, so an entry
        whose TTL has elapsed (per the injected ``timer``) raises ``KeyError``
        and is counted as a miss.
        """
        with self._lock:
            try:
                value = self._cache[key]
            except KeyError:
                self.misses += 1
                return None
            self.hits += 1
            return value

    def set(self, key: str, value: Any) -> None:
        """Insert/overwrite ``key`` and record its insertion time."""
        with self._lock:
            self._cache[key] = value
            self._set_times[key] = self._timer()

    def delete(self, key: str) -> bool:
        """Remove ``key`` if present; return whether it existed."""
        with self._lock:
            existed = key in self._cache
            self._cache.pop(key, None)
            self._set_times.pop(key, None)
            return existed

    def clear(self) -> None:
        """Drop all entries and reset hit/miss counters."""
        with self._lock:
            self._cache.clear()
            self._set_times.clear()
            self.hits = 0
            self.misses = 0

    # -- dunder helpers --------------------------------------------------

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._cache

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    # -- bulk / pattern operations --------------------------------------

    def scan_delete(self, pattern: str) -> int:
        """Delete every live key matching the glob ``pattern``; return count."""
        with self._lock:
            # Snapshot keys first — deleting while iterating the cache mutates it.
            matched = [k for k in list(self._cache.keys()) if fnmatch.fnmatch(k, pattern)]
            for k in matched:
                self._cache.pop(k, None)
                self._set_times.pop(k, None)
            return len(matched)

    def near_expiry_keys(self, fraction: float) -> list[str]:
        """Return live keys within ``fraction`` of their TTL of expiring.

        An entry qualifies when its age ``(now - set_time)`` has reached at
        least ``(1 - fraction) * ttl`` — i.e. only the final ``fraction`` of the
        TTL window remains. Keys missing an insertion time are skipped.
        """
        with self._lock:
            now = self._timer()
            threshold = (1.0 - fraction) * self._ttl
            result: list[str] = []
            for key in list(self._cache.keys()):
                set_time = self._set_times.get(key)
                if set_time is None:
                    continue
                if (now - set_time) >= threshold:
                    result.append(key)
            return result

    # -- introspection ---------------------------------------------------

    def approx_bytes(self) -> int:
        """Approximate in-memory size of the cached *values*.

        Computed on demand by JSON-encoding each live value (``default=str`` so
        non-serializable objects degrade gracefully). This is an estimate for
        the dashboard, not an exact ``sys.getsizeof`` accounting.
        """
        with self._lock:
            total = 0
            for value in list(self._cache.values()):
                total += len(json.dumps(value, default=str).encode())
            return total

    def stats(self) -> dict[str, Any]:
        """Return a snapshot of hit/miss accounting and memory footprint."""
        with self._lock:
            hits = self.hits
            misses = self.misses
            total = hits + misses
            hit_rate = (hits / total) if total else 0.0
            entries = len(self._cache)
            approx = self.approx_bytes()
            return {
                "hits": hits,
                "misses": misses,
                "total": total,
                "hit_rate": hit_rate,
                "entries": entries,
                "max_size": self._cache.maxsize,
                "approx_bytes": approx,
                "approx_mb": approx / (1024 * 1024),
            }
