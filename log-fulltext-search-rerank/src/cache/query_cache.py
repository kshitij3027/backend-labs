"""In-process LRU cache for :class:`SearchResponse` objects.

Cache key is ``(normalized_query, mode, index_version)`` — any write to
the underlying index bumps ``index.version``, which changes the key, so
stale entries stop being looked up. Eviction is by least-recent use.

The cache also keeps running hit/miss counters + a bounded ring of
recent latencies so ``/api/search/stats`` can surface hit ratio and
p95 latency without a separate metrics system.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Hashable


class QueryCache:
    """Bounded LRU cache with built-in hit/miss + latency stats.

    The cache is keyed on whatever :class:`Hashable` tuple the caller
    chooses — the service layer (:class:`~src.service.SearchService`)
    picks ``(normalized_query, mode, limit, index_version)`` so
    different limits and different post-ingest snapshots do not share
    entries. Keeping the key policy in the caller means this cache
    stays reusable for any future ``(key, value)`` use.

    Latency tracking is bounded: the ring holds at most
    ``latency_window`` recent samples so long-running processes do not
    accumulate an unbounded list. p95 is computed with a straight
    sort each call, which is fine at the default window of 200.
    """

    def __init__(self, max_size: int = 1000, latency_window: int = 200) -> None:
        self._max_size = max_size
        # ``OrderedDict`` makes LRU bookkeeping a two-liner: ``get``
        # promotes via ``move_to_end`` and over-capacity ``put`` pops
        # the front (oldest). No manual doubly-linked-list
        # bookkeeping required.
        self._store: "OrderedDict[Hashable, Any]" = OrderedDict()
        self._hits = 0
        self._misses = 0
        # Bounded ring of recent latency samples in milliseconds.
        # Used by :meth:`p95_latency_ms` for the stats endpoint.
        self._latencies_ms: list[float] = []
        self._latency_window = latency_window

    def get(self, key: Hashable) -> Any | None:
        """Return the cached value for ``key`` or ``None`` on miss.

        Hits promote the key to the MRU end of the store so subsequent
        evictions target genuinely cold entries. Misses bump the miss
        counter so ``hit_ratio`` stays accurate.
        """
        if key in self._store:
            self._store.move_to_end(key)
            self._hits += 1
            return self._store[key]
        self._misses += 1
        return None

    def put(self, key: Hashable, value: Any) -> None:
        """Insert or update ``key`` with ``value``.

        Updating an existing key also promotes it to MRU — the common
        case (re-populating after a miss) gets that behaviour for free.
        When the store grows past ``max_size`` the LRU (front of
        ``OrderedDict``) is evicted.
        """
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = value
        if len(self._store) > self._max_size:
            # ``last=False`` pops the oldest entry — the LRU.
            self._store.popitem(last=False)

    def invalidate_all(self) -> None:
        """Drop every cached entry.

        Counters + latency ring are preserved — they are process-level
        diagnostics, not cache contents. Callers that want a full
        reset should construct a new :class:`QueryCache`.
        """
        self._store.clear()

    def record_latency(self, ms: float) -> None:
        """Append ``ms`` to the bounded latency ring.

        Evicts the oldest sample when the ring overflows so the
        window slides forward in real time.
        """
        self._latencies_ms.append(ms)
        if len(self._latencies_ms) > self._latency_window:
            # Plain ``pop(0)`` is O(n) but the window is tiny (200 by
            # default), so the constant factor dwarfs any algorithmic
            # concern a deque would resolve.
            self._latencies_ms.pop(0)

    @property
    def hit_ratio(self) -> float:
        """Return hits / (hits + misses) as a float in ``[0, 1]``.

        Zero when no lookups have happened yet (rather than raising)
        so the stats endpoint can render the metric unconditionally.
        """
        total = self._hits + self._misses
        return (self._hits / total) if total else 0.0

    @property
    def hits(self) -> int:
        """Total number of cache hits observed since construction."""
        return self._hits

    @property
    def misses(self) -> int:
        """Total number of cache misses observed since construction."""
        return self._misses

    @property
    def size(self) -> int:
        """Number of entries currently cached."""
        return len(self._store)

    def p95_latency_ms(self) -> float:
        """Return the 95th-percentile latency across the current window.

        Returns ``0.0`` when no samples have been recorded so the
        stats endpoint is safe to call on a freshly-booted service.
        The index arithmetic uses ``max(0, int(n * 0.95) - 1)`` so the
        result stays within the sorted list for any non-empty window.
        """
        if not self._latencies_ms:
            return 0.0
        sorted_lat = sorted(self._latencies_ms)
        idx = max(0, int(len(sorted_lat) * 0.95) - 1)
        return sorted_lat[idx]
