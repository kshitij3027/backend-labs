"""Cache-aware reconstruction: a bounded LRU over reconstructed entries + gzip helpers.

Random-access reconstruction (``store.reconstruct_index``) is already cheap — bounded
by ``keyframe_interval`` delta applications — but a live dashboard tends to hammer the
*same* few indices (the page the user is looking at) over and over. This module adds a
small, transparent cache in front of that path so repeated lookups of a hot index pay
the delta-replay cost once, then serve from memory. The cache only ever makes the
``<100ms`` reconstruction-latency gate (see *plan.md*) easier to hold; it can never make
a lookup slower than a fresh reconstruct, and it never changes the answer.

**Transparency by deep copy.** :class:`ReconstructionCache` stores and returns *deep
copies* of entries. A caller mutating a returned :data:`LogEntry` therefore cannot
corrupt the cached copy, the store's encoded keyframes, or a later reader's result — the
cache is observationally identical to recomputing every time, just faster. The store
already hands back fresh entries; the extra copy here defends the *cache's* own slot.

**Staleness.** The cache is keyed only by entry index, so it is valid only as long as the
underlying compressed log is unchanged. Any write that replaces the log (a new
``/api/compress``, or ``/api/reset``) must call :meth:`ReconstructionCache.clear` so stale
entries are dropped; the API wiring does exactly that.

**Deterministic gzip (the "gzip composition" piece).** :func:`gzip_bytes` /
:func:`gunzip_bytes` are the byte-reproducible gzip round-trip the engine uses wherever a
gzip blob must be stable run-to-run. ``gzip.compress(data, mtime=0)`` zeroes the timestamp
field in the gzip header, so identical input yields byte-identical output across calls and
processes (the same ``mtime=0`` discipline ``app.store`` uses for its gzip byte counts).
``gunzip_bytes(gzip_bytes(x)) == x`` for every ``x``.
"""
from __future__ import annotations

import collections
import copy
import gzip
import threading
from typing import Any, Callable

# A reconstructed log record is a plain JSON-native dict (mirrors app.models.LogEntry);
# kept as a local alias so this module has no dependency on the API model layer.
LogEntry = dict[str, Any]


class ReconstructionCache:
    """Thread-safe bounded LRU mapping entry index → reconstructed :data:`LogEntry`.

    Backed by an :class:`collections.OrderedDict` under a single
    :class:`threading.Lock`. Insertion/lookup order encodes recency: the most-recently
    used key sits at the end, the least-recently used at the front, so eviction (when the
    cache exceeds ``maxsize``) pops from the front.

    **Deep-copy isolation.** Every stored value is a deep copy of what ``compute()``
    returned, and every value handed back is a fresh deep copy of the stored slot. A
    caller mutating a returned entry can therefore never reach into the cache or the
    store, so the cache is transparent — it returns exactly what a fresh reconstruct
    would, only faster.

    **Disabled mode.** ``maxsize <= 0`` disables caching entirely: :meth:`get_or_compute`
    always calls ``compute()`` and stores nothing (it still counts the lookup as a miss),
    and :meth:`stats` reports ``enabled=False``. This lets ``RECONSTRUCT_CACHE_SIZE=0``
    turn the feature off without any call-site branching.
    """

    def __init__(self, maxsize: int) -> None:
        """Create the cache. ``maxsize <= 0`` disables caching (always computes)."""
        self._maxsize = maxsize
        self._enabled = maxsize > 0
        self._store: "collections.OrderedDict[int, LogEntry]" = collections.OrderedDict()
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def get_or_compute(self, index: int, compute: Callable[[], LogEntry]) -> LogEntry:
        """Return the entry for ``index``, computing + caching it on a miss.

        On a **hit**, the key is moved to most-recently-used and a deep copy of the
        cached entry is returned. On a **miss**, ``compute()`` is called to produce the
        entry, a deep copy is stored (evicting the least-recently-used entry if the cache
        would exceed ``maxsize``), and a separate deep copy is returned. Either path
        increments the corresponding hit/miss counter.

        ``compute`` is invoked **outside** the lock so a slow reconstruct never serializes
        other threads against the cache, and so an exception it raises (e.g. ``IndexError``
        for an out-of-range index) propagates to the caller with **nothing stored** for the
        bad key. When caching is disabled (``maxsize <= 0``) this always computes and
        stores nothing, but still counts the lookup as a miss.

        The returned value and the stored value never share structure (independent deep
        copies), so a caller mutating the result cannot corrupt a future lookup.
        """
        if not self._enabled:
            # Caching off: compute every time, store nothing, but still count the lookup.
            with self._lock:
                self._misses += 1
            return compute()

        # Fast path: hit. Move to MRU and hand back an isolated copy.
        with self._lock:
            if index in self._store:
                self._store.move_to_end(index)
                self._hits += 1
                return copy.deepcopy(self._store[index])
            self._misses += 1

        # Miss: compute outside the lock (it may be slow or raise). A raise here stores
        # nothing for ``index`` — exactly what we want for an out-of-range lookup.
        value = compute()

        with self._lock:
            # Store an isolated deep copy and mark it most-recently-used. (Re-check
            # membership: a concurrent computer may have populated this key meanwhile;
            # overwrite-and-promote keeps the slot fresh and the LRU order correct.)
            self._store[index] = copy.deepcopy(value)
            self._store.move_to_end(index)
            # Evict least-recently-used entries until within bounds (normally one).
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)

        # Return a separate copy so the caller and the cached slot never alias.
        return copy.deepcopy(value)

    def clear(self) -> None:
        """Drop all cached entries (call when the underlying compressed log changes).

        Leaves the lifetime hit/miss counters untouched — clearing reflects a data
        change, not a metrics reset. Use :meth:`reset_stats` to zero the counters.
        """
        with self._lock:
            self._store.clear()

    def reset_stats(self) -> None:
        """Zero the lifetime hit/miss counters (does not touch cached entries)."""
        with self._lock:
            self._hits = 0
            self._misses = 0

    def stats(self) -> dict:
        """Return a JSON-native snapshot of cache occupancy and hit-rate.

        Shape: ``{"size", "maxsize", "hits", "misses", "hit_rate", "enabled"}`` where
        ``hit_rate = round(hits / (hits + misses), 4)`` (``0.0`` when there have been no
        lookups). ``size`` is the current number of cached entries, ``enabled`` mirrors
        whether ``maxsize > 0``. Always safe to call, even when caching is disabled.
        """
        with self._lock:
            hits = self._hits
            misses = self._misses
            total = hits + misses
            hit_rate = round(hits / total, 4) if total else 0.0
            return {
                "size": len(self._store),
                "maxsize": self._maxsize,
                "hits": hits,
                "misses": misses,
                "hit_rate": hit_rate,
                "enabled": self._enabled,
            }


# --------------------------------------------------------------------------- #
# Deterministic gzip helpers — byte-reproducible round trip (mtime-stable).
# --------------------------------------------------------------------------- #
def gzip_bytes(data: bytes) -> bytes:
    """Gzip ``data`` with a fixed mtime, so the output is byte-reproducible.

    ``mtime=0`` zeroes the timestamp field in the gzip header, so ``gzip_bytes(x)`` is
    byte-identical across calls and processes for identical ``x`` (no embedded clock).
    Inverse of :func:`gunzip_bytes`: ``gunzip_bytes(gzip_bytes(x)) == x`` for all ``x``.
    """
    return gzip.compress(data, mtime=0)


def gunzip_bytes(blob: bytes) -> bytes:
    """Decompress a gzip ``blob`` produced by :func:`gzip_bytes` back to the original bytes."""
    return gzip.decompress(blob)
