"""Thread-safe bounded LRU prediction cache (Commit 16).

The classifier's hot path is dominated by repeated, near-identical log lines
(health-check pings, the same error retried, a noisy component logging the same
message hundreds of times a second). The expensive part — vectorizing the text
and running two soft-voting ensembles — produces the *same* answer every time
for those repeats, so recomputing it is pure waste.

:class:`PredictionCache` is a small, dependency-free **Least-Recently-Used**
cache that memoizes ``preprocessed-pattern -> result-dict``. It is wired into
:meth:`src.ensemble.LogClassifier.classify` (the single-log path) keyed on the
*normalized* pattern (``preprocess(raw_log)``) so that two different raw spellings
of the same underlying message share a cache slot, while genuinely distinct
messages miss and get computed.

Design notes (the contract callers rely on):

* **Thread-safe.** ``POST /classify`` runs in FastAPI's worker threadpool, so
  several requests touch the cache concurrently. Every read and write is guarded
  by a single :class:`threading.Lock`; the critical sections are tiny (an
  ``OrderedDict`` move / insert / pop), so the lock is never a throughput
  bottleneck.
* **Copy-in / copy-out.** :meth:`put` stores a shallow ``dict`` copy and
  :meth:`get` returns a shallow copy, so a caller mutating the dict it receives
  (or the dict it stored) can never corrupt the cached value. The result dicts
  are flat ``{str: str|float}`` maps, so a shallow copy fully isolates them.
* **Bounded.** At most ``maxsize`` entries are retained; inserting past the cap
  evicts the least-recently-used entry (``popitem(last=False)``). ``maxsize <= 0``
  disables caching entirely (every :meth:`get` misses and :meth:`put` is a no-op),
  which is a convenient way to turn the optimization off via config.
* **Deterministic-safe.** Because the underlying model is deterministic, a cache
  hit returns a value identical to recomputing — caching changes *latency*, never
  *output*. A freshly retrained model uses a brand-new cache instance, so a model
  hot-swap can never serve a stale prediction from an older model version.

This module is intentionally self-contained (no project imports) so it is trivial
to unit-test in isolation and cannot introduce import cycles with the ensemble.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Hashable, Optional


class PredictionCache:
    """A thread-safe, bounded LRU cache of classification result dicts.

    Maps a hashable key (the preprocessed log pattern) to a result ``dict``. The
    most-recently-accessed key is kept at the "new" end of an
    :class:`collections.OrderedDict`; once the cache is full the least-recently-used
    key (the "old" end) is evicted on the next insert.

    Attributes:
        maxsize: Maximum number of entries retained. ``<= 0`` disables the cache.
        hits: Lifetime count of :meth:`get` calls that found a cached value.
        misses: Lifetime count of :meth:`get` calls that did not.
    """

    def __init__(self, maxsize: int = 1024) -> None:
        """Create an empty cache holding at most ``maxsize`` entries.

        Args:
            maxsize: Capacity (number of distinct keys). Values ``<= 0`` disable
                the cache: :meth:`get` always misses and :meth:`put` is a no-op,
                so the classifier always recomputes (useful to switch caching off
                via ``cfg.cache_size``).
        """
        self.maxsize: int = int(maxsize)
        self._store: "OrderedDict[Hashable, dict[str, Any]]" = OrderedDict()
        self._lock = threading.Lock()
        self.hits: int = 0
        self.misses: int = 0

    def get(self, key: Hashable) -> Optional[dict[str, Any]]:
        """Return a **copy** of the cached value for ``key``, or ``None`` on a miss.

        On a hit the key is marked most-recently-used (moved to the new end) and
        :attr:`hits` is incremented; on a miss :attr:`misses` is incremented. The
        returned dict is a fresh shallow copy, so the caller may mutate it without
        affecting the cached entry.

        Args:
            key: The lookup key (here, the preprocessed log pattern).

        Returns:
            A shallow copy of the stored result dict, or ``None`` if absent (or the
            cache is disabled).
        """
        if self.maxsize <= 0:
            # Disabled cache: count the miss so hit_rate reads honestly.
            with self._lock:
                self.misses += 1
            return None

        with self._lock:
            value = self._store.get(key)
            if value is None:
                self.misses += 1
                return None
            # Mark as most-recently-used, then hand back an isolated copy.
            self._store.move_to_end(key)
            self.hits += 1
            return dict(value)

    def put(self, key: Hashable, value: dict[str, Any]) -> None:
        """Store a **copy** of ``value`` under ``key``, evicting the LRU if full.

        A shallow copy of ``value`` is stored (so a later mutation of the caller's
        dict does not change the cached entry) and marked most-recently-used.
        Re-putting an existing key refreshes both its value and its recency. If the
        insert pushes the size past :attr:`maxsize`, the least-recently-used entry
        is evicted. No-op when the cache is disabled (``maxsize <= 0``).

        Args:
            key: The key to store under (the preprocessed log pattern).
            value: The result dict to cache. Copied defensively before storing.
        """
        if self.maxsize <= 0:
            return

        with self._lock:
            # Store an isolated copy and move it to the MRU end (handles both new
            # keys and refreshes of an existing key).
            self._store[key] = dict(value)
            self._store.move_to_end(key)
            # Evict from the LRU end until back within capacity (a single insert
            # can only overflow by one, but the loop is robust to any drift).
            while len(self._store) > self.maxsize:
                self._store.popitem(last=False)

    def stats(self) -> dict[str, Any]:
        """Return a JSON-safe snapshot of cache effectiveness.

        Returns:
            A dict with::

                {
                  "hits": <int>,        # lifetime cache hits
                  "misses": <int>,      # lifetime cache misses
                  "hit_rate": <float>,  # hits / (hits + misses), 0.0 if no lookups
                  "size": <int>,        # entries currently held
                  "capacity": <int>,    # configured maxsize
                }
        """
        with self._lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total) if total else 0.0
            return {
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": hit_rate,
                "size": len(self._store),
                "capacity": self.maxsize,
            }

    def clear(self) -> None:
        """Drop all cached entries and reset the hit/miss counters.

        Leaves :attr:`maxsize` unchanged; the cache stays usable afterwards.
        """
        with self._lock:
            self._store.clear()
            self.hits = 0
            self.misses = 0
