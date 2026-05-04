"""Consistent-hash ring assigning cart_ids to home regions.

A slimmed-down adaptation of
``consistent-hashing-log-distribution/src/hash_ring.py:16-150`` — we keep
only the bits we actually need for the cart workload (Feature B):

* SHA-1 hashing for stability across processes / restarts.
* ``virtual_nodes=64`` per region so a 1000-key sample lands in each
  region within ±15% of perfectly even (per ``plan.md`` §6).
* ``bisect_right`` lookup so ``get_home_region`` is O(log N).
* :class:`threading.RLock` for thread safety. We don't ``await`` while
  holding it, so it's also fine to call from an async route handler.

What we deliberately *don't* keep from the reference:

* ``add_node`` / ``remove_node`` returning ``affected_ranges`` — the
  ring is constructed once at app startup and never mutated, so the
  rebalancing metadata machinery is dead weight here.
* ``MAX_HASH`` constant — we never compare against it; SHA-1 fits in a
  Python int and ``bisect_right`` handles unbounded values fine.
"""
from __future__ import annotations

import hashlib
import threading
from bisect import bisect_right
from typing import List


class RegionRing:
    """Thread-safe consistent-hash ring of region ids.

    Each cart_id maps deterministically to exactly one *home* region;
    that home region's primary owns the canonical version of the cart.
    Even though the controller still routes every write through the
    elected primary (per ``plan.md`` §2), having a stable home region
    embedded in the cart's payload lets the conflict resolver tie-break
    deterministically when concurrent writes from different "regions"
    race (Feature B's main demo).
    """

    def __init__(self, regions: List[str], virtual_nodes: int = 64) -> None:
        self.virtual_nodes: int = virtual_nodes
        # ``_ring`` maps a virtual-node hash → physical region id.
        self._ring: dict[int, str] = {}
        # ``_sorted_keys`` is the same set of vnode hashes, sorted, so
        # we can ``bisect_right`` to find the next vnode clockwise.
        self._sorted_keys: List[int] = []
        self._regions: set[str] = set()
        # ``RLock`` (re-entrant) instead of ``Lock`` so a future
        # ``add_region`` that internally calls ``_add_region`` won't
        # deadlock — currently nothing reentrant is needed but the
        # cost is the same and the safety margin is real.
        self._lock = threading.RLock()
        for r in regions:
            self._add_region(r)

    def _hash(self, key: str) -> int:
        """Return a stable int hash for ``key`` (full SHA-1 digest)."""
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    def _add_region(self, region_id: str) -> None:
        """Insert ``virtual_nodes`` vnode positions for ``region_id``.

        Re-sorts ``_sorted_keys`` once at the end (cheaper than keeping
        it sorted via ``bisect.insort`` per insert when the count of
        new entries is large — 64 inserts vs 64 log-N positions).
        """
        with self._lock:
            self._regions.add(region_id)
            for i in range(self.virtual_nodes):
                vnode = f"{region_id}:vn{i}"
                h = self._hash(vnode)
                # In the (vanishingly unlikely) event of a SHA-1
                # collision between two vnode keys, the first writer
                # wins — same policy as the reference implementation.
                if h not in self._ring:
                    self._ring[h] = region_id
                    self._sorted_keys.append(h)
            self._sorted_keys.sort()

    def get_home_region(self, cart_id: str) -> str:
        """Return the home region for ``cart_id`` (deterministic)."""
        with self._lock:
            if not self._sorted_keys:
                # An empty ring is a programmer error — surface it
                # loudly rather than silently routing nowhere.
                raise RuntimeError("region ring is empty")
            h = self._hash(cart_id)
            idx = bisect_right(self._sorted_keys, h)
            # Wrap around: a hash above the highest vnode goes to the
            # first vnode (clockwise topology of the ring).
            if idx == len(self._sorted_keys):
                idx = 0
            return self._ring[self._sorted_keys[idx]]

    def regions(self) -> List[str]:
        """Return a sorted list of physical region ids on the ring."""
        with self._lock:
            return sorted(self._regions)
