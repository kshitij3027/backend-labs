"""Consistent hash ring with virtual nodes.

SHA-1 based, deterministic, stdlib-only. O(log N) lookups via ``bisect`` over
a sorted list of ``(position, node_id)`` tuples.
"""

from __future__ import annotations

import bisect
import hashlib
from typing import Iterable


class ConsistentHashRing:
    """A consistent hash ring mapping arbitrary string keys to physical nodes.

    Each physical node is represented by ``virtual_nodes`` positions on the
    ring (spread via ``f"{node_id}#{i}"`` hashes) so key distribution is
    approximately uniform even for small cluster sizes.
    """

    def __init__(self, virtual_nodes: int = 100) -> None:
        self.virtual_nodes: int = virtual_nodes
        # sorted list of (position, node_id); kept sorted via bisect.insort.
        self._ring: list[tuple[int, str]] = []
        self._nodes: set[str] = set()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hash(key: str) -> int:
        """SHA-1 of *key*; return the first 8 bytes as a big-endian int."""
        digest = hashlib.sha1(key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], byteorder="big", signed=False)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_node(self, node_id: str) -> None:
        """Register a physical node on the ring (no-op if already present)."""
        if node_id in self._nodes:
            return
        for i in range(self.virtual_nodes):
            position = self._hash(f"{node_id}#{i}")
            bisect.insort(self._ring, (position, node_id))
        self._nodes.add(node_id)

    def remove_node(self, node_id: str) -> None:
        """Remove every virtual-node entry belonging to *node_id*."""
        if node_id not in self._nodes:
            return
        self._ring = [entry for entry in self._ring if entry[1] != node_id]
        self._nodes.discard(node_id)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_node(self, key: str) -> str | None:
        """Return the physical node owning *key*, or None if the ring is empty."""
        if not self._ring:
            return None
        position = self._hash(key)
        # bisect over positions — search a surrogate tuple so ordering is stable.
        idx = bisect.bisect(self._ring, (position, ""))
        if idx == len(self._ring):
            idx = 0
        return self._ring[idx][1]

    def get_nodes_for_terms(
        self, terms: Iterable[str]
    ) -> dict[str, list[str]]:
        """Group *terms* by owning node.

        Returns a dict mapping ``node_id -> [terms]``. Terms whose ``get_node``
        call returns ``None`` (empty ring) are skipped.
        """
        grouped: dict[str, list[str]] = {}
        for term in terms:
            node = self.get_node(term)
            if node is None:
                continue
            grouped.setdefault(node, []).append(term)
        return grouped

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def nodes(self) -> list[str]:
        """Sorted list of physical node ids currently on the ring."""
        return sorted(self._nodes)

    def size(self) -> int:
        """Number of physical nodes currently on the ring."""
        return len(self._nodes)
