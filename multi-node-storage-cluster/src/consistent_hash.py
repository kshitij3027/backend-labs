"""Consistent hash ring for distributing files across storage nodes."""

import hashlib
from bisect import bisect_right


class HashRing:
    """Consistent hash ring for distributing files across nodes.

    Uses virtual nodes to ensure even distribution.
    """

    def __init__(self, nodes: list[str], virtual_nodes: int = 150):
        """
        Args:
            nodes: List of node IDs (e.g., ["node1", "node2", "node3"])
            virtual_nodes: Number of virtual nodes per physical node
        """
        self.virtual_nodes = virtual_nodes
        self.ring = {}  # hash -> node_id
        self.sorted_keys = []  # sorted list of hashes
        self._nodes = set()

        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        """Generate a consistent hash for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        """Add a node to the ring with virtual nodes."""
        self._nodes.add(node)
        for i in range(self.virtual_nodes):
            vnode_key = f"{node}:vn{i}"
            h = self._hash(vnode_key)
            self.ring[h] = node
            self.sorted_keys.append(h)
        self.sorted_keys.sort()

    def remove_node(self, node: str):
        """Remove a node and all its virtual nodes from the ring."""
        self._nodes.discard(node)
        for i in range(self.virtual_nodes):
            vnode_key = f"{node}:vn{i}"
            h = self._hash(vnode_key)
            if h in self.ring:
                del self.ring[h]
                self.sorted_keys.remove(h)

    def get_node(self, key: str) -> str | None:
        """Get the primary node for a key."""
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect_right(self.sorted_keys, h) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[idx]]

    def get_nodes(self, key: str, count: int) -> list[str]:
        """Get `count` distinct nodes for a key, walking clockwise.

        Skips duplicate physical nodes (from virtual nodes).
        Returns fewer nodes if not enough distinct nodes exist.
        """
        if not self.ring:
            return []

        count = min(count, len(self._nodes))

        h = self._hash(key)
        idx = bisect_right(self.sorted_keys, h) % len(self.sorted_keys)

        result = []
        seen = set()
        checked = 0

        while len(result) < count and checked < len(self.sorted_keys):
            node = self.ring[self.sorted_keys[(idx + checked) % len(self.sorted_keys)]]
            if node not in seen:
                result.append(node)
                seen.add(node)
            checked += 1

        return result

    @property
    def nodes(self) -> set[str]:
        """Return set of physical nodes in the ring."""
        return set(self._nodes)
