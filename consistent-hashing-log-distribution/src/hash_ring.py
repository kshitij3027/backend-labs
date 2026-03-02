"""Consistent hash ring for distributing log streams across collector nodes.

Uses SHA-1 hashing with virtual nodes for even distribution and
threading locks for concurrent access safety.
"""

import hashlib
import threading
from bisect import bisect_right


# Maximum hash value for SHA-1 (2^160)
MAX_HASH = 2**160


class HashRing:
    """Thread-safe consistent hash ring with virtual nodes.

    Distributes keys across nodes using consistent hashing, ensuring
    minimal key redistribution when nodes are added or removed.

    Attributes:
        virtual_nodes: Number of virtual nodes per physical node.
    """

    def __init__(self, nodes: list[str] | None = None, virtual_nodes: int = 150):
        """Initialize the hash ring.

        Args:
            nodes: Optional list of initial node IDs to add.
            virtual_nodes: Number of virtual nodes per physical node (default 150).
        """
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}  # hash_position -> node_id
        self.sorted_keys: list[int] = []  # sorted hash positions
        self._nodes: set[str] = set()
        self._lock = threading.RLock()

        if nodes:
            for node in nodes:
                self._add_node_unlocked(node)

    def _hash(self, key: str) -> int:
        """Generate a consistent SHA-1 hash for a key.

        Args:
            key: The string key to hash.

        Returns:
            Integer hash value in range [0, 2^160).
        """
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    def _add_node_unlocked(self, node_id: str) -> dict:
        """Add a node without acquiring the lock (caller must hold it).

        Args:
            node_id: Unique identifier for the node.

        Returns:
            Metadata dict with node_id, vnodes_added, and affected_ranges.
        """
        self._nodes.add(node_id)
        vnodes_added = 0
        affected_positions = []

        for i in range(self.virtual_nodes):
            vnode_key = f"{node_id}:vn{i}"
            h = self._hash(vnode_key)
            if h not in self.ring:
                self.ring[h] = node_id
                self.sorted_keys.append(h)
                vnodes_added += 1
                affected_positions.append(h)

        self.sorted_keys.sort()

        # Calculate affected ranges: for each new vnode, identify the range
        # of keys that will now map to this node instead of the next node
        affected_ranges = []
        for pos in affected_positions:
            idx = self.sorted_keys.index(pos)
            # The affected range is from the previous vnode to this one
            if len(self.sorted_keys) > 1:
                prev_idx = (idx - 1) % len(self.sorted_keys)
                prev_pos = self.sorted_keys[prev_idx]
                affected_ranges.append({
                    "start": prev_pos,
                    "end": pos,
                })

        return {
            "node_id": node_id,
            "vnodes_added": vnodes_added,
            "affected_ranges": affected_ranges,
        }

    def add_node(self, node_id: str) -> dict:
        """Add a node to the ring with virtual nodes.

        Args:
            node_id: Unique identifier for the node.

        Returns:
            Metadata dict with keys:
                - node_id: The added node ID.
                - vnodes_added: Number of virtual nodes created.
                - affected_ranges: List of hash ranges affected by the addition.
        """
        with self._lock:
            return self._add_node_unlocked(node_id)

    def remove_node(self, node_id: str) -> dict:
        """Remove a node and all its virtual nodes from the ring.

        Args:
            node_id: The node ID to remove.

        Returns:
            Metadata dict with keys:
                - node_id: The removed node ID.
                - vnodes_removed: Number of virtual nodes removed.
                - affected_ranges: List of hash ranges affected by the removal.
        """
        with self._lock:
            self._nodes.discard(node_id)
            vnodes_removed = 0
            removed_positions = []

            for i in range(self.virtual_nodes):
                vnode_key = f"{node_id}:vn{i}"
                h = self._hash(vnode_key)
                if h in self.ring and self.ring[h] == node_id:
                    del self.ring[h]
                    removed_positions.append(h)
                    vnodes_removed += 1

            # Rebuild sorted_keys from remaining ring keys
            self.sorted_keys = sorted(self.ring.keys())

            # Calculate affected ranges for removed positions
            affected_ranges = []
            for pos in removed_positions:
                affected_ranges.append({"position": pos})

            return {
                "node_id": node_id,
                "vnodes_removed": vnodes_removed,
                "affected_ranges": affected_ranges,
            }

    def get_node(self, key: str) -> str | None:
        """Get the primary node responsible for a key.

        Uses bisect_right for O(log n) lookup with ring wraparound.

        Args:
            key: The key to look up.

        Returns:
            The node ID responsible for the key, or None if ring is empty.
        """
        with self._lock:
            if not self.ring:
                return None
            h = self._hash(key)
            idx = bisect_right(self.sorted_keys, h) % len(self.sorted_keys)
            return self.ring[self.sorted_keys[idx]]

    def get_nodes(self, key: str, count: int) -> list[str]:
        """Get multiple distinct nodes for a key, walking clockwise.

        Used for replication: returns `count` distinct physical nodes
        by walking clockwise from the key's position on the ring.

        Args:
            key: The key to look up.
            count: Number of distinct physical nodes to return.

        Returns:
            List of distinct node IDs (may be fewer than count if
            not enough physical nodes exist).
        """
        with self._lock:
            if not self.ring:
                return []

            count = min(count, len(self._nodes))
            h = self._hash(key)
            idx = bisect_right(self.sorted_keys, h) % len(self.sorted_keys)

            result = []
            seen = set()
            checked = 0

            while len(result) < count and checked < len(self.sorted_keys):
                pos = self.sorted_keys[(idx + checked) % len(self.sorted_keys)]
                node = self.ring[pos]
                if node not in seen:
                    result.append(node)
                    seen.add(node)
                checked += 1

            return result

    def get_ring_metrics(self) -> dict:
        """Calculate ring distribution metrics.

        Computes per-node vnode counts and estimated load percentages
        based on arc lengths between adjacent vnodes on the ring.

        Returns:
            Dict with keys:
                - total_vnodes: Total number of virtual nodes on the ring.
                - nodes: Dict mapping node_id to metrics dict with
                  'vnode_count' and 'load_percent'.
        """
        with self._lock:
            if not self.ring:
                return {"total_vnodes": 0, "nodes": {}}

            # Count vnodes per node
            vnode_counts: dict[str, int] = {}
            for node_id in self.ring.values():
                vnode_counts[node_id] = vnode_counts.get(node_id, 0) + 1

            # Calculate arc-length-based load distribution
            # Each arc between consecutive sorted keys represents a fraction
            # of the total ring space. The node owning the right endpoint
            # of the arc is responsible for that fraction.
            node_arc_sum: dict[str, float] = {n: 0.0 for n in self._nodes}
            n_keys = len(self.sorted_keys)

            for i in range(n_keys):
                current_pos = self.sorted_keys[i]
                prev_pos = self.sorted_keys[i - 1]  # wraps to last element when i=0

                # Calculate arc length (handles wraparound)
                if current_pos > prev_pos:
                    arc_length = current_pos - prev_pos
                else:
                    # Wraparound: from prev_pos to max, then 0 to current_pos
                    arc_length = (MAX_HASH - prev_pos) + current_pos

                # The node at current_pos owns this arc
                owning_node = self.ring[current_pos]
                arc_fraction = arc_length / MAX_HASH
                node_arc_sum[owning_node] += arc_fraction

            # Build per-node metrics
            node_metrics = {}
            for node_id in self._nodes:
                load_pct = node_arc_sum.get(node_id, 0.0) * 100.0
                node_metrics[node_id] = {
                    "vnode_count": vnode_counts.get(node_id, 0),
                    "load_percent": round(load_pct, 2),
                }

            return {
                "total_vnodes": len(self.sorted_keys),
                "nodes": node_metrics,
            }

    @property
    def nodes(self) -> set[str]:
        """Return set of physical nodes currently in the ring."""
        with self._lock:
            return set(self._nodes)
