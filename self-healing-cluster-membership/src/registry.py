"""Membership registry for tracking cluster nodes."""

import asyncio
import time
from typing import Optional

from src.config import ClusterConfig
from src.models import NodeInfo, NodeRole, NodeStatus


# Status ordering for merge conflict resolution at same incarnation:
# FAILED > SUSPECTED > HEALTHY (worse status wins)
_STATUS_PRIORITY = {
    NodeStatus.HEALTHY: 0,
    NodeStatus.SUSPECTED: 1,
    NodeStatus.FAILED: 2,
}


class MembershipRegistry:
    """Thread-safe registry of cluster member nodes.

    Uses SWIM-style merge semantics:
    - Higher incarnation always wins.
    - At the same incarnation, worse status wins (FAILED > SUSPECTED > HEALTHY).
    - Unknown nodes are added automatically.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, NodeInfo] = {}
        self._lock = asyncio.Lock()

    async def register_self(self, config: ClusterConfig) -> NodeInfo:
        """Create and store a NodeInfo entry for this node."""
        node = NodeInfo(
            node_id=config.node_id,
            address=config.advertise_address,
            port=config.port,
            role=NodeRole(config.role),
            status=NodeStatus.HEALTHY,
            last_seen=time.time(),
            heartbeat_count=0,
            suspicion_level=0.0,
            incarnation=0,
        )
        async with self._lock:
            self._nodes[config.node_id] = node
        return node

    async def update_node(self, node_info: NodeInfo) -> None:
        """Store or update a node in the registry."""
        async with self._lock:
            self._nodes[node_info.node_id] = node_info

    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Retrieve a node by its ID, or None if not found."""
        async with self._lock:
            return self._nodes.get(node_id)

    async def get_all_nodes(self) -> dict[str, NodeInfo]:
        """Return a copy of all nodes in the registry."""
        async with self._lock:
            return dict(self._nodes)

    async def get_healthy_nodes(self) -> list[NodeInfo]:
        """Return all nodes with HEALTHY status."""
        async with self._lock:
            return [n for n in self._nodes.values() if n.status == NodeStatus.HEALTHY]

    async def get_peers(self, self_id: str) -> list[NodeInfo]:
        """Return all nodes except the one with the given ID."""
        async with self._lock:
            return [n for n in self._nodes.values() if n.node_id != self_id]

    async def mark_suspected(self, node_id: str) -> None:
        """Mark a node as SUSPECTED."""
        async with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].status = NodeStatus.SUSPECTED

    async def mark_failed(self, node_id: str) -> None:
        """Mark a node as FAILED."""
        async with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].status = NodeStatus.FAILED

    async def mark_healthy(self, node_id: str) -> None:
        """Mark a node as HEALTHY and reset suspicion level."""
        async with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].status = NodeStatus.HEALTHY
                self._nodes[node_id].suspicion_level = 0.0

    async def remove_node(self, node_id: str) -> None:
        """Remove a node from the registry entirely."""
        async with self._lock:
            self._nodes.pop(node_id, None)

    async def merge_digest(self, digest: list[dict]) -> None:
        """Merge a gossip digest into the local registry.

        SWIM-style merge rules:
        1. Higher incarnation always wins.
        2. At the same incarnation, worse status wins
           (FAILED > SUSPECTED > HEALTHY).
        3. Unknown nodes are added.
        """
        async with self._lock:
            for entry in digest:
                incoming = NodeInfo.from_dict(entry)
                existing = self._nodes.get(incoming.node_id)

                if existing is None:
                    # Unknown node — add it
                    self._nodes[incoming.node_id] = incoming
                    continue

                if incoming.incarnation > existing.incarnation:
                    # Higher incarnation always wins
                    self._nodes[incoming.node_id] = incoming
                elif incoming.incarnation == existing.incarnation:
                    # Same incarnation — worse status wins
                    incoming_priority = _STATUS_PRIORITY[incoming.status]
                    existing_priority = _STATUS_PRIORITY[existing.status]
                    if incoming_priority > existing_priority:
                        self._nodes[incoming.node_id] = incoming

    async def get_digest(self) -> list[dict]:
        """Return a digest of all nodes as a list of dicts."""
        async with self._lock:
            return [node.to_dict() for node in self._nodes.values()]

    async def get_leader(self) -> Optional[NodeInfo]:
        """Return the current leader node, or None if no leader is set."""
        async with self._lock:
            for node in self._nodes.values():
                if node.role == NodeRole.LEADER:
                    return node
            return None

    async def set_leader(self, node_id: str) -> None:
        """Promote a node to LEADER, demoting any existing leader."""
        async with self._lock:
            for nid, node in self._nodes.items():
                if node.role == NodeRole.LEADER:
                    node.role = NodeRole.WORKER
            if node_id in self._nodes:
                self._nodes[node_id].role = NodeRole.LEADER

    async def clear_leader(self) -> None:
        """Demote all nodes back to WORKER role."""
        async with self._lock:
            for node in self._nodes.values():
                node.role = NodeRole.WORKER
