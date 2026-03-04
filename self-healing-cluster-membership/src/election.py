"""Deterministic leader election based on highest node ID."""

import logging
from typing import Optional

from src.models import NodeRole, NodeStatus
from src.registry import MembershipRegistry

logger = logging.getLogger(__name__)


class LeaderElection:
    """Deterministic leader election: highest healthy node ID wins.

    No voting protocol needed since gossip ensures eventual
    consistency of membership view across all nodes.
    """

    def __init__(self, registry: MembershipRegistry) -> None:
        self._registry = registry

    async def elect_leader(self) -> Optional[str]:
        """Elect the leader as the highest-ID healthy node.

        Returns the node_id of the new leader, or None if no healthy nodes.
        """
        healthy = await self._registry.get_healthy_nodes()
        if not healthy:
            logger.warning("No healthy nodes available for leader election")
            return None

        # Sort by node_id and pick highest
        winner = max(healthy, key=lambda n: n.node_id)
        await self._registry.set_leader(winner.node_id)
        logger.info("Leader elected: %s", winner.node_id)
        return winner.node_id

    async def check_leader_health(self) -> bool:
        """Check if the current leader is still healthy.

        Returns True if leader exists and is HEALTHY, False otherwise.
        """
        leader = await self._registry.get_leader()
        if leader is None:
            return False
        return leader.status == NodeStatus.HEALTHY

    async def on_leader_failure(self, failed_node_id: str) -> Optional[str]:
        """Handle the failure of a node that might be the leader.

        If the failed node was the leader, clear leadership and
        trigger a new election.

        Returns the new leader's node_id, or None.
        """
        leader = await self._registry.get_leader()
        if leader and leader.node_id == failed_node_id:
            logger.warning("Leader %s has failed, triggering re-election", failed_node_id)
            await self._registry.clear_leader()
            return await self.elect_leader()
        return leader.node_id if leader else None
