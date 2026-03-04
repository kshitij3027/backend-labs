"""Tests for the LeaderElection class."""

import pytest

from src.config import ClusterConfig
from src.election import LeaderElection
from src.models import NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


@pytest.fixture
def registry() -> MembershipRegistry:
    return MembershipRegistry()


@pytest.fixture
def election(registry: MembershipRegistry) -> LeaderElection:
    return LeaderElection(registry)


async def _register_node(
    registry: MembershipRegistry,
    node_id: str,
    status: NodeStatus = NodeStatus.HEALTHY,
    role: NodeRole = NodeRole.WORKER,
) -> NodeInfo:
    """Helper to register a node with a given status."""
    node = NodeInfo(
        node_id=node_id,
        address="127.0.0.1",
        port=5000,
        role=role,
        status=status,
    )
    await registry.update_node(node)
    return node


class TestElectLeader:
    async def test_highest_id_wins(self, registry, election):
        """Register 3 healthy nodes, elect leader -> highest ID wins."""
        await _register_node(registry, "node-1")
        await _register_node(registry, "node-3")
        await _register_node(registry, "node-5")

        winner = await election.elect_leader()
        assert winner == "node-5"

        leader = await registry.get_leader()
        assert leader is not None
        assert leader.node_id == "node-5"
        assert leader.role == NodeRole.LEADER

    async def test_skips_failed_nodes(self, registry, election):
        """Highest node is FAILED, next highest wins."""
        await _register_node(registry, "node-1")
        await _register_node(registry, "node-3")
        await _register_node(registry, "node-5", status=NodeStatus.FAILED)

        winner = await election.elect_leader()
        assert winner == "node-3"

    async def test_no_healthy_returns_none(self, registry, election):
        """All nodes FAILED, elect_leader returns None."""
        await _register_node(registry, "node-1", status=NodeStatus.FAILED)
        await _register_node(registry, "node-2", status=NodeStatus.FAILED)

        result = await election.elect_leader()
        assert result is None

    async def test_deterministic_election(self, registry, election):
        """Calling elect_leader multiple times always gives same result."""
        await _register_node(registry, "node-1")
        await _register_node(registry, "node-3")
        await _register_node(registry, "node-5")

        results = []
        for _ in range(5):
            winner = await election.elect_leader()
            results.append(winner)

        assert all(r == "node-5" for r in results)


class TestCheckLeaderHealth:
    async def test_check_leader_health_true(self, registry, election):
        """Leader is HEALTHY, returns True."""
        await _register_node(registry, "node-1", role=NodeRole.LEADER)

        result = await election.check_leader_health()
        assert result is True

    async def test_check_leader_health_false_no_leader(self, registry, election):
        """No leader set, returns False."""
        await _register_node(registry, "node-1")

        result = await election.check_leader_health()
        assert result is False

    async def test_check_leader_health_false_failed(self, registry, election):
        """Leader is FAILED, returns False."""
        node = await _register_node(registry, "node-1", role=NodeRole.LEADER)
        await registry.mark_failed("node-1")

        result = await election.check_leader_health()
        assert result is False


class TestOnLeaderFailure:
    async def test_on_leader_failure_triggers_reelection(self, registry, election):
        """When the leader fails, a new leader is elected."""
        await _register_node(registry, "node-1")
        await _register_node(registry, "node-3")
        await _register_node(registry, "node-5", role=NodeRole.LEADER)

        # Mark node-5 as failed in registry
        await registry.mark_failed("node-5")

        new_leader = await election.on_leader_failure("node-5")
        # node-5 is FAILED, so next highest healthy is node-3
        assert new_leader == "node-3"

        leader = await registry.get_leader()
        assert leader is not None
        assert leader.node_id == "node-3"

    async def test_on_leader_failure_non_leader_noop(self, registry, election):
        """Failing a non-leader node does not change the leader."""
        await _register_node(registry, "node-1")
        await _register_node(registry, "node-3", role=NodeRole.LEADER)
        await _register_node(registry, "node-5")

        result = await election.on_leader_failure("node-1")
        # Leader should still be node-3
        assert result == "node-3"

        leader = await registry.get_leader()
        assert leader is not None
        assert leader.node_id == "node-3"
