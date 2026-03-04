"""Tests for network partition handling."""

import pytest

from src.config import ClusterConfig
from src.election import LeaderElection
from src.failure_detector import PhiAccrualFailureDetector
from src.health import HealthMonitor
from src.models import NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


class TestMajorityPartition:
    """Test majority partition rules for health monitoring."""

    async def test_minority_partition_cannot_mark_failed(self):
        """Nodes in the minority partition cannot mark others as FAILED."""
        config = ClusterConfig(node_id="node-1", port=5001, phi_threshold=8.0)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)
        # Add 4 more nodes
        for i in range(2, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Mark 3 nodes as FAILED (simulating partition where we're in minority)
        for nid in ["node-2", "node-3", "node-4"]:
            await registry.mark_failed(nid)

        # Now we have: node-1 (HEALTHY, self), node-5 (HEALTHY), 3 FAILED
        # We're at 2 reachable out of 5 = minority
        has_majority = await monitor._has_majority()
        assert not has_majority

    async def test_majority_partition_can_mark_failed(self):
        """Nodes in the majority partition can mark others as FAILED."""
        config = ClusterConfig(node_id="node-1", port=5001, phi_threshold=8.0)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)
        for i in range(2, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Mark 1 node as FAILED (we have 4 out of 5 = majority)
        await registry.mark_failed("node-5")

        has_majority = await monitor._has_majority()
        assert has_majority

    async def test_exactly_half_is_not_majority(self):
        """Exactly half of nodes reachable is not a majority."""
        config = ClusterConfig(node_id="node-1", port=5001, phi_threshold=8.0)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)
        for i in range(2, 5):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # 4 nodes total, mark 2 as FAILED -> 2 reachable out of 4 = exactly half
        await registry.mark_failed("node-3")
        await registry.mark_failed("node-4")

        has_majority = await monitor._has_majority()
        assert not has_majority

    async def test_single_node_always_has_majority(self):
        """A single-node cluster always has majority."""
        config = ClusterConfig(node_id="node-1", port=5001, phi_threshold=8.0)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)

        has_majority = await monitor._has_majority()
        assert has_majority

    async def test_suspected_nodes_count_as_reachable(self):
        """Suspected nodes should count as reachable for majority calculation."""
        config = ClusterConfig(node_id="node-1", port=5001, phi_threshold=8.0)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)
        for i in range(2, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Mark 2 as FAILED, 1 as SUSPECTED
        await registry.mark_failed("node-4")
        await registry.mark_failed("node-5")
        await registry.mark_suspected("node-3")

        # Reachable: node-1 (self), node-2 (HEALTHY), node-3 (SUSPECTED) = 3 out of 5
        has_majority = await monitor._has_majority()
        assert has_majority


class TestElectionMajority:
    """Test majority rules for leader election."""

    async def test_minority_partition_cannot_elect_leader(self):
        """Cannot elect leader without majority."""
        registry = MembershipRegistry()
        election = LeaderElection(registry)

        # Register 5 nodes via register_self (simulating nodes joining)
        for i in range(1, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Mark 3 as FAILED (only 2 healthy out of 5)
        for nid in ["node-3", "node-4", "node-5"]:
            await registry.mark_failed(nid)

        result = await election.elect_leader()
        assert result is None

    async def test_majority_partition_can_elect_leader(self):
        """Can elect leader with majority."""
        registry = MembershipRegistry()
        election = LeaderElection(registry)

        # Register 5 nodes
        for i in range(1, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Mark 2 as FAILED (3 healthy out of 5 = majority)
        for nid in ["node-4", "node-5"]:
            await registry.mark_failed(nid)

        result = await election.elect_leader()
        assert result == "node-3"  # Highest healthy node

    async def test_election_all_healthy_with_majority(self):
        """All healthy nodes = majority, leader should be elected."""
        registry = MembershipRegistry()
        election = LeaderElection(registry)

        for i in range(1, 4):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        result = await election.elect_leader()
        assert result == "node-3"

    async def test_single_node_election(self):
        """Single node cluster can always elect itself."""
        registry = MembershipRegistry()
        election = LeaderElection(registry)

        node = NodeInfo(
            node_id="node-1",
            address="127.0.0.1",
            port=5001,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registry.update_node(node)

        result = await election.elect_leader()
        assert result == "node-1"


class TestPartitionHeal:
    """Test partition heal scenarios."""

    async def test_partition_heal_restores_membership(self):
        """After partition heals, nodes can restore membership via gossip."""
        registry = MembershipRegistry()

        # Register nodes
        for i in range(1, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Mark some as FAILED (simulating partition)
        await registry.mark_failed("node-4")
        await registry.mark_failed("node-5")

        # Simulate partition heal: mark them healthy again
        await registry.mark_healthy("node-4")
        await registry.mark_healthy("node-5")

        healthy = await registry.get_healthy_nodes()
        assert len(healthy) == 5

    async def test_leader_reelection_after_partition_heal(self):
        """After partition heals, leader election picks highest healthy node."""
        registry = MembershipRegistry()
        election = LeaderElection(registry)

        for i in range(1, 6):
            node = NodeInfo(
                node_id=f"node-{i}",
                address="127.0.0.1",
                port=5000 + i,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Simulate partition: nodes 4, 5 fail
        await registry.mark_failed("node-4")
        await registry.mark_failed("node-5")

        # Elect with majority (3 out of 5)
        leader = await election.elect_leader()
        assert leader == "node-3"

        # Heal partition
        await registry.mark_healthy("node-4")
        await registry.mark_healthy("node-5")

        # Re-elect: node-5 should win now
        leader = await election.elect_leader()
        assert leader == "node-5"
