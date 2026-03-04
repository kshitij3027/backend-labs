"""Integration tests for cluster component interactions."""

import asyncio
import time

from src.config import ClusterConfig
from src.election import LeaderElection
from src.failure_detector import PhiAccrualFailureDetector
from src.gossip import GossipProtocol
from src.health import HealthMonitor
from src.models import GossipMessage, NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


class TestHeartbeatToPhiPipeline:
    """Test the full heartbeat -> failure detector -> phi pipeline."""

    async def test_heartbeat_records_and_phi_stays_low(self):
        """Regular heartbeats keep phi low."""
        config = ClusterConfig(node_id="node-1", port=5001)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)
        # Add a peer
        peer = NodeInfo(
            node_id="node-2",
            address="127.0.0.1",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registry.update_node(peer)

        # Simulate regular heartbeats
        for _ in range(5):
            await monitor.handle_heartbeat("node-2")
            await asyncio.sleep(0.05)

        phi = detector.compute_phi("node-2")
        assert phi < 3.0, f"Expected phi < 3.0 after regular heartbeats, got {phi}"

    async def test_missed_heartbeat_triggers_suspicion(self):
        """Missing heartbeats cause phi to rise, triggering SUSPECTED."""
        config = ClusterConfig(node_id="node-1", port=5001, phi_threshold=8.0)
        registry = MembershipRegistry()
        detector = PhiAccrualFailureDetector(config)
        monitor = HealthMonitor(config, registry, detector)

        await registry.register_self(config)
        peer = NodeInfo(
            node_id="node-2",
            address="127.0.0.1",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registry.update_node(peer)

        # Record some heartbeats at regular intervals
        for _ in range(5):
            await monitor.handle_heartbeat("node-2")
            await asyncio.sleep(0.05)

        # Simulate delay by backdating last heartbeat
        detector._last_heartbeat["node-2"] = time.time() - 0.2  # ~4x mean interval

        phi = detector.compute_phi("node-2")
        assert phi >= 1.0, f"Expected phi >= 1.0 after delay, got {phi}"


class TestGossipPropagatesFailure:
    """Test that gossip spreads failure information."""

    async def test_gossip_propagates_status_change(self):
        """A failed status in gossip updates local registry."""
        config = ClusterConfig(node_id="node-1", port=5001)
        registry = MembershipRegistry()
        gossip = GossipProtocol(config, registry)

        await registry.register_self(config)
        peer = NodeInfo(
            node_id="node-2",
            address="127.0.0.1",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registry.update_node(peer)

        # Simulate receiving gossip that node-2 is FAILED
        failed_digest = [
            {
                "node_id": "node-2",
                "address": "127.0.0.1",
                "port": 5002,
                "role": "worker",
                "status": "failed",
                "last_seen": time.time(),
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 1,
            }
        ]
        message = GossipMessage(
            sender_id="node-3", digest=failed_digest, timestamp=time.time()
        )
        await gossip.handle_gossip(message)

        node = await registry.get_node("node-2")
        assert node.status == NodeStatus.FAILED

    async def test_suspicion_refutation_via_gossip(self):
        """A node refutes false suspicion by bumping incarnation."""
        config = ClusterConfig(node_id="node-1", port=5001)
        registry = MembershipRegistry()
        gossip = GossipProtocol(config, registry)

        await registry.register_self(config)

        # Receive gossip that says WE (node-1) are SUSPECTED
        suspect_digest = [
            {
                "node_id": "node-1",
                "address": "127.0.0.1",
                "port": 5001,
                "role": "worker",
                "status": "suspected",
                "last_seen": time.time(),
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 0,
            }
        ]
        message = GossipMessage(
            sender_id="node-3", digest=suspect_digest, timestamp=time.time()
        )
        await gossip.handle_gossip(message)

        self_node = await registry.get_node("node-1")
        assert self_node.status == NodeStatus.HEALTHY
        assert self_node.incarnation > 0  # Incarnation was bumped


class TestLeaderReelectionOnFailure:
    """Test leader re-election when the leader fails."""

    async def test_leader_failure_triggers_new_election(self):
        """When the leader node fails, a new leader is elected."""
        registry = MembershipRegistry()
        election = LeaderElection(registry)

        # Register 3 healthy nodes via update_node
        for nid in ["node-1", "node-3", "node-5"]:
            node = NodeInfo(
                node_id=nid,
                address="127.0.0.1",
                port=5000,
                role=NodeRole.WORKER,
                status=NodeStatus.HEALTHY,
            )
            await registry.update_node(node)

        # Elect initial leader (should be node-5, highest ID)
        leader = await election.elect_leader()
        assert leader == "node-5"

        # Mark node-5 as failed
        await registry.mark_failed("node-5")

        # Trigger leader failure handling
        new_leader = await election.on_leader_failure("node-5")
        assert new_leader == "node-3"  # Next highest healthy

        # Verify via registry
        leader_node = await registry.get_leader()
        assert leader_node.node_id == "node-3"
