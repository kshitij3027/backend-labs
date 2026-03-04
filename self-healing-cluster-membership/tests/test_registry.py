"""Tests for the MembershipRegistry."""

import pytest

from src.config import ClusterConfig
from src.models import NodeInfo, NodeRole, NodeStatus
from src.registry import MembershipRegistry


class TestRegisterSelf:
    """Tests for registering the local node."""

    async def test_register_self_creates_node(self, registry, default_config):
        """register_self stores a NodeInfo and returns it."""
        node = await registry.register_self(default_config)
        assert node.node_id == "test-node-1"
        assert node.status == NodeStatus.HEALTHY
        assert node.role == NodeRole.WORKER

    async def test_register_self_stores_in_registry(self, registry, default_config):
        """The registered node is retrievable via get_node."""
        await registry.register_self(default_config)
        node = await registry.get_node("test-node-1")
        assert node is not None
        assert node.node_id == "test-node-1"


class TestUpdateNode:
    """Tests for updating nodes in the registry."""

    async def test_update_existing_node(self, registered_registry):
        """update_node overwrites an existing entry."""
        node = await registered_registry.get_node("test-node-1")
        node.heartbeat_count = 99
        await registered_registry.update_node(node)

        updated = await registered_registry.get_node("test-node-1")
        assert updated.heartbeat_count == 99

    async def test_update_adds_new_node(self, registry):
        """update_node can add a completely new node."""
        new_node = NodeInfo(
            node_id="new-node",
            address="10.0.0.99",
            port=5099,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registry.update_node(new_node)
        stored = await registry.get_node("new-node")
        assert stored is not None
        assert stored.port == 5099


class TestGetNodes:
    """Tests for retrieving nodes from the registry."""

    async def test_get_all_nodes(self, registered_registry):
        """get_all_nodes returns a dict of all registered nodes."""
        nodes = await registered_registry.get_all_nodes()
        assert len(nodes) == 1
        assert "test-node-1" in nodes

    async def test_get_node_not_found(self, registry):
        """get_node returns None for unknown node IDs."""
        assert await registry.get_node("nonexistent") is None

    async def test_get_healthy_nodes_filters(self, registered_registry):
        """get_healthy_nodes only returns HEALTHY nodes."""
        # Add a suspected node
        suspected = NodeInfo(
            node_id="sick-node",
            address="10.0.0.2",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.SUSPECTED,
        )
        await registered_registry.update_node(suspected)

        healthy = await registered_registry.get_healthy_nodes()
        assert len(healthy) == 1
        assert healthy[0].node_id == "test-node-1"

    async def test_get_peers_excludes_self(self, registered_registry):
        """get_peers returns all nodes except the one with the given ID."""
        peer = NodeInfo(
            node_id="peer-node",
            address="10.0.0.3",
            port=5003,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registered_registry.update_node(peer)

        peers = await registered_registry.get_peers("test-node-1")
        assert len(peers) == 1
        assert peers[0].node_id == "peer-node"

    async def test_get_peers_empty_when_alone(self, registered_registry):
        """get_peers returns empty list when the node is alone."""
        peers = await registered_registry.get_peers("test-node-1")
        assert peers == []


class TestStatusTransitions:
    """Tests for mark_suspected, mark_failed, mark_healthy."""

    async def test_mark_suspected(self, registered_registry):
        await registered_registry.mark_suspected("test-node-1")
        node = await registered_registry.get_node("test-node-1")
        assert node.status == NodeStatus.SUSPECTED

    async def test_mark_failed(self, registered_registry):
        await registered_registry.mark_failed("test-node-1")
        node = await registered_registry.get_node("test-node-1")
        assert node.status == NodeStatus.FAILED

    async def test_mark_healthy(self, registered_registry):
        """mark_healthy resets status and suspicion_level."""
        await registered_registry.mark_suspected("test-node-1")
        node = await registered_registry.get_node("test-node-1")
        node.suspicion_level = 5.0
        await registered_registry.update_node(node)

        await registered_registry.mark_healthy("test-node-1")
        node = await registered_registry.get_node("test-node-1")
        assert node.status == NodeStatus.HEALTHY
        assert node.suspicion_level == 0.0

    async def test_mark_nonexistent_node_is_noop(self, registry):
        """Marking a node that doesn't exist doesn't raise."""
        await registry.mark_suspected("ghost")
        await registry.mark_failed("ghost")
        await registry.mark_healthy("ghost")

    async def test_remove_node(self, registered_registry):
        await registered_registry.remove_node("test-node-1")
        assert await registered_registry.get_node("test-node-1") is None


class TestMergeDigest:
    """Tests for SWIM-style digest merge."""

    async def test_higher_incarnation_wins(self, registered_registry):
        """A digest entry with higher incarnation overwrites the local node."""
        digest = [
            {
                "node_id": "test-node-1",
                "address": "0.0.0.0",
                "port": 5001,
                "role": "worker",
                "status": "suspected",
                "last_seen": 9999.0,
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 5,
            }
        ]
        await registered_registry.merge_digest(digest)
        node = await registered_registry.get_node("test-node-1")
        assert node.incarnation == 5
        assert node.status == NodeStatus.SUSPECTED

    async def test_same_incarnation_worse_status_wins(self, registered_registry):
        """At the same incarnation, the worse status wins."""
        # Current node is HEALTHY at incarnation 0
        digest = [
            {
                "node_id": "test-node-1",
                "address": "0.0.0.0",
                "port": 5001,
                "role": "worker",
                "status": "suspected",
                "last_seen": 9999.0,
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 0,
            }
        ]
        await registered_registry.merge_digest(digest)
        node = await registered_registry.get_node("test-node-1")
        assert node.status == NodeStatus.SUSPECTED

    async def test_same_incarnation_better_status_loses(self, registered_registry):
        """At the same incarnation, a better status does NOT overwrite."""
        await registered_registry.mark_suspected("test-node-1")

        digest = [
            {
                "node_id": "test-node-1",
                "address": "0.0.0.0",
                "port": 5001,
                "role": "worker",
                "status": "healthy",
                "last_seen": 9999.0,
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 0,
            }
        ]
        await registered_registry.merge_digest(digest)
        node = await registered_registry.get_node("test-node-1")
        # Should stay SUSPECTED because healthy < suspected
        assert node.status == NodeStatus.SUSPECTED

    async def test_adds_unknown_nodes(self, registry):
        """merge_digest adds nodes that aren't already in the registry."""
        digest = [
            {
                "node_id": "unknown-node",
                "address": "10.0.0.50",
                "port": 5050,
                "role": "worker",
                "status": "healthy",
                "last_seen": 1000.0,
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 0,
            }
        ]
        await registry.merge_digest(digest)
        node = await registry.get_node("unknown-node")
        assert node is not None
        assert node.address == "10.0.0.50"

    async def test_lower_incarnation_loses(self, registered_registry):
        """A digest entry with lower incarnation is ignored."""
        # Bump local incarnation to 10
        node = await registered_registry.get_node("test-node-1")
        node.incarnation = 10
        await registered_registry.update_node(node)

        digest = [
            {
                "node_id": "test-node-1",
                "address": "0.0.0.0",
                "port": 5001,
                "role": "worker",
                "status": "failed",
                "last_seen": 9999.0,
                "heartbeat_count": 0,
                "suspicion_level": 0.0,
                "incarnation": 3,
            }
        ]
        await registered_registry.merge_digest(digest)
        node = await registered_registry.get_node("test-node-1")
        assert node.incarnation == 10
        assert node.status == NodeStatus.HEALTHY


class TestGetDigest:
    """Tests for get_digest."""

    async def test_get_digest_returns_dicts(self, registered_registry):
        """get_digest returns a list of plain dicts for all nodes."""
        digest = await registered_registry.get_digest()
        assert len(digest) == 1
        assert isinstance(digest[0], dict)
        assert digest[0]["node_id"] == "test-node-1"

    async def test_get_digest_multiple_nodes(self, registered_registry):
        """get_digest includes all registered nodes."""
        peer = NodeInfo(
            node_id="peer-1",
            address="10.0.0.2",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registered_registry.update_node(peer)

        digest = await registered_registry.get_digest()
        assert len(digest) == 2
        ids = {d["node_id"] for d in digest}
        assert ids == {"test-node-1", "peer-1"}


class TestLeaderManagement:
    """Tests for set_leader, get_leader, clear_leader."""

    async def test_get_leader_none_by_default(self, registered_registry):
        """No leader by default (default role is WORKER)."""
        leader = await registered_registry.get_leader()
        assert leader is None

    async def test_set_leader(self, registered_registry):
        """set_leader promotes a node to LEADER."""
        await registered_registry.set_leader("test-node-1")
        leader = await registered_registry.get_leader()
        assert leader is not None
        assert leader.node_id == "test-node-1"
        assert leader.role == NodeRole.LEADER

    async def test_set_leader_demotes_previous(self, registered_registry):
        """Setting a new leader demotes the old one."""
        peer = NodeInfo(
            node_id="peer-1",
            address="10.0.0.2",
            port=5002,
            role=NodeRole.WORKER,
            status=NodeStatus.HEALTHY,
        )
        await registered_registry.update_node(peer)

        await registered_registry.set_leader("test-node-1")
        await registered_registry.set_leader("peer-1")

        leader = await registered_registry.get_leader()
        assert leader.node_id == "peer-1"

        old_leader = await registered_registry.get_node("test-node-1")
        assert old_leader.role == NodeRole.WORKER

    async def test_clear_leader(self, registered_registry):
        """clear_leader sets all nodes back to WORKER."""
        await registered_registry.set_leader("test-node-1")
        await registered_registry.clear_leader()

        leader = await registered_registry.get_leader()
        assert leader is None

        node = await registered_registry.get_node("test-node-1")
        assert node.role == NodeRole.WORKER
