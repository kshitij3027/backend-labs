"""Tests for the ClusterMember class."""

import pytest
from unittest.mock import AsyncMock, patch

from src.config import ClusterConfig
from src.models import NodeRole, NodeStatus
from src.node import ClusterMember


def _make_config(**overrides) -> ClusterConfig:
    """Create a ClusterConfig with sensible defaults for testing."""
    defaults = {
        "node_id": "test-node-1",
        "address": "127.0.0.1",
        "port": 5001,
        "role": "worker",
    }
    defaults.update(overrides)
    return ClusterConfig(**defaults)


class TestClusterMemberCreation:
    def test_creates_all_components(self):
        """ClusterMember creates registry, gossip, health, server, election."""
        config = _make_config()
        node = ClusterMember(config)

        assert node.registry is not None
        assert node.config is config
        assert node._detector is not None
        assert node._election is not None
        assert node._gossip is not None
        assert node._health is not None
        assert node._server is not None


class TestClusterMemberStart:
    async def test_start_registers_self(self):
        """After start(), the node should be in the registry."""
        config = _make_config()
        node = ClusterMember(config)

        with patch.object(node._server, "start", new_callable=AsyncMock), \
             patch.object(node._server, "stop", new_callable=AsyncMock), \
             patch.object(node._gossip, "start", new_callable=AsyncMock), \
             patch.object(node._gossip, "stop", new_callable=AsyncMock), \
             patch.object(node._health, "start", new_callable=AsyncMock), \
             patch.object(node._health, "stop", new_callable=AsyncMock):
            await node.start()

            self_node = await node.registry.get_node("test-node-1")
            assert self_node is not None
            assert self_node.status == NodeStatus.HEALTHY
            assert self_node.node_id == "test-node-1"

            await node.stop()

    async def test_start_sets_leader_role(self):
        """A node with role='leader' should be set as leader after start()."""
        config = _make_config(role="leader")
        node = ClusterMember(config)

        with patch.object(node._server, "start", new_callable=AsyncMock), \
             patch.object(node._server, "stop", new_callable=AsyncMock), \
             patch.object(node._gossip, "start", new_callable=AsyncMock), \
             patch.object(node._gossip, "stop", new_callable=AsyncMock), \
             patch.object(node._health, "start", new_callable=AsyncMock), \
             patch.object(node._health, "stop", new_callable=AsyncMock):
            await node.start()

            leader = await node.registry.get_leader()
            assert leader is not None
            assert leader.node_id == "test-node-1"
            assert leader.role == NodeRole.LEADER

            await node.stop()

    async def test_stop_cleans_up(self):
        """start() followed by stop() should not crash."""
        config = _make_config()
        node = ClusterMember(config)

        with patch.object(node._server, "start", new_callable=AsyncMock), \
             patch.object(node._server, "stop", new_callable=AsyncMock) as mock_server_stop, \
             patch.object(node._gossip, "start", new_callable=AsyncMock), \
             patch.object(node._gossip, "stop", new_callable=AsyncMock) as mock_gossip_stop, \
             patch.object(node._health, "start", new_callable=AsyncMock), \
             patch.object(node._health, "stop", new_callable=AsyncMock) as mock_health_stop:
            await node.start()
            await node.stop()

            mock_server_stop.assert_awaited_once()
            mock_gossip_stop.assert_awaited_once()
            mock_health_stop.assert_awaited_once()
