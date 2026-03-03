"""Tests for the heartbeat manager."""

import asyncio
from unittest.mock import AsyncMock
import pytest
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_client import RpcClient
from src.heartbeat import HeartbeatManager


@pytest.fixture
def three_node_config():
    return RaftConfig(
        node_id="node-1",
        host="127.0.0.1",
        port=5001,
        peers=["node-2:5002", "node-3:5003"],
        heartbeat_interval=50,
    )


@pytest.fixture
def node(three_node_config):
    return RaftNode(three_node_config)


@pytest.fixture
def mock_rpc_client():
    return AsyncMock(spec=RpcClient)


@pytest.fixture
def heartbeat_manager(node, three_node_config, mock_rpc_client):
    return HeartbeatManager(node, three_node_config, mock_rpc_client)


class TestHeartbeatSending:
    """Test heartbeat sending behavior."""

    @pytest.mark.asyncio
    async def test_sends_heartbeats_to_all_peers(self, node, three_node_config, mock_rpc_client):
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)
        mock_rpc_client.send_append_entries.return_value = (1, True)

        # Make node a leader
        await node.become_candidate()
        await node.become_leader()

        await hm._send_heartbeats()

        assert mock_rpc_client.send_append_entries.call_count == 2  # 2 peers

        # Verify correct arguments
        calls = mock_rpc_client.send_append_entries.call_args_list
        peer_addresses = {call.kwargs.get("peer_address", call.args[0] if call.args else None) for call in calls}
        assert "node-2:5002" in peer_addresses
        assert "node-3:5003" in peer_addresses

    @pytest.mark.asyncio
    async def test_step_down_on_higher_term(self, node, three_node_config, mock_rpc_client):
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)

        await node.become_candidate()
        await node.become_leader()
        assert node.state == NodeState.LEADER

        # Peer responds with higher term
        mock_rpc_client.send_append_entries.side_effect = [
            (5, False),  # Higher term
            (1, True),
        ]

        await hm._send_heartbeats()

        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 5

    @pytest.mark.asyncio
    async def test_handles_peer_failure(self, node, three_node_config, mock_rpc_client):
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)

        await node.become_candidate()
        await node.become_leader()

        # One peer unreachable, one responds OK
        mock_rpc_client.send_append_entries.side_effect = [None, (1, True)]

        await hm._send_heartbeats()

        # Should still be leader
        assert node.state == NodeState.LEADER

    @pytest.mark.asyncio
    async def test_all_peers_unreachable(self, node, three_node_config, mock_rpc_client):
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)

        await node.become_candidate()
        await node.become_leader()

        mock_rpc_client.send_append_entries.return_value = None

        await hm._send_heartbeats()

        # Should still be leader (just can't reach peers)
        assert node.state == NodeState.LEADER


class TestHeartbeatLoop:
    """Test heartbeat loop behavior."""

    @pytest.mark.asyncio
    async def test_loop_can_be_stopped(self, heartbeat_manager):
        task = asyncio.create_task(heartbeat_manager.run_heartbeat_loop())
        await asyncio.sleep(0.05)
        await heartbeat_manager.stop()
        await asyncio.wait_for(task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_no_heartbeats_when_follower(self, node, three_node_config, mock_rpc_client):
        """Followers should not send heartbeats."""
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)

        assert node.state == NodeState.FOLLOWER

        task = asyncio.create_task(hm.run_heartbeat_loop())
        await asyncio.sleep(0.15)  # Wait for a couple of heartbeat intervals
        await hm.stop()
        await asyncio.wait_for(task, timeout=1.0)

        mock_rpc_client.send_append_entries.assert_not_called()

    @pytest.mark.asyncio
    async def test_heartbeats_sent_when_leader(self, node, three_node_config, mock_rpc_client):
        """Leaders should send periodic heartbeats."""
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)
        mock_rpc_client.send_append_entries.return_value = (1, True)

        await node.become_candidate()
        await node.become_leader()

        task = asyncio.create_task(hm.run_heartbeat_loop())
        await asyncio.sleep(0.15)  # Should fire ~2-3 heartbeats at 50ms interval
        await hm.stop()
        await asyncio.wait_for(task, timeout=1.0)

        # Should have sent heartbeats multiple times
        assert mock_rpc_client.send_append_entries.call_count >= 2

    @pytest.mark.asyncio
    async def test_no_heartbeats_when_dead(self, node, three_node_config, mock_rpc_client):
        """Stopped nodes should not send heartbeats even if they were leader."""
        hm = HeartbeatManager(node, three_node_config, mock_rpc_client)

        await node.become_candidate()
        await node.become_leader()
        await node.stop()

        task = asyncio.create_task(hm.run_heartbeat_loop())
        await asyncio.sleep(0.15)
        await hm.stop()
        await asyncio.wait_for(task, timeout=1.0)

        mock_rpc_client.send_append_entries.assert_not_called()
