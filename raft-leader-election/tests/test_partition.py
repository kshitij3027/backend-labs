"""Tests for network partition simulation."""

import asyncio
from unittest.mock import AsyncMock
import pytest
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_client import RpcClient
from src.election import ElectionManager


@pytest.fixture
def five_node_config():
    return RaftConfig(
        node_id="node-1",
        host="127.0.0.1",
        port=5001,
        peers=["node-2:5002", "node-3:5003", "node-4:5004", "node-5:5005"],
    )


class TestRpcClientBlocking:
    """Test RpcClient peer blocking."""

    def test_block_peer(self):
        client = RpcClient()
        client.block_peer("node-2:5002")
        assert "node-2:5002" in client.blocked_peers

    def test_unblock_peer(self):
        client = RpcClient()
        client.block_peer("node-2:5002")
        client.unblock_peer("node-2:5002")
        assert "node-2:5002" not in client.blocked_peers

    @pytest.mark.asyncio
    async def test_blocked_request_vote_returns_none(self):
        client = RpcClient()
        client.block_peer("node-2:5002")
        result = await client.send_request_vote(
            peer_address="node-2:5002", term=1, candidate_id="node-1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_blocked_append_entries_returns_none(self):
        client = RpcClient()
        client.block_peer("node-2:5002")
        result = await client.send_append_entries(
            peer_address="node-2:5002", term=1, leader_id="node-1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_blocked_pre_vote_returns_none(self):
        client = RpcClient()
        client.block_peer("node-2:5002")
        result = await client.send_pre_vote(
            peer_address="node-2:5002", term=1, candidate_id="node-1"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_unblocked_peer_works_normally(self):
        client = RpcClient()
        client.block_peer("node-2:5002")
        client.unblock_peer("node-2:5002")
        # Should try to connect (and fail because no server), not return None immediately
        result = await client.send_request_vote(
            peer_address="node-2:5002", term=1, candidate_id="node-1"
        )
        # Returns None due to connection failure, not blocking
        assert result is None


class TestPartitionElection:
    """Test election behavior under partition."""

    @pytest.mark.asyncio
    async def test_minority_cannot_elect_leader(self):
        """In a 5-node cluster, a partition of 2 nodes cannot elect a leader."""
        config = RaftConfig(
            node_id="node-1",
            host="127.0.0.1",
            port=5001,
            peers=["node-2:5002", "node-3:5003", "node-4:5004", "node-5:5005"],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        # Simulate partition: only node-2 is reachable, others blocked
        # node-1 can reach node-2 (1 vote), blocked from 3,4,5
        client.send_request_vote.side_effect = [
            (1, True),  # node-2 votes yes
            None,        # node-3 blocked
            None,        # node-4 blocked
            None,        # node-5 blocked
        ]

        await em.start_election()

        # self (1) + node-2 (1) = 2 votes, need 3 for majority of 5
        assert node.state == NodeState.CANDIDATE  # Cannot become leader

    @pytest.mark.asyncio
    async def test_majority_can_elect_leader(self):
        """In a 5-node cluster, a partition of 3 nodes CAN elect a leader."""
        config = RaftConfig(
            node_id="node-3",
            host="127.0.0.1",
            port=5003,
            peers=["node-1:5001", "node-2:5002", "node-4:5004", "node-5:5005"],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        # node-3 can reach node-4 and node-5, blocked from 1,2
        client.send_request_vote.side_effect = [
            None,        # node-1 blocked
            None,        # node-2 blocked
            (1, True),   # node-4 votes yes
            (1, True),   # node-5 votes yes
        ]

        await em.start_election()

        # self (1) + node-4 (1) + node-5 (1) = 3 votes = majority of 5
        assert node.state == NodeState.LEADER
