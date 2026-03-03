"""Tests for priority elections and pre-vote mechanism."""

import asyncio
from unittest.mock import AsyncMock
import pytest
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_client import RpcClient
from src.election import ElectionManager


class TestPriorityTimeouts:
    """Test that priority affects election timeout."""

    def test_higher_priority_shorter_timeout(self):
        """Higher priority nodes should have shorter election timeouts."""
        low_priority_config = RaftConfig(
            node_id="node-1", port=5001,
            peers=["node-2:5002"],
            priority=1,
            election_timeout_min=200,
            election_timeout_max=200,  # Fixed for determinism
        )
        high_priority_config = RaftConfig(
            node_id="node-2", port=5002,
            peers=["node-1:5001"],
            priority=5,
            election_timeout_min=200,
            election_timeout_max=200,
        )

        node_low = RaftNode(low_priority_config)
        node_high = RaftNode(high_priority_config)
        client = AsyncMock(spec=RpcClient)

        em_low = ElectionManager(node_low, low_priority_config, client)
        em_high = ElectionManager(node_high, high_priority_config, client)

        # Sample timeouts
        low_timeouts = [em_low._random_timeout() for _ in range(10)]
        high_timeouts = [em_high._random_timeout() for _ in range(10)]

        avg_low = sum(low_timeouts) / len(low_timeouts)
        avg_high = sum(high_timeouts) / len(high_timeouts)

        # Higher priority should have shorter timeout
        assert avg_high < avg_low

    def test_priority_1_is_baseline(self):
        """Priority 1 should give standard timeout (no scaling)."""
        config = RaftConfig(
            node_id="node-1", port=5001,
            peers=["node-2:5002"],
            priority=1,
            election_timeout_min=200,
            election_timeout_max=200,
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        timeout = em._random_timeout()
        assert abs(timeout - 0.2) < 0.001  # 200ms = 0.2s, no scaling


class TestPreVote:
    """Test pre-vote mechanism."""

    @pytest.mark.asyncio
    async def test_pre_vote_passes_then_real_election(self):
        """If pre-vote passes, real election should proceed."""
        config = RaftConfig(
            node_id="node-1", port=5001,
            peers=["node-2:5002", "node-3:5003"],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        # Pre-vote: both peers grant
        # Real vote: both peers grant
        client.send_pre_vote.return_value = (1, True)
        client.send_request_vote.return_value = (1, True)

        await em.start_election()

        assert node.state == NodeState.LEADER
        assert client.send_pre_vote.call_count == 2
        assert client.send_request_vote.call_count == 2

    @pytest.mark.asyncio
    async def test_pre_vote_fails_no_real_election(self):
        """If pre-vote fails, real election should NOT proceed."""
        config = RaftConfig(
            node_id="node-1", port=5001,
            peers=["node-2:5002", "node-3:5003"],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        # Pre-vote: both peers deny
        client.send_pre_vote.return_value = (0, False)

        await em.start_election()

        # Should NOT have become candidate or sent real votes
        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 0  # Term not incremented
        assert client.send_request_vote.call_count == 0

    @pytest.mark.asyncio
    async def test_pre_vote_partial_still_passes(self):
        """Pre-vote with partial responses can still pass with majority."""
        config = RaftConfig(
            node_id="node-1", port=5001,
            peers=["node-2:5002", "node-3:5003", "node-4:5004", "node-5:5005"],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        # Pre-vote: 2 grant, 2 unreachable (self + 2 = 3 = majority of 5)
        client.send_pre_vote.side_effect = [(1, True), (1, True), None, None]
        client.send_request_vote.side_effect = [(1, True), (1, True), None, None]

        await em.start_election()

        assert node.state == NodeState.LEADER

    @pytest.mark.asyncio
    async def test_pre_vote_higher_term_steps_down(self):
        """If pre-vote discovers higher term, step down."""
        config = RaftConfig(
            node_id="node-1", port=5001,
            peers=["node-2:5002", "node-3:5003"],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        # Pre-vote response with higher term
        client.send_pre_vote.side_effect = [(5, False), (1, True)]

        await em.start_election()

        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 5
        assert client.send_request_vote.call_count == 0

    @pytest.mark.asyncio
    async def test_single_node_skips_pre_vote(self):
        """Single-node cluster should skip pre-vote entirely."""
        config = RaftConfig(
            node_id="node-1", port=5001,
            peers=[],
        )
        node = RaftNode(config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, config, client)

        await em.start_election()

        assert node.state == NodeState.LEADER
        client.send_pre_vote.assert_not_called()
        client.send_request_vote.assert_not_called()
