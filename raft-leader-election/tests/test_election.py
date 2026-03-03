"""Tests for the election manager."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
import pytest_asyncio
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_client import RpcClient
from src.election import ElectionManager


@pytest.fixture
def three_node_config():
    return RaftConfig(
        node_id="node-1",
        host="127.0.0.1",
        port=5001,
        peers=["node-2:5002", "node-3:5003"],
        election_timeout_min=150,
        election_timeout_max=300,
    )


@pytest.fixture
def five_node_config():
    return RaftConfig(
        node_id="node-1",
        host="127.0.0.1",
        port=5001,
        peers=["node-2:5002", "node-3:5003", "node-4:5004", "node-5:5005"],
        election_timeout_min=150,
        election_timeout_max=300,
    )


@pytest.fixture
def single_node_config():
    return RaftConfig(
        node_id="node-1",
        host="127.0.0.1",
        port=5001,
        peers=[],
    )


@pytest.fixture
def node(three_node_config):
    return RaftNode(three_node_config)


@pytest.fixture
def five_node(five_node_config):
    return RaftNode(five_node_config)


@pytest.fixture
def mock_rpc_client():
    client = AsyncMock(spec=RpcClient)
    # Default: pre-vote always passes so existing tests work
    client.send_pre_vote.return_value = (1, True)
    return client


@pytest.fixture
def election_manager(node, three_node_config, mock_rpc_client):
    return ElectionManager(node, three_node_config, mock_rpc_client)


class TestElectionTimeout:
    """Test election timeout behavior."""

    def test_random_timeout_in_range(self, election_manager, three_node_config):
        for _ in range(100):
            timeout = election_manager._random_timeout()
            assert three_node_config.election_timeout_min / 1000 <= timeout
            assert timeout <= three_node_config.election_timeout_max / 1000

    def test_reset_election_timer(self, election_manager):
        election_manager.reset_election_timer()
        assert election_manager._reset_event.is_set()


class TestStartElection:
    """Test the election process."""

    @pytest.mark.asyncio
    async def test_single_node_wins_immediately(self, single_node_config):
        node = RaftNode(single_node_config)
        client = AsyncMock(spec=RpcClient)
        em = ElectionManager(node, single_node_config, client)

        await em.start_election()

        assert node.state == NodeState.LEADER
        assert node.current_term == 1
        # No RPCs should be sent in single-node cluster
        client.send_request_vote.assert_not_called()

    @pytest.mark.asyncio
    async def test_wins_with_majority(self, node, three_node_config, mock_rpc_client):
        """3-node cluster: need 2 votes (self + 1 peer)."""
        em = ElectionManager(node, three_node_config, mock_rpc_client)

        # Both peers grant votes
        mock_rpc_client.send_request_vote.return_value = (1, True)

        await em.start_election()

        assert node.state == NodeState.LEADER
        assert node.current_term == 1

    @pytest.mark.asyncio
    async def test_wins_with_one_peer_vote_in_three_node(self, three_node_config):
        """3-node cluster: self + 1 peer = 2 = majority of 3."""
        node = RaftNode(three_node_config)
        client = AsyncMock(spec=RpcClient)
        client.send_pre_vote.return_value = (1, True)
        em = ElectionManager(node, three_node_config, client)

        # One peer grants, one denies
        client.send_request_vote.side_effect = [(1, True), (1, False)]

        await em.start_election()

        assert node.state == NodeState.LEADER

    @pytest.mark.asyncio
    async def test_loses_no_majority(self, three_node_config):
        """3-node cluster: only self vote = 1 < majority of 2."""
        node = RaftNode(three_node_config)
        client = AsyncMock(spec=RpcClient)
        client.send_pre_vote.return_value = (1, True)
        em = ElectionManager(node, three_node_config, client)

        # Both peers deny
        client.send_request_vote.return_value = (1, False)

        await em.start_election()

        assert node.state == NodeState.CANDIDATE  # stays candidate

    @pytest.mark.asyncio
    async def test_loses_in_five_node_cluster(self, five_node, five_node_config):
        """5-node cluster: need 3 votes. Self + 1 = 2 < 3."""
        client = AsyncMock(spec=RpcClient)
        client.send_pre_vote.return_value = (1, True)
        em = ElectionManager(five_node, five_node_config, client)

        # Only one peer grants, three deny
        client.send_request_vote.side_effect = [
            (1, True), (1, False), (1, False), (1, False)
        ]

        await em.start_election()

        assert five_node.state == NodeState.CANDIDATE

    @pytest.mark.asyncio
    async def test_wins_in_five_node_cluster(self, five_node, five_node_config):
        """5-node cluster: self + 2 peers = 3 = majority."""
        client = AsyncMock(spec=RpcClient)
        client.send_pre_vote.return_value = (1, True)
        em = ElectionManager(five_node, five_node_config, client)

        # Two peers grant, two deny
        client.send_request_vote.side_effect = [
            (1, True), (1, True), (1, False), (1, False)
        ]

        await em.start_election()

        assert five_node.state == NodeState.LEADER

    @pytest.mark.asyncio
    async def test_step_down_on_higher_term(self, node, three_node_config, mock_rpc_client):
        """If a peer responds with a higher term, step down."""
        em = ElectionManager(node, three_node_config, mock_rpc_client)

        # Peer responds with higher term
        mock_rpc_client.send_request_vote.side_effect = [
            (5, False),  # Higher term
            (1, True),   # This shouldn't matter
        ]

        await em.start_election()

        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 5

    @pytest.mark.asyncio
    async def test_handles_peer_failure(self, three_node_config):
        """Election still works if some peers are unreachable."""
        node = RaftNode(three_node_config)
        client = AsyncMock(spec=RpcClient)
        client.send_pre_vote.return_value = (1, True)
        em = ElectionManager(node, three_node_config, client)

        # One peer unreachable (None), one grants
        client.send_request_vote.side_effect = [None, (1, True)]

        await em.start_election()

        assert node.state == NodeState.LEADER  # self + 1 peer = 2

    @pytest.mark.asyncio
    async def test_all_peers_unreachable(self, three_node_config):
        """If all peers are unreachable, election fails."""
        node = RaftNode(three_node_config)
        client = AsyncMock(spec=RpcClient)
        client.send_pre_vote.return_value = (1, True)
        em = ElectionManager(node, three_node_config, client)

        # All peers unreachable
        client.send_request_vote.return_value = None

        await em.start_election()

        assert node.state == NodeState.CANDIDATE

    @pytest.mark.asyncio
    async def test_increments_term(self, node, three_node_config, mock_rpc_client):
        """Each election should increment the term."""
        em = ElectionManager(node, three_node_config, mock_rpc_client)

        mock_rpc_client.send_request_vote.return_value = (1, False)
        await em.start_election()
        assert node.current_term == 1

        mock_rpc_client.send_request_vote.return_value = (2, False)
        await em.start_election()
        assert node.current_term == 2

    @pytest.mark.asyncio
    async def test_dead_node_skips_election(self, node, three_node_config, mock_rpc_client):
        """A stopped node should not run elections."""
        em = ElectionManager(node, three_node_config, mock_rpc_client)
        await node.stop()

        await em.start_election()

        # Should not have changed state
        assert node.current_term == 0
        mock_rpc_client.send_request_vote.assert_not_called()


class TestElectionTimerLoop:
    """Test the election timer loop behavior."""

    @pytest.mark.asyncio
    async def test_timer_can_be_stopped(self, election_manager):
        """The timer loop should exit when stopped."""
        task = asyncio.create_task(election_manager.run_election_timer())
        await asyncio.sleep(0.05)
        await election_manager.stop()
        await asyncio.wait_for(task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_timer_reset_prevents_election(self, three_node_config):
        """Resetting the timer should prevent election timeout."""
        node = RaftNode(three_node_config)
        client = AsyncMock(spec=RpcClient)
        config = RaftConfig(
            node_id="node-1",
            host="127.0.0.1",
            port=5001,
            peers=["node-2:5002"],
            election_timeout_min=200,
            election_timeout_max=200,  # Fixed timeout for determinism
        )
        em = ElectionManager(node, config, client)

        task = asyncio.create_task(em.run_election_timer())

        # Keep resetting before timeout fires
        for _ in range(5):
            await asyncio.sleep(0.05)
            em.reset_election_timer()

        await em.stop()
        await asyncio.wait_for(task, timeout=1.0)

        # Should still be follower (no election triggered)
        assert node.state == NodeState.FOLLOWER
