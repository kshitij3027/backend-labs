"""Tests for the Raft node state machine."""

import asyncio
import pytest
from src.node import RaftNode, NodeState, ElectionEvent
from src.config import RaftConfig


@pytest.fixture
def node(three_node_config):
    """Create a RaftNode with a 3-node cluster config."""
    return RaftNode(three_node_config)


@pytest.fixture
def five_node(five_node_config):
    """Create a RaftNode with a 5-node cluster config."""
    return RaftNode(five_node_config)


class TestNodeInitialization:
    """Test initial node state."""

    def test_initial_state_is_follower(self, node):
        assert node.state == NodeState.FOLLOWER

    def test_initial_term_is_zero(self, node):
        assert node.current_term == 0

    def test_initial_voted_for_is_none(self, node):
        assert node.voted_for is None

    def test_initial_leader_is_none(self, node):
        assert node.leader_id is None

    def test_initial_is_alive(self, node):
        assert node.is_alive is True

    def test_node_id_from_config(self, node):
        assert node.node_id == "test-node-1"


class TestBecomeCandidate:
    """Test transition to candidate state."""

    @pytest.mark.asyncio
    async def test_increments_term(self, node):
        term = await node.become_candidate()
        assert term == 1
        assert node.current_term == 1

    @pytest.mark.asyncio
    async def test_votes_for_self(self, node):
        await node.become_candidate()
        assert node.voted_for == "test-node-1"

    @pytest.mark.asyncio
    async def test_state_is_candidate(self, node):
        await node.become_candidate()
        assert node.state == NodeState.CANDIDATE

    @pytest.mark.asyncio
    async def test_clears_leader_id(self, node):
        # Simulate having a leader first
        await node.handle_append_entries("old-leader", 1)
        assert node.leader_id == "old-leader"

        await node.become_candidate()
        assert node.leader_id is None

    @pytest.mark.asyncio
    async def test_consecutive_elections_increment_term(self, node):
        await node.become_candidate()
        assert node.current_term == 1
        await node.become_candidate()
        assert node.current_term == 2
        await node.become_candidate()
        assert node.current_term == 3

    @pytest.mark.asyncio
    async def test_logs_event(self, node):
        await node.become_candidate()
        events = node.election_log
        assert len(events) >= 1
        assert events[-1].event_type == "became_candidate"


class TestBecomeLeader:
    """Test transition to leader state."""

    @pytest.mark.asyncio
    async def test_state_is_leader(self, node):
        await node.become_candidate()
        await node.become_leader()
        assert node.state == NodeState.LEADER

    @pytest.mark.asyncio
    async def test_leader_id_is_self(self, node):
        await node.become_candidate()
        await node.become_leader()
        assert node.leader_id == "test-node-1"

    @pytest.mark.asyncio
    async def test_logs_event(self, node):
        await node.become_candidate()
        await node.become_leader()
        events = node.election_log
        assert any(e.event_type == "became_leader" for e in events)


class TestStepDown:
    """Test stepping down to follower."""

    @pytest.mark.asyncio
    async def test_step_down_from_leader(self, node):
        await node.become_candidate()
        await node.become_leader()
        assert node.state == NodeState.LEADER

        await node.step_down(new_term=5, leader_id="node-2")
        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 5
        assert node.leader_id == "node-2"
        assert node.voted_for is None

    @pytest.mark.asyncio
    async def test_step_down_from_candidate(self, node):
        await node.become_candidate()
        assert node.state == NodeState.CANDIDATE

        await node.step_down(new_term=3)
        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 3

    @pytest.mark.asyncio
    async def test_step_down_clears_vote(self, node):
        await node.become_candidate()
        assert node.voted_for == "test-node-1"

        await node.step_down(new_term=5)
        assert node.voted_for is None

    @pytest.mark.asyncio
    async def test_step_down_same_term_keeps_vote(self, node):
        """Step down without term change should not clear voted_for."""
        await node.become_candidate()  # term=1, voted_for=self
        await node.step_down(new_term=1)  # same term
        # voted_for should remain since term didn't increase
        assert node.voted_for == "test-node-1"


class TestHandleVoteRequest:
    """Test RequestVote handling."""

    @pytest.mark.asyncio
    async def test_grant_vote_higher_term(self, node):
        term, granted = await node.handle_vote_request(
            candidate_id="node-2", candidate_term=1
        )
        assert granted is True
        assert node.voted_for == "node-2"

    @pytest.mark.asyncio
    async def test_deny_vote_lower_term(self, node):
        await node.become_candidate()  # term=1
        term, granted = await node.handle_vote_request(
            candidate_id="node-2", candidate_term=0
        )
        assert granted is False

    @pytest.mark.asyncio
    async def test_deny_vote_already_voted(self, node):
        # Vote for node-2 first
        await node.handle_vote_request(candidate_id="node-2", candidate_term=1)

        # Try to vote for node-3 in same term
        term, granted = await node.handle_vote_request(
            candidate_id="node-3", candidate_term=1
        )
        assert granted is False
        assert node.voted_for == "node-2"

    @pytest.mark.asyncio
    async def test_grant_vote_new_term_clears_old_vote(self, node):
        # Vote for node-2 in term 1
        await node.handle_vote_request(candidate_id="node-2", candidate_term=1)
        assert node.voted_for == "node-2"

        # New term - should be able to vote for node-3
        term, granted = await node.handle_vote_request(
            candidate_id="node-3", candidate_term=2
        )
        assert granted is True
        assert node.voted_for == "node-3"

    @pytest.mark.asyncio
    async def test_grant_vote_same_candidate_again(self, node):
        """Allow re-voting for the same candidate in the same term."""
        await node.handle_vote_request(candidate_id="node-2", candidate_term=1)
        term, granted = await node.handle_vote_request(
            candidate_id="node-2", candidate_term=1
        )
        assert granted is True

    @pytest.mark.asyncio
    async def test_higher_term_steps_down_leader(self, node):
        await node.become_candidate()
        await node.become_leader()
        assert node.state == NodeState.LEADER

        term, granted = await node.handle_vote_request(
            candidate_id="node-2", candidate_term=5
        )
        assert node.state == NodeState.FOLLOWER
        assert node.current_term == 5
        assert granted is True


class TestHandleAppendEntries:
    """Test AppendEntries (heartbeat) handling."""

    @pytest.mark.asyncio
    async def test_accept_heartbeat_valid_leader(self, node):
        term, success = await node.handle_append_entries(
            leader_id="node-2", leader_term=1
        )
        assert success is True
        assert node.leader_id == "node-2"
        assert node.state == NodeState.FOLLOWER

    @pytest.mark.asyncio
    async def test_reject_heartbeat_stale_term(self, node):
        await node.become_candidate()  # term=1
        term, success = await node.handle_append_entries(
            leader_id="node-2", leader_term=0
        )
        assert success is False

    @pytest.mark.asyncio
    async def test_heartbeat_updates_term(self, node):
        await node.handle_append_entries(leader_id="node-2", leader_term=5)
        assert node.current_term == 5

    @pytest.mark.asyncio
    async def test_heartbeat_steps_down_candidate(self, node):
        await node.become_candidate()  # term=1
        await node.handle_append_entries(leader_id="node-2", leader_term=1)
        assert node.state == NodeState.FOLLOWER
        assert node.leader_id == "node-2"

    @pytest.mark.asyncio
    async def test_heartbeat_steps_down_leader(self, node):
        """A leader receiving a heartbeat with same/higher term steps down."""
        await node.become_candidate()
        await node.become_leader()
        assert node.state == NodeState.LEADER

        await node.handle_append_entries(leader_id="node-2", leader_term=5)
        assert node.state == NodeState.FOLLOWER
        assert node.leader_id == "node-2"


class TestStopAndStart:
    """Test node stop/start."""

    @pytest.mark.asyncio
    async def test_stop_node(self, node):
        await node.stop()
        assert node.is_alive is False

    @pytest.mark.asyncio
    async def test_start_node(self, node):
        await node.stop()
        await node.start()
        assert node.is_alive is True
        assert node.state == NodeState.FOLLOWER

    @pytest.mark.asyncio
    async def test_start_resets_state(self, node):
        await node.become_candidate()
        await node.become_leader()
        await node.stop()
        await node.start()
        assert node.state == NodeState.FOLLOWER
        assert node.leader_id is None
        assert node.voted_for is None


class TestElectionLog:
    """Test election event logging."""

    @pytest.mark.asyncio
    async def test_events_are_logged(self, node):
        await node.become_candidate()
        await node.become_leader()
        events = node.election_log
        assert len(events) >= 2

    @pytest.mark.asyncio
    async def test_event_has_timestamp(self, node):
        await node.become_candidate()
        events = node.election_log
        assert events[0].timestamp > 0

    @pytest.mark.asyncio
    async def test_log_capped_at_max_size(self, node):
        node._max_log_size = 5
        for _ in range(10):
            await node.become_candidate()
        events = node.election_log
        assert len(events) <= 5
