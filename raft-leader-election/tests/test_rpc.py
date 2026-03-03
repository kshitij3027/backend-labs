"""Tests for gRPC server and client (round-trip RPCs)."""

import asyncio
import pytest
import pytest_asyncio
import grpc
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_server import RaftRpcServer, RaftServicer, NodeAdminServicer
from src.rpc_client import RpcClient
from src.proto import raft_pb2, raft_pb2_grpc


@pytest.fixture
def server_config():
    return RaftConfig(
        node_id="server-node",
        host="127.0.0.1",
        port=50051,
        peers=[],
    )


@pytest.fixture
def server_node(server_config):
    return RaftNode(server_config)


@pytest_asyncio.fixture
async def grpc_server(server_node, server_config):
    """Start a gRPC server and yield it, then stop."""
    heartbeat_received = False

    def on_heartbeat():
        nonlocal heartbeat_received
        heartbeat_received = True

    server = RaftRpcServer(
        server_node, server_config.host, server_config.port,
        on_heartbeat_received=on_heartbeat
    )
    await server.start()
    await asyncio.sleep(0.1)  # Let server bind
    yield server
    await server.stop()


@pytest.fixture
def client():
    return RpcClient(rpc_timeout=2.0)


class TestRequestVoteRoundTrip:
    """Test RequestVote RPC round-trip."""

    @pytest.mark.asyncio
    async def test_vote_granted(self, grpc_server, client, server_node):
        result = await client.send_request_vote(
            peer_address="127.0.0.1:50051",
            term=1,
            candidate_id="candidate-1",
        )
        assert result is not None
        term, granted = result
        assert granted is True
        assert server_node.voted_for == "candidate-1"

    @pytest.mark.asyncio
    async def test_vote_denied_lower_term(self, grpc_server, client, server_node):
        # Advance server node term
        await server_node.become_candidate()  # term=1

        result = await client.send_request_vote(
            peer_address="127.0.0.1:50051",
            term=0,
            candidate_id="candidate-1",
        )
        assert result is not None
        term, granted = result
        assert granted is False

    @pytest.mark.asyncio
    async def test_vote_denied_already_voted(self, grpc_server, client, server_node):
        # Vote for candidate-1 first
        await client.send_request_vote(
            peer_address="127.0.0.1:50051",
            term=1,
            candidate_id="candidate-1",
        )

        # Try to vote for candidate-2 in same term
        result = await client.send_request_vote(
            peer_address="127.0.0.1:50051",
            term=1,
            candidate_id="candidate-2",
        )
        assert result is not None
        _, granted = result
        assert granted is False

    @pytest.mark.asyncio
    async def test_connection_failure_returns_none(self, client):
        result = await client.send_request_vote(
            peer_address="127.0.0.1:59999",
            term=1,
            candidate_id="candidate-1",
        )
        assert result is None


class TestAppendEntriesRoundTrip:
    """Test AppendEntries RPC round-trip."""

    @pytest.mark.asyncio
    async def test_heartbeat_accepted(self, grpc_server, client, server_node):
        result = await client.send_append_entries(
            peer_address="127.0.0.1:50051",
            term=1,
            leader_id="leader-1",
        )
        assert result is not None
        term, success = result
        assert success is True
        assert server_node.leader_id == "leader-1"
        assert server_node.state == NodeState.FOLLOWER

    @pytest.mark.asyncio
    async def test_heartbeat_rejected_stale_term(self, grpc_server, client, server_node):
        await server_node.become_candidate()  # term=1

        result = await client.send_append_entries(
            peer_address="127.0.0.1:50051",
            term=0,
            leader_id="stale-leader",
        )
        assert result is not None
        _, success = result
        assert success is False

    @pytest.mark.asyncio
    async def test_connection_failure_returns_none(self, client):
        result = await client.send_append_entries(
            peer_address="127.0.0.1:59999",
            term=1,
            leader_id="leader-1",
        )
        assert result is None


class TestPreVoteRoundTrip:
    """Test PreVote RPC round-trip."""

    @pytest.mark.asyncio
    async def test_pre_vote_granted(self, grpc_server, client, server_node):
        result = await client.send_pre_vote(
            peer_address="127.0.0.1:50051",
            term=1,
            candidate_id="candidate-1",
        )
        assert result is not None
        term, granted = result
        assert granted is True
        # Pre-vote should NOT change voted_for
        assert server_node.voted_for is None


class TestAdminService:
    """Test NodeAdminService RPCs."""

    @pytest.mark.asyncio
    async def test_get_status(self, grpc_server, server_node):
        channel = grpc.aio.insecure_channel("127.0.0.1:50051")
        stub = raft_pb2_grpc.NodeAdminServiceStub(channel)


        response = await stub.GetStatus(raft_pb2.GetStatusRequest())
        assert response.node_id == "server-node"
        assert response.state == "follower"
        assert response.term == 0
        assert response.is_alive is True
        await channel.close()

    @pytest.mark.asyncio
    async def test_stop_node(self, grpc_server, server_node):
        channel = grpc.aio.insecure_channel("127.0.0.1:50051")
        stub = raft_pb2_grpc.NodeAdminServiceStub(channel)


        response = await stub.StopNode(raft_pb2.StopNodeRequest(graceful=True))
        assert response.success is True
        assert server_node.is_alive is False
        await channel.close()

    @pytest.mark.asyncio
    async def test_get_election_log(self, grpc_server, server_node):
        # Generate some events
        await server_node.become_candidate()
        await server_node.become_leader()

        channel = grpc.aio.insecure_channel("127.0.0.1:50051")
        stub = raft_pb2_grpc.NodeAdminServiceStub(channel)


        response = await stub.GetElectionLog(raft_pb2.GetElectionLogRequest(limit=10))
        assert len(response.events) >= 2
        event_types = [e.event_type for e in response.events]
        assert "became_candidate" in event_types
        assert "became_leader" in event_types
        await channel.close()


class TestStoppedNode:
    """Test that RPCs to a stopped node return UNAVAILABLE."""

    @pytest.mark.asyncio
    async def test_request_vote_on_stopped_node(self, grpc_server, client, server_node):
        await server_node.stop()

        result = await client.send_request_vote(
            peer_address="127.0.0.1:50051",
            term=1,
            candidate_id="candidate-1",
        )
        # Should return None because the RPC fails with UNAVAILABLE
        assert result is None

    @pytest.mark.asyncio
    async def test_append_entries_on_stopped_node(self, grpc_server, client, server_node):
        await server_node.stop()

        result = await client.send_append_entries(
            peer_address="127.0.0.1:50051",
            term=1,
            leader_id="leader-1",
        )
        assert result is None
