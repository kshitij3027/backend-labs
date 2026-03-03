"""gRPC server for Raft node — handles incoming RPCs."""

import asyncio
import grpc
from src.proto import raft_pb2, raft_pb2_grpc
from src.node import RaftNode


class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    """Handles Raft consensus RPCs (RequestVote, AppendEntries, PreVote)."""

    def __init__(self, node: RaftNode, on_heartbeat_received=None):
        self._node = node
        self._on_heartbeat_received = on_heartbeat_received  # callback to reset election timer

    async def RequestVote(self, request, context):
        if not self._node.is_alive:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Node is stopped")
            return raft_pb2.RequestVoteResponse()

        term, granted = await self._node.handle_vote_request(
            candidate_id=request.candidate_id,
            candidate_term=request.term,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
            is_pre_vote=request.is_pre_vote,
            candidate_priority=request.priority,
        )
        return raft_pb2.RequestVoteResponse(term=term, vote_granted=granted)

    async def AppendEntries(self, request, context):
        if not self._node.is_alive:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Node is stopped")
            return raft_pb2.AppendEntriesResponse()

        term, success = await self._node.handle_append_entries(
            leader_id=request.leader_id,
            leader_term=request.term,
            entries=list(request.entries) if request.entries else None,
        )

        # Reset election timer on successful heartbeat
        if success and self._on_heartbeat_received:
            self._on_heartbeat_received()

        return raft_pb2.AppendEntriesResponse(term=term, success=success)

    async def PreVote(self, request, context):
        if not self._node.is_alive:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Node is stopped")
            return raft_pb2.RequestVoteResponse()

        term, granted = await self._node.handle_vote_request(
            candidate_id=request.candidate_id,
            candidate_term=request.term,
            last_log_index=request.last_log_index,
            last_log_term=request.last_log_term,
            is_pre_vote=True,
            candidate_priority=request.priority,
        )
        return raft_pb2.RequestVoteResponse(term=term, vote_granted=granted)


class NodeAdminServicer(raft_pb2_grpc.NodeAdminServiceServicer):
    """Handles admin RPCs for the dashboard."""

    def __init__(self, node: RaftNode, rpc_client=None):
        self._node = node
        self._rpc_client = rpc_client

    async def GetStatus(self, request, context):
        return raft_pb2.GetStatusResponse(
            node_id=self._node.node_id,
            state=self._node.state.value,
            term=self._node.current_term,
            voted_for=self._node.voted_for or "",
            leader_id=self._node.leader_id or "",
            is_alive=self._node.is_alive,
        )

    async def StopNode(self, request, context):
        await self._node.stop()
        return raft_pb2.StopNodeResponse(
            success=True, message=f"Node {self._node.node_id} stopped"
        )

    async def GetElectionLog(self, request, context):
        events = self._node.election_log
        limit = request.limit if request.limit > 0 else 50
        events = events[-limit:]

        proto_events = []
        for e in events:
            proto_events.append(raft_pb2.ElectionEvent(
                timestamp=e.timestamp,
                event_type=e.event_type,
                node_id=e.node_id,
                term=e.term,
                details=e.details,
            ))

        return raft_pb2.GetElectionLogResponse(events=proto_events)

    async def BlockPeer(self, request, context):
        if self._rpc_client:
            self._rpc_client.block_peer(request.peer_address)
            return raft_pb2.BlockPeerResponse(
                success=True, message=f"Blocked {request.peer_address}"
            )
        return raft_pb2.BlockPeerResponse(success=False, message="No RPC client")

    async def UnblockPeer(self, request, context):
        if self._rpc_client:
            self._rpc_client.unblock_peer(request.peer_address)
            return raft_pb2.BlockPeerResponse(
                success=True, message=f"Unblocked {request.peer_address}"
            )
        return raft_pb2.BlockPeerResponse(success=False, message="No RPC client")

    async def GetBlockedPeers(self, request, context):
        blocked = list(self._rpc_client.blocked_peers) if self._rpc_client else []
        return raft_pb2.GetBlockedPeersResponse(blocked_peers=blocked)


class RaftRpcServer:
    """Manages the gRPC server lifecycle."""

    def __init__(self, node: RaftNode, host: str, port: int, on_heartbeat_received=None, rpc_client=None):
        self._node = node
        self._host = host
        self._port = port
        self._server = None
        self._on_heartbeat_received = on_heartbeat_received
        self._rpc_client = rpc_client

    async def start(self):
        """Start the gRPC server."""
        self._server = grpc.aio.server()

        raft_servicer = RaftServicer(self._node, self._on_heartbeat_received)
        admin_servicer = NodeAdminServicer(self._node, rpc_client=self._rpc_client)

        raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_servicer, self._server)
        raft_pb2_grpc.add_NodeAdminServiceServicer_to_server(admin_servicer, self._server)

        listen_addr = f"{self._host}:{self._port}"
        self._server.add_insecure_port(listen_addr)
        await self._server.start()
        return self

    async def stop(self):
        """Stop the gRPC server."""
        if self._server:
            await self._server.stop(grace=1)

    async def wait_for_termination(self):
        """Wait until the server terminates."""
        if self._server:
            await self._server.wait_for_termination()
