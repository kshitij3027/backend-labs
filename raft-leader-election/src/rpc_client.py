"""gRPC client for sending RPCs to peer Raft nodes."""

import asyncio
import grpc
from typing import Optional, Tuple

from src.proto import raft_pb2, raft_pb2_grpc


class RpcClient:
    """Manages gRPC channels to peer nodes and sends RPCs."""

    def __init__(self, rpc_timeout: float = 0.1):
        self._channels: dict[str, grpc.aio.Channel] = {}
        self._stubs: dict[str, raft_pb2_grpc.RaftServiceStub] = {}
        self._admin_stubs: dict[str, raft_pb2_grpc.NodeAdminServiceStub] = {}
        self._rpc_timeout = rpc_timeout  # seconds

    def _get_channel(self, peer_address: str) -> grpc.aio.Channel:
        """Get or create a gRPC channel to a peer."""
        if peer_address not in self._channels:
            self._channels[peer_address] = grpc.aio.insecure_channel(peer_address)
            self._stubs[peer_address] = raft_pb2_grpc.RaftServiceStub(
                self._channels[peer_address]
            )
            self._admin_stubs[peer_address] = raft_pb2_grpc.NodeAdminServiceStub(
                self._channels[peer_address]
            )
        return self._channels[peer_address]

    async def send_request_vote(
        self, peer_address: str, term: int, candidate_id: str,
        last_log_index: int = 0, last_log_term: int = 0,
        is_pre_vote: bool = False, priority: int = 1
    ) -> Optional[Tuple[int, bool]]:
        """Send RequestVote RPC to a peer.

        Returns (term, vote_granted) or None on failure.
        """
        try:
            self._get_channel(peer_address)
            stub = self._stubs[peer_address]
            request = raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=candidate_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
                is_pre_vote=is_pre_vote,
                priority=priority,
            )
            response = await asyncio.wait_for(
                stub.RequestVote(request),
                timeout=self._rpc_timeout,
            )
            return response.term, response.vote_granted
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, OSError):
            return None

    async def send_append_entries(
        self, peer_address: str, term: int, leader_id: str,
        entries: list = None
    ) -> Optional[Tuple[int, bool]]:
        """Send AppendEntries RPC (heartbeat) to a peer.

        Returns (term, success) or None on failure.
        """
        try:
            self._get_channel(peer_address)
            stub = self._stubs[peer_address]
            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leader_id=leader_id,
                entries=entries or [],
            )
            response = await asyncio.wait_for(
                stub.AppendEntries(request),
                timeout=self._rpc_timeout,
            )
            return response.term, response.success
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, OSError):
            return None

    async def send_pre_vote(
        self, peer_address: str, term: int, candidate_id: str,
        last_log_index: int = 0, last_log_term: int = 0,
        priority: int = 1
    ) -> Optional[Tuple[int, bool]]:
        """Send PreVote RPC to a peer.

        Returns (term, vote_granted) or None on failure.
        """
        try:
            self._get_channel(peer_address)
            stub = self._stubs[peer_address]
            request = raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=candidate_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
                is_pre_vote=True,
                priority=priority,
            )
            response = await asyncio.wait_for(
                stub.PreVote(request),
                timeout=self._rpc_timeout,
            )
            return response.term, response.vote_granted
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, OSError):
            return None

    async def close(self):
        """Close all gRPC channels."""
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()
        self._admin_stubs.clear()
