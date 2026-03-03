"""Heartbeat manager for Raft leader."""

import asyncio
import structlog
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_client import RpcClient

logger = structlog.get_logger()


class HeartbeatManager:
    """Sends periodic heartbeats when the node is a leader.

    Heartbeats are empty AppendEntries RPCs sent to all peers
    at the configured interval (default 50ms).
    """

    def __init__(self, node: RaftNode, config: RaftConfig, rpc_client: RpcClient):
        self._node = node
        self._config = config
        self._rpc_client = rpc_client
        self._running = False

    async def run_heartbeat_loop(self):
        """Main heartbeat loop. Sends heartbeats only when leader."""
        self._running = True
        interval = self._config.heartbeat_interval / 1000.0  # ms to seconds

        while self._running:
            if not self._node.is_alive:
                await asyncio.sleep(interval)
                continue

            if self._node.state != NodeState.LEADER:
                await asyncio.sleep(interval)
                continue

            # Send heartbeats to all peers in parallel
            await self._send_heartbeats()
            await asyncio.sleep(interval)

    async def _send_heartbeats(self):
        """Send AppendEntries (heartbeat) to all peers."""
        current_term = self._node.current_term
        node_id = self._node.node_id

        tasks = []
        for peer in self._config.peers:
            tasks.append(
                self._rpc_client.send_append_entries(
                    peer_address=peer,
                    term=current_term,
                    leader_id=node_id,
                )
            )

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception) or result is None:
                continue

            resp_term, success = result

            # Step down if we discover a higher term
            if resp_term > current_term:
                logger.info(
                    "heartbeat_higher_term",
                    node_id=node_id,
                    peer=self._config.peers[i],
                    our_term=current_term,
                    their_term=resp_term,
                )
                await self._node.step_down(resp_term)
                return  # Stop sending heartbeats

    async def stop(self):
        """Stop the heartbeat loop."""
        self._running = False
