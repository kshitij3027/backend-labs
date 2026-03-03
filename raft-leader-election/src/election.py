"""Election manager for Raft leader election."""

import asyncio
import random
import structlog
from src.node import RaftNode, NodeState
from src.config import RaftConfig
from src.rpc_client import RpcClient

logger = structlog.get_logger()


class ElectionManager:
    """Manages election timers and election execution.

    Runs a coroutine that sleeps for a randomized election timeout.
    When the timeout fires (no heartbeat received), starts an election.
    The timer is reset when a heartbeat is received.
    """

    def __init__(self, node: RaftNode, config: RaftConfig, rpc_client: RpcClient):
        self._node = node
        self._config = config
        self._rpc_client = rpc_client
        self._reset_event = asyncio.Event()
        self._running = False

    def reset_election_timer(self):
        """Reset the election timer (called when heartbeat received)."""
        self._reset_event.set()

    def _random_timeout(self) -> float:
        """Generate a random election timeout in seconds."""
        ms = random.randint(
            self._config.election_timeout_min,
            self._config.election_timeout_max,
        )
        return ms / 1000.0

    async def run_election_timer(self):
        """Main election timer coroutine.

        Runs in a loop:
        1. Wait for a random timeout
        2. If not reset during that period, start an election
        3. Repeat
        """
        self._running = True
        while self._running:
            self._reset_event.clear()
            timeout = self._random_timeout()

            try:
                # Wait for either the timeout to expire or a reset
                await asyncio.wait_for(self._reset_event.wait(), timeout=timeout)
                # Reset was triggered (heartbeat received) - restart timer
                continue
            except asyncio.TimeoutError:
                # Timeout expired - no heartbeat received
                pass

            if not self._running or not self._node.is_alive:
                continue

            # Only start election if we're a follower or candidate
            if self._node.state in (NodeState.FOLLOWER, NodeState.CANDIDATE):
                await self.start_election()

    async def start_election(self):
        """Start a new election.

        1. Become candidate (increments term, votes for self)
        2. Send RequestVote RPCs to all peers in parallel
        3. Count votes (need strict majority)
        4. If majority: become leader
        5. If not: remain candidate (timer will fire again)
        """
        if not self._node.is_alive:
            return

        # Step 1: Become candidate
        new_term = await self._node.become_candidate()
        votes_received = 1  # Vote for self
        majority = self._config.majority

        logger.info(
            "election_started",
            node_id=self._node.node_id,
            term=new_term,
            majority_needed=majority,
        )

        if not self._config.peers:
            # Single-node cluster: we win immediately
            await self._node.become_leader()
            logger.info(
                "became_leader",
                node_id=self._node.node_id,
                term=new_term,
            )
            return

        # Step 2: Send RequestVote to all peers in parallel
        tasks = []
        for peer in self._config.peers:
            tasks.append(
                self._rpc_client.send_request_vote(
                    peer_address=peer,
                    term=new_term,
                    candidate_id=self._node.node_id,
                    priority=self._config.priority,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Step 3: Count votes
        for result in results:
            if isinstance(result, Exception) or result is None:
                continue

            resp_term, vote_granted = result

            # If we discover a higher term, step down immediately
            if resp_term > new_term:
                await self._node.step_down(resp_term)
                logger.info(
                    "election_step_down",
                    node_id=self._node.node_id,
                    discovered_term=resp_term,
                )
                return

            if vote_granted:
                votes_received += 1

        # Step 4: Check if we won
        # Only become leader if we're still a candidate in the same term
        if (
            self._node.state == NodeState.CANDIDATE
            and self._node.current_term == new_term
            and votes_received >= majority
        ):
            await self._node.become_leader()
            logger.info(
                "became_leader",
                node_id=self._node.node_id,
                term=new_term,
                votes=votes_received,
            )
        else:
            logger.info(
                "election_lost",
                node_id=self._node.node_id,
                term=new_term,
                votes=votes_received,
                majority_needed=majority,
            )

    async def stop(self):
        """Stop the election timer."""
        self._running = False
        self._reset_event.set()  # Unblock the timer
