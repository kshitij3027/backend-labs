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
        """Generate a random election timeout in seconds.

        Higher priority nodes get shorter timeouts, making them
        more likely to start elections first and become leader.
        Priority scaling: timeout is divided by (1 + (priority - 1) * 0.1)
        So priority=1 is baseline, priority=10 gets ~half the timeout.
        """
        ms = random.randint(
            self._config.election_timeout_min,
            self._config.election_timeout_max,
        )
        # Scale by priority (higher priority = shorter timeout)
        priority_scale = 1.0 + (self._config.priority - 1) * 0.1
        scaled_ms = ms / priority_scale
        return scaled_ms / 1000.0

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

        If pre-vote is enabled:
        1. Send PreVote RPCs to check if we could win
        2. Only proceed to real election if pre-vote majority achieved

        Real election:
        1. Become candidate (increments term, votes for self)
        2. Send RequestVote RPCs to all peers in parallel
        3. Count votes (need strict majority)
        4. If majority: become leader
        5. If not: remain candidate (timer will fire again)
        """
        if not self._node.is_alive:
            return

        # Pre-vote phase: check if we could win without incrementing term
        if self._config.peers:  # Skip pre-vote for single-node cluster
            pre_vote_ok = await self._run_pre_vote()
            if not pre_vote_ok:
                logger.info(
                    "pre_vote_failed",
                    node_id=self._node.node_id,
                    term=self._node.current_term,
                )
                return  # Don't start real election

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

    async def _run_pre_vote(self) -> bool:
        """Run pre-vote phase to check if we could win an election.

        Sends PreVote RPCs with term+1 (without actually incrementing).
        Returns True if we got a majority of pre-votes.
        """
        pre_vote_term = self._node.current_term + 1
        votes = 1  # Vote for self
        majority = self._config.majority

        logger.info(
            "pre_vote_started",
            node_id=self._node.node_id,
            pre_vote_term=pre_vote_term,
        )

        tasks = []
        for peer in self._config.peers:
            tasks.append(
                self._rpc_client.send_pre_vote(
                    peer_address=peer,
                    term=pre_vote_term,
                    candidate_id=self._node.node_id,
                    priority=self._config.priority,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception) or result is None:
                continue
            resp_term, vote_granted = result

            # If we discover a term higher than our proposed term, step down
            if resp_term > pre_vote_term:
                await self._node.step_down(resp_term)
                return False

            if vote_granted:
                votes += 1

        pre_vote_passed = votes >= majority
        logger.info(
            "pre_vote_result",
            node_id=self._node.node_id,
            votes=votes,
            majority=majority,
            passed=pre_vote_passed,
        )
        return pre_vote_passed

    async def stop(self):
        """Stop the election timer."""
        self._running = False
        self._reset_event.set()  # Unblock the timer
