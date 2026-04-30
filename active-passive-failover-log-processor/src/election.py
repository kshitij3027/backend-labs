"""Leader election coordinator.

The protocol is a hybrid of priority-ordered candidacy and a
Redis ``SET NX EX`` mutex. The *real* decision point is the lock; the
candidacy broadcast and the priority-based jitter are purely for
observability and to space out lock contention so the lower-priority
node tends to attempt first.

Per-election sequence (inside :py:meth:`ElectionCoordinator.run_election`):

1. Sleep ``priority * jitter_per_priority_unit`` seconds. Lower priority
   wakes sooner, biasing the order of lock attempts without affecting
   correctness.
2. Broadcast ``ElectionMessage`` to every peer in parallel via
   ``peer_client.send_candidacy``. ``asyncio.gather(..., return_exceptions=True)``
   guarantees a dead peer cannot stall the broadcast.
3. ``redis_client.acquire_lock(ttl=lock_ttl)`` — Redis serialises
   contention. **Whoever Redis says wins, wins.**
4. On win: build ``ElectionResult(winner=node_id, ...)``, broadcast it
   to all peers in parallel, return :py:attr:`NodeState.PRIMARY`.
5. On loss: read the current lock holder; if non-null, broadcast that
   value as the winner so peers converge on the same view; return
   :py:attr:`NodeState.STANDBY`.

The whole sequence runs under :py:func:`asyncio.wait_for` with the
``election_timeout``. On timeout the coordinator returns ``STANDBY`` and
bumps a counter — the caller is expected to re-trigger an election on
the next tick. Note that a timeout doesn't mean the lock acquire failed
(it might have completed in Redis after we gave up) — the caller should
reconcile by reading ``redis_client.read_lock_holder()`` next tick.

Term semantics
--------------
``current_term`` increments by 1 on every call to ``run_election``
(before the candidacy broadcast). Both ``ElectionMessage`` and
``ElectionResult`` carry the term so peers can disambiguate stale
broadcasts that arrive out of order.

Jitter scaling deviation
------------------------
plan.md specifies "priority * 100ms" jitter. With priority in
``[0, 999]`` that yields a jitter window of 0–99.9 seconds — far longer
than the 10-second election timeout, which would mean every standby
times out before its turn. We instead use **1ms per priority unit**
(``jitter_per_priority_unit=0.001``) so the jitter window fits comfortably
inside the election timeout (max ~1s of jitter for the highest-priority
node). This is deliberate; see the constructor docstring.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from src.models import ElectionMessage, ElectionResult, NodeState
from src.peer_client import PeerClient
from src.redis_client import RedisClient

logger = logging.getLogger(__name__)


class ElectionCoordinator:
    """Orchestrates a single hybrid leader election.

    Construction is cheap and side-effect-free; spinning up an instance
    does NOT start the election timer or any background tasks. The
    caller invokes :py:meth:`run_election` exactly when it wants to try
    to win the leadership.
    """

    def __init__(
        self,
        node_id: str,
        priority: int,
        peers: list[tuple[str, int]],
        redis_client: RedisClient,
        peer_client: PeerClient,
        lock_ttl: int = 6,
        election_timeout: float = 10.0,
        # See the module docstring "Jitter scaling deviation" for why
        # this is 1ms-per-unit, not 100ms-per-unit as plan.md suggests.
        jitter_per_priority_unit: float = 0.001,
    ) -> None:
        self.node_id: str = node_id
        self.priority: int = priority
        self.peers: list[tuple[str, int]] = list(peers)
        self.redis_client: RedisClient = redis_client
        self.peer_client: PeerClient = peer_client
        self.lock_ttl: int = lock_ttl
        self.election_timeout: float = election_timeout
        self.jitter_per_priority_unit: float = jitter_per_priority_unit

        # Term counter (monotonic; bumped on every run_election call).
        self._current_term: int = 0

        # Last winner observed via handle_election_result. Useful for
        # /role to display "known leader" even when we've never been
        # PRIMARY ourselves.
        self._known_winner: Optional[str] = None

        # Counters — read directly by the /metrics endpoint in commit 4a.
        self.elections_run_total: int = 0
        self.elections_won_total: int = 0
        self.elections_lost_total: int = 0
        self.elections_timed_out_total: int = 0
        self.candidacies_received_total: int = 0
        self.results_received_total: int = 0

    # --- public read-only accessors ----------------------------------------

    @property
    def current_term(self) -> int:
        """Election term number. Bumped at the start of every ``run_election``."""
        return self._current_term

    @property
    def known_winner(self) -> Optional[str]:
        """Last winner observed via :py:meth:`handle_election_result`, or None."""
        return self._known_winner

    @property
    def metrics(self) -> dict[str, int]:
        """Snapshot of the election counters for the ``/metrics`` endpoint."""
        return {
            "elections_run_total": self.elections_run_total,
            "elections_won_total": self.elections_won_total,
            "elections_lost_total": self.elections_lost_total,
            "elections_timed_out_total": self.elections_timed_out_total,
            "candidacies_received_total": self.candidacies_received_total,
            "results_received_total": self.results_received_total,
        }

    # --- main entry point --------------------------------------------------

    async def run_election(self) -> NodeState:
        """Run one full election; return the resulting node state.

        Returns :py:attr:`NodeState.PRIMARY` if we won the lock,
        :py:attr:`NodeState.STANDBY` otherwise (loss, timeout, or any
        protocol error).
        """
        self.elections_run_total += 1
        self._current_term += 1
        term = self._current_term

        try:
            return await asyncio.wait_for(
                self._run_election_inner(term),
                timeout=self.election_timeout,
            )
        except asyncio.TimeoutError:
            self.elections_timed_out_total += 1
            logger.warning(
                "election timed out (node=%s, term=%d, timeout=%.2fs)",
                self.node_id,
                term,
                self.election_timeout,
            )
            return NodeState.STANDBY

    async def _run_election_inner(self, term: int) -> NodeState:
        """Body of the election; everything above is the timeout wrapper."""
        # 1. Priority-based jitter — gives lower-priority nodes a head
        #    start so contention on the lock is partially serialised by
        #    intent rather than purely by luck.
        jitter = self.priority * self.jitter_per_priority_unit
        if jitter > 0:
            await asyncio.sleep(jitter)

        # 2. Broadcast candidacy in parallel. We don't care about the
        #    success of any individual peer — peers being down is the
        #    expected case during failover.
        candidacy_msg = ElectionMessage(
            candidate=self.node_id,
            priority=self.priority,
            term=term,
            timestamp=time.time(),
        )
        await self._broadcast_candidacy(candidacy_msg)

        # 3. Race for the leader lock. Redis decides the winner.
        won = await self.redis_client.acquire_lock(ttl=self.lock_ttl)

        if won:
            self.elections_won_total += 1
            logger.info(
                "election won (node=%s, term=%d)", self.node_id, term
            )
            result = ElectionResult(
                winner=self.node_id,
                term=term,
                timestamp=time.time(),
            )
            self._known_winner = self.node_id
            await self._broadcast_result(result)
            return NodeState.PRIMARY

        # Lost the race.
        self.elections_lost_total += 1
        winner = await self.redis_client.read_lock_holder()
        logger.info(
            "election lost (node=%s, term=%d, winner=%s)",
            self.node_id,
            term,
            winner if winner is not None else "<unknown>",
        )

        if winner is None:
            # Lock disappeared (TTL race) between our SET NX failing and
            # our GET. Don't broadcast a meaningless ``winner=""`` to
            # peers — they'll just have to re-run their own elections.
            return NodeState.STANDBY

        self._known_winner = winner
        result = ElectionResult(
            winner=winner,
            term=term,
            timestamp=time.time(),
        )
        await self._broadcast_result(result)
        return NodeState.STANDBY

    # --- inbound (peer-side) handlers --------------------------------------

    async def handle_candidacy(self, msg: ElectionMessage) -> None:
        """Receive-side handler for ``POST /election/candidacy``.

        We deliberately do NOT vote here — election outcome is decided
        by the Redis lock race. This handler exists so peers can update
        each other's observability (term, known candidates) and for the
        receive-side counter to reflect cross-cluster traffic.
        """
        self.candidacies_received_total += 1
        logger.info(
            "candidacy received (from=%s, priority=%d, term=%d, our_node=%s)",
            msg.candidate,
            msg.priority,
            msg.term,
            self.node_id,
        )

    async def handle_election_result(self, result: ElectionResult) -> None:
        """Receive-side handler for ``POST /election/result``.

        Updates :py:attr:`known_winner` and the receive counter. The
        ``/role`` endpoint reads ``known_winner`` to surface the cluster
        leader even on nodes that have never been PRIMARY themselves.
        """
        self.results_received_total += 1
        self._known_winner = result.winner
        logger.info(
            "election result received (winner=%s, term=%d, our_node=%s)",
            result.winner,
            result.term,
            self.node_id,
        )

    # --- broadcast helpers -------------------------------------------------

    async def _broadcast_candidacy(self, msg: ElectionMessage) -> None:
        """Fan out a candidacy message to all peers concurrently.

        Empty peer list is a valid no-op; ``asyncio.gather()`` of zero
        tasks resolves immediately with an empty list.
        """
        if not self.peers:
            return
        tasks = [self.peer_client.send_candidacy(peer, msg) for peer in self.peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _broadcast_result(self, result: ElectionResult) -> None:
        """Fan out an election result to all peers concurrently."""
        if not self.peers:
            return
        tasks = [
            self.peer_client.send_election_result(peer, result) for peer in self.peers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
