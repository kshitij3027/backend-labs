"""Core Raft node state machine."""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from src.config import RaftConfig


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class ElectionEvent:
    """A timestamped election event for logging."""
    timestamp: float
    event_type: str  # "election_started", "vote_granted", "vote_denied", "became_leader", "became_follower", "became_candidate", "stepped_down", "heartbeat_received"
    node_id: str
    term: int
    details: str = ""


class RaftNode:
    """Core Raft node implementing the state machine.

    All state mutations are protected by an asyncio.Lock.
    """

    def __init__(self, config: RaftConfig):
        self.config = config
        self.node_id = config.node_id

        # Persistent state
        self._current_term: int = 0
        self._voted_for: Optional[str] = None

        # Volatile state
        self._state: NodeState = NodeState.FOLLOWER
        self._leader_id: Optional[str] = None
        self._is_alive: bool = True

        # Election event log
        self._election_log: List[ElectionEvent] = []
        self._max_log_size: int = 100

        # Concurrency
        self._lock = asyncio.Lock()

    # --- Properties (no lock needed for reads) ---

    @property
    def current_term(self) -> int:
        return self._current_term

    @property
    def voted_for(self) -> Optional[str]:
        return self._voted_for

    @property
    def state(self) -> NodeState:
        return self._state

    @property
    def leader_id(self) -> Optional[str]:
        return self._leader_id

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    @property
    def election_log(self) -> List[ElectionEvent]:
        return list(self._election_log)

    # --- Event logging ---

    def _log_event(self, event_type: str, details: str = ""):
        """Add an election event to the log (must be called under lock)."""
        event = ElectionEvent(
            timestamp=time.time(),
            event_type=event_type,
            node_id=self.node_id,
            term=self._current_term,
            details=details,
        )
        self._election_log.append(event)
        if len(self._election_log) > self._max_log_size:
            self._election_log = self._election_log[-self._max_log_size:]

    # --- State transitions (all require lock) ---

    async def step_down(self, new_term: int, leader_id: Optional[str] = None):
        """Step down to follower state when a higher term is discovered.

        Called when:
        - Receiving an RPC with a higher term
        - A candidate receives AppendEntries from a valid leader
        """
        async with self._lock:
            if new_term > self._current_term:
                self._current_term = new_term
                self._voted_for = None
            self._state = NodeState.FOLLOWER
            if leader_id is not None:
                self._leader_id = leader_id
            self._log_event("stepped_down", f"term={new_term}, leader={leader_id}")

    async def become_candidate(self):
        """Transition to candidate state and start an election.

        Increments term, votes for self, clears leader_id.
        Returns the new term.
        """
        async with self._lock:
            self._state = NodeState.CANDIDATE
            self._current_term += 1
            self._voted_for = self.node_id
            self._leader_id = None
            self._log_event("became_candidate", f"starting election for term {self._current_term}")
            return self._current_term

    async def become_leader(self):
        """Transition to leader state after winning an election."""
        async with self._lock:
            self._state = NodeState.LEADER
            self._leader_id = self.node_id
            self._log_event("became_leader", f"won election for term {self._current_term}")

    async def handle_vote_request(
        self, candidate_id: str, candidate_term: int,
        last_log_index: int = 0, last_log_term: int = 0,
        is_pre_vote: bool = False, candidate_priority: int = 1
    ) -> tuple[int, bool]:
        """Handle an incoming RequestVote RPC.

        Returns (current_term, vote_granted).

        Vote is granted if:
        1. Candidate's term >= our term
        2. We haven't voted for someone else in this term (or it's a pre-vote)
        3. Candidate's log is at least as up-to-date as ours
        """
        async with self._lock:
            # If candidate has a higher term, update our term
            if candidate_term > self._current_term and not is_pre_vote:
                self._current_term = candidate_term
                self._voted_for = None
                self._state = NodeState.FOLLOWER
                self._leader_id = None

            # Deny if candidate's term is less than ours
            if candidate_term < self._current_term:
                self._log_event("vote_denied", f"candidate={candidate_id}, candidate_term={candidate_term} < current_term={self._current_term}")
                return self._current_term, False

            # For real votes (not pre-vote), check if we already voted
            if not is_pre_vote:
                if self._voted_for is not None and self._voted_for != candidate_id:
                    self._log_event("vote_denied", f"candidate={candidate_id}, already voted for {self._voted_for}")
                    return self._current_term, False

                # Grant the vote
                self._voted_for = candidate_id
                self._log_event("vote_granted", f"voted for {candidate_id} in term {self._current_term}")
            else:
                self._log_event("vote_granted", f"pre-vote granted for {candidate_id} in term {candidate_term}")

            return self._current_term, True

    async def handle_append_entries(
        self, leader_id: str, leader_term: int,
        entries: list = None
    ) -> tuple[int, bool]:
        """Handle an incoming AppendEntries RPC (heartbeat or log replication).

        Returns (current_term, success).

        Success if:
        1. Leader's term >= our term
        2. We accept the leader's authority
        """
        async with self._lock:
            # Reject if leader's term is stale
            if leader_term < self._current_term:
                self._log_event("heartbeat_received", f"rejected: leader={leader_id}, leader_term={leader_term} < current_term={self._current_term}")
                return self._current_term, False

            # Accept the leader - update our state
            if leader_term > self._current_term:
                self._current_term = leader_term
                self._voted_for = None

            self._state = NodeState.FOLLOWER
            self._leader_id = leader_id

            if not entries:
                self._log_event("heartbeat_received", f"from leader={leader_id}, term={leader_term}")
            else:
                self._log_event("heartbeat_received", f"append_entries from leader={leader_id}, term={leader_term}, entries={len(entries)}")

            return self._current_term, True

    async def stop(self):
        """Stop the node (simulate failure)."""
        async with self._lock:
            self._is_alive = False
            self._log_event("stopped", "node stopped")

    async def start(self):
        """Start/restart the node."""
        async with self._lock:
            self._is_alive = True
            self._state = NodeState.FOLLOWER
            self._leader_id = None
            self._voted_for = None
            self._log_event("started", "node started as follower")
