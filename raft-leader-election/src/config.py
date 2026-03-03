"""Configuration loader for Raft nodes."""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class RaftConfig:
    """Configuration for a Raft node."""

    node_id: str = "node-1"
    host: str = "0.0.0.0"
    port: int = 5001
    peers: List[str] = field(default_factory=list)  # ["node-2:5002", "node-3:5003", ...]

    # Election timeouts (milliseconds)
    election_timeout_min: int = 150
    election_timeout_max: int = 300

    # Heartbeat interval (milliseconds)
    heartbeat_interval: int = 50

    # Node priority (higher = more likely to become leader)
    priority: int = 1

    @property
    def cluster_size(self) -> int:
        """Total number of nodes in the cluster (self + peers)."""
        return len(self.peers) + 1

    @property
    def majority(self) -> int:
        """Strict majority needed for election (more than half)."""
        return (self.cluster_size // 2) + 1


def load_config() -> RaftConfig:
    """Load configuration from environment variables."""
    peers_str = os.environ.get("PEERS", "")
    peers = [p.strip() for p in peers_str.split(",") if p.strip()]

    return RaftConfig(
        node_id=os.environ.get("NODE_ID", "node-1"),
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "5001")),
        peers=peers,
        election_timeout_min=int(os.environ.get("ELECTION_TIMEOUT_MIN", "150")),
        election_timeout_max=int(os.environ.get("ELECTION_TIMEOUT_MAX", "300")),
        heartbeat_interval=int(os.environ.get("HEARTBEAT_INTERVAL", "50")),
        priority=int(os.environ.get("PRIORITY", "1")),
    )
