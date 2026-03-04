"""Configuration for the self-healing cluster membership system."""

import os
from dataclasses import dataclass, field


@dataclass
class ClusterConfig:
    """Configuration for a cluster node."""
    node_id: str = "node-1"
    address: str = "0.0.0.0"
    port: int = 5000
    role: str = "worker"
    gossip_interval: float = 2.0
    health_check_interval: float = 1.0
    phi_threshold: float = 8.0
    gossip_fanout: int = 3
    seed_nodes: list[str] = field(default_factory=list)
    suspected_health_check_multiplier: float = 0.5
    heartbeat_window_size: int = 20
    cleanup_interval: float = 30.0


def load_config() -> ClusterConfig:
    """Load cluster configuration from environment variables."""
    seed_nodes_raw = os.environ.get("SEED_NODES", "")
    seed_nodes = [s.strip() for s in seed_nodes_raw.split(",") if s.strip()]

    return ClusterConfig(
        node_id=os.environ.get("NODE_ID", "node-1"),
        address=os.environ.get("ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "5000")),
        role=os.environ.get("ROLE", "worker"),
        gossip_interval=float(os.environ.get("GOSSIP_INTERVAL", "2.0")),
        health_check_interval=float(os.environ.get("HEALTH_CHECK_INTERVAL", "1.0")),
        phi_threshold=float(os.environ.get("PHI_THRESHOLD", "8.0")),
        gossip_fanout=int(os.environ.get("GOSSIP_FANOUT", "3")),
        seed_nodes=seed_nodes,
        suspected_health_check_multiplier=float(
            os.environ.get("SUSPECTED_HEALTH_CHECK_MULTIPLIER", "0.5")
        ),
        heartbeat_window_size=int(os.environ.get("HEARTBEAT_WINDOW_SIZE", "20")),
        cleanup_interval=float(os.environ.get("CLEANUP_INTERVAL", "30.0")),
    )
