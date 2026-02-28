"""Cluster configuration management.

Provides dataclass-based configuration for storage nodes and utility
functions for loading config from environment variables or generating
local development cluster configurations.
"""

import json
import os
from dataclasses import dataclass, field


@dataclass
class ClusterConfig:
    """Configuration for a single storage node in the cluster."""

    node_id: str
    host: str = "0.0.0.0"
    port: int = 5001
    storage_dir: str = "/data"
    cluster_nodes: list = field(default_factory=list)
    replication_factor: int = 2
    health_check_interval: int = 10
    quorum_size: int = 2


def load_config() -> ClusterConfig:
    """Load cluster configuration from environment variables.

    Reads the following env vars:
        NODE_ID           — unique identifier for this node (required)
        HOST              — bind address (default: 0.0.0.0)
        PORT              — listen port (default: 5001)
        STORAGE_DIR       — path to local data directory (default: /data)
        CLUSTER_NODES     — JSON array of {id, host, port} dicts
        REPLICATION_FACTOR — number of replicas per key (default: 2)
        HEALTH_CHECK_INTERVAL — seconds between health checks (default: 10)
        QUORUM_SIZE       — minimum nodes for quorum (default: 2)

    Returns:
        ClusterConfig populated from the environment.

    Raises:
        ValueError: If NODE_ID is not set.
    """
    node_id = os.environ.get("NODE_ID")
    if not node_id:
        raise ValueError("NODE_ID environment variable is required")

    cluster_nodes_raw = os.environ.get("CLUSTER_NODES", "[]")
    try:
        cluster_nodes = json.loads(cluster_nodes_raw)
    except json.JSONDecodeError:
        cluster_nodes = []

    return ClusterConfig(
        node_id=node_id,
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", 5001)),
        storage_dir=os.environ.get("STORAGE_DIR", "/data"),
        cluster_nodes=cluster_nodes,
        replication_factor=int(os.environ.get("REPLICATION_FACTOR", 2)),
        health_check_interval=int(os.environ.get("HEALTH_CHECK_INTERVAL", 10)),
        quorum_size=int(os.environ.get("QUORUM_SIZE", 2)),
    )


def generate_cluster_config(num_nodes: int = 3, base_port: int = 5001) -> list[ClusterConfig]:
    """Generate configurations for a local development cluster.

    Creates *num_nodes* configs where each node knows about all other
    nodes in the cluster.  Useful for spinning up a local multi-node
    cluster for development and testing.

    Args:
        num_nodes: Number of nodes in the cluster.
        base_port: Starting port number; each subsequent node increments by 1.

    Returns:
        List of ClusterConfig instances, one per node.
    """
    all_nodes = [
        {"id": f"node{i + 1}", "host": "localhost", "port": base_port + i}
        for i in range(num_nodes)
    ]

    configs = []
    for i in range(num_nodes):
        configs.append(
            ClusterConfig(
                node_id=f"node{i + 1}",
                host="0.0.0.0",
                port=base_port + i,
                storage_dir=f"/data/node{i + 1}",
                cluster_nodes=list(all_nodes),
                replication_factor=min(2, num_nodes),
                health_check_interval=10,
                quorum_size=(num_nodes // 2) + 1,
            )
        )

    return configs
