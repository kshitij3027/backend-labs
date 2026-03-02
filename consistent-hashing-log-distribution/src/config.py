"""Configuration management for the consistent hashing cluster."""

import os
from dataclasses import dataclass, field

import yaml


@dataclass
class NodeConfig:
    """Configuration for a single storage node."""
    id: str
    host: str = "localhost"
    port: int = 5000
    data_dir: str = "data"


@dataclass
class ClusterConfig:
    """Configuration for the consistent hashing cluster."""
    name: str = "default-cluster"
    virtual_nodes: int = 150
    replica_count: int = 1
    nodes: list[NodeConfig] = field(default_factory=list)
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 5000


def load_config(path: str = "config/cluster.yaml") -> ClusterConfig:
    """Load cluster configuration from a YAML file.

    Args:
        path: Path to the YAML config file.

    Returns:
        ClusterConfig with settings from the file.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
    """
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    cluster_data = data.get("cluster", {})

    nodes = []
    for node_data in cluster_data.get("nodes", []):
        nodes.append(NodeConfig(
            id=node_data["id"],
            host=node_data.get("host", "localhost"),
            port=node_data.get("port", 5000),
            data_dir=node_data.get("data_dir", "data"),
        ))

    return ClusterConfig(
        name=cluster_data.get("name", "default-cluster"),
        virtual_nodes=cluster_data.get("virtual_nodes", 150),
        replica_count=cluster_data.get("replica_count", 1),
        nodes=nodes,
        dashboard_host=cluster_data.get("dashboard", {}).get("host", "0.0.0.0"),
        dashboard_port=cluster_data.get("dashboard", {}).get("port", 5000),
    )


def load_config_from_env() -> ClusterConfig:
    """Load cluster configuration from environment variables.

    Useful for Docker deployments where config file may not be available.
    Environment variables:
        CLUSTER_NAME, CLUSTER_VIRTUAL_NODES, CLUSTER_REPLICA_COUNT,
        CLUSTER_NODES (comma-separated node IDs, e.g., "node1,node2,node3"),
        DASHBOARD_HOST, DASHBOARD_PORT
    """
    node_ids = os.environ.get("CLUSTER_NODES", "node1,node2,node3").split(",")
    nodes = [NodeConfig(id=nid.strip()) for nid in node_ids if nid.strip()]

    return ClusterConfig(
        name=os.environ.get("CLUSTER_NAME", "default-cluster"),
        virtual_nodes=int(os.environ.get("CLUSTER_VIRTUAL_NODES", "150")),
        replica_count=int(os.environ.get("CLUSTER_REPLICA_COUNT", "1")),
        nodes=nodes,
        dashboard_host=os.environ.get("DASHBOARD_HOST", "0.0.0.0"),
        dashboard_port=int(os.environ.get("DASHBOARD_PORT", "5000")),
    )
