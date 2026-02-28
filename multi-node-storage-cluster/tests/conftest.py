"""Shared pytest fixtures for multi-node storage cluster tests."""

import os

import pytest

from src.config import ClusterConfig, generate_cluster_config


@pytest.fixture
def sample_config():
    """Return a ClusterConfig with sensible test defaults."""
    return ClusterConfig(
        node_id="test-node",
        host="127.0.0.1",
        port=5001,
        storage_dir="/tmp/test-storage",
        cluster_nodes=[
            {"id": "node1", "host": "localhost", "port": 5001},
            {"id": "node2", "host": "localhost", "port": 5002},
            {"id": "node3", "host": "localhost", "port": 5003},
        ],
        replication_factor=2,
        health_check_interval=10,
        quorum_size=2,
    )


@pytest.fixture
def cluster_configs():
    """Return a list of 3 ClusterConfig objects for a local dev cluster."""
    return generate_cluster_config(num_nodes=3, base_port=5001)


@pytest.fixture
def tmp_storage(tmp_path):
    """Create and return a temporary storage directory."""
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    return storage_dir
