"""Shared test fixtures for consistent hashing tests."""
import pytest
from src.hash_ring import HashRing
from src.storage_node import StorageNode
from src.config import ClusterConfig, NodeConfig
from src.cluster_coordinator import ClusterCoordinator


@pytest.fixture
def empty_ring():
    return HashRing()


@pytest.fixture
def single_node_ring():
    return HashRing(nodes=["node1"])


@pytest.fixture
def three_node_ring():
    return HashRing(nodes=["node1", "node2", "node3"])


@pytest.fixture
def storage_node():
    return StorageNode("test-node")


@pytest.fixture
def cluster_config():
    return ClusterConfig(
        name="test-cluster",
        virtual_nodes=150,
        nodes=[
            NodeConfig(id="node1"),
            NodeConfig(id="node2"),
            NodeConfig(id="node3"),
        ],
    )


@pytest.fixture
def coordinator(cluster_config):
    return ClusterCoordinator(cluster_config)
