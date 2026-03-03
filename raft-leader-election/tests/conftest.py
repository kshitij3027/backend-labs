"""Shared test fixtures for Raft tests."""

import pytest
from src.config import RaftConfig


@pytest.fixture
def single_node_config():
    """Config for a single node with no peers."""
    return RaftConfig(
        node_id="test-node-1",
        host="127.0.0.1",
        port=5001,
        peers=[],
    )


@pytest.fixture
def three_node_config():
    """Config for node-1 in a 3-node cluster."""
    return RaftConfig(
        node_id="test-node-1",
        host="127.0.0.1",
        port=5001,
        peers=["test-node-2:5002", "test-node-3:5003"],
    )


@pytest.fixture
def five_node_config():
    """Config for node-1 in a 5-node cluster."""
    return RaftConfig(
        node_id="test-node-1",
        host="127.0.0.1",
        port=5001,
        peers=[
            "test-node-2:5002",
            "test-node-3:5003",
            "test-node-4:5004",
            "test-node-5:5005",
        ],
    )
