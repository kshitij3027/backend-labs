"""Shared fixtures for self-healing cluster membership tests."""

import pytest

from src.config import ClusterConfig
from src.registry import MembershipRegistry


@pytest.fixture
def default_config() -> ClusterConfig:
    """Return a basic single-node config for testing."""
    return ClusterConfig(
        node_id="test-node-1",
        port=5001,
    )


@pytest.fixture
def five_node_config() -> ClusterConfig:
    """Return a config with seed nodes for a 5-node cluster."""
    return ClusterConfig(
        node_id="test-node-1",
        port=5001,
        seed_nodes=[
            "test-node-2:5002",
            "test-node-3:5003",
            "test-node-4:5004",
            "test-node-5:5005",
        ],
    )


@pytest.fixture
def registry() -> MembershipRegistry:
    """Return a fresh, empty MembershipRegistry."""
    return MembershipRegistry()


@pytest.fixture
async def registered_registry(
    registry: MembershipRegistry,
    default_config: ClusterConfig,
) -> MembershipRegistry:
    """Return a registry with the default node already registered."""
    await registry.register_self(default_config)
    return registry
