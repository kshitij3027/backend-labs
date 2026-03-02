"""Shared test fixtures for consistent hashing tests."""
import pytest
from src.hash_ring import HashRing


@pytest.fixture
def empty_ring():
    return HashRing()


@pytest.fixture
def single_node_ring():
    return HashRing(nodes=["node1"])


@pytest.fixture
def three_node_ring():
    return HashRing(nodes=["node1", "node2", "node3"])
