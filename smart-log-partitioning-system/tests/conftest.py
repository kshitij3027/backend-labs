"""Shared test fixtures for the Smart Log Partitioning System."""

import pytest

from src.config import PartitionConfig
from src.router import PartitionRouter


@pytest.fixture
def source_config():
    return PartitionConfig(strategy="source", num_nodes=3)


@pytest.fixture
def time_config():
    return PartitionConfig(strategy="time", time_bucket_hours=1)


@pytest.fixture
def hybrid_config():
    return PartitionConfig(strategy="hybrid", num_nodes=3, time_bucket_hours=1)


@pytest.fixture
def source_router(source_config):
    return PartitionRouter(source_config)


@pytest.fixture
def time_router(time_config):
    return PartitionRouter(time_config)


@pytest.fixture
def hybrid_router(hybrid_config):
    return PartitionRouter(hybrid_config)
