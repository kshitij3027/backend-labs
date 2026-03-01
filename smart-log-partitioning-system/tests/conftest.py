"""Shared test fixtures for the Smart Log Partitioning System."""

import pytest

from src.config import PartitionConfig
from src.manager import PartitionManager
from src.optimizer import QueryOptimizer
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


@pytest.fixture
def source_manager(tmp_path, source_config):
    config = PartitionConfig(strategy="source", num_nodes=3, data_dir=str(tmp_path))
    return PartitionManager(config)


@pytest.fixture
def source_optimizer(source_router, source_manager):
    return QueryOptimizer(source_router, source_manager)
