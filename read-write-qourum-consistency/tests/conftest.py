import pytest
from app.models import ConsistencyLevel, QuorumConfig, VectorClock, LogEntry
from app.metrics import QuorumMetrics


@pytest.fixture
def quorum_config():
    return QuorumConfig()


@pytest.fixture
def vector_clock():
    return VectorClock()


@pytest.fixture
def metrics():
    return QuorumMetrics()
