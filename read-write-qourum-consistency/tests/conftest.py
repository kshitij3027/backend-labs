import pytest
import httpx
from app.models import ConsistencyLevel, QuorumConfig, VectorClock, LogEntry
from app.metrics import QuorumMetrics
from app.node_server import create_node_app
from app.coordinator import QuorumCoordinator, NodeConnection


@pytest.fixture
def quorum_config():
    return QuorumConfig()


@pytest.fixture
def vector_clock():
    return VectorClock()


@pytest.fixture
def metrics():
    return QuorumMetrics()


@pytest.fixture
def node_app():
    return create_node_app("test-node")


@pytest.fixture
async def node_client(node_app):
    transport = httpx.ASGITransport(app=node_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def coordinator():
    config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
    metrics = QuorumMetrics()
    nodes = [NodeConnection(node_id=f"node-{i+1}", base_url=f"http://node-{i+1}:8001") for i in range(5)]
    return QuorumCoordinator(nodes, config, metrics)
