import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch

from app.main import app
from app.coordinator import QuorumCoordinator, NodeConnection
from app.models import QuorumConfig, ConsistencyLevel
from app.metrics import QuorumMetrics


def make_response(status_code=200, json_data=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


@pytest.fixture
def mock_coordinator():
    config = QuorumConfig()
    metrics = QuorumMetrics()
    nodes = [NodeConnection(node_id=f"node-{i+1}", base_url=f"http://node-{i+1}:8001") for i in range(5)]
    coord = QuorumCoordinator(nodes, config, metrics)
    coord.client = AsyncMock()
    return coord, config, metrics


@pytest.fixture
async def client(mock_coordinator):
    coord, config, metrics = mock_coordinator

    import app.main as main_module
    main_module.coordinator = coord
    main_module.config = config
    main_module.metrics = metrics

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestCoordinatorAPI:
    async def test_health_endpoint(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"

    async def test_dashboard_returns_html(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    async def test_write_log(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coord.client.post = AsyncMock(return_value=mock_resp)

        resp = await client.post("/api/logs", json={"key": "k1", "value": "v1"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True

    async def test_read_log(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        entry = {"key": "k1", "value": "v1", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        mock_resp = make_response(200, entry)
        coord.client.get = AsyncMock(return_value=mock_resp)

        resp = await client.get("/api/logs/k1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["value"] == "v1"

    async def test_list_keys(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        mock_resp = make_response(200, {"keys": ["k1", "k2"]})
        coord.client.get = AsyncMock(return_value=mock_resp)

        resp = await client.get("/api/logs")
        assert resp.status_code == 200
        data = resp.json()
        assert "keys" in data

    async def test_update_consistency(self, client):
        resp = await client.post("/api/cluster/config", json={"level": "strong"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["consistency_level"] == "strong"
        assert data["read_quorum"] == 5
        assert data["write_quorum"] == 5

    async def test_update_consistency_invalid(self, client):
        resp = await client.post("/api/cluster/config", json={"level": "invalid"})
        assert resp.status_code == 400

    async def test_cluster_status(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        health_resp = make_response(200, {"node_id": "node-1", "is_healthy": True, "keys_count": 0})
        coord.client.get = AsyncMock(return_value=health_resp)

        resp = await client.get("/api/cluster/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "config" in data
        assert "nodes" in data
        assert "metrics" in data

    async def test_fail_node(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        mock_resp = make_response(200, {"node_id": "node-1", "is_healthy": False})
        coord.client.post = AsyncMock(return_value=mock_resp)

        resp = await client.post("/api/nodes/node-1/fail")
        assert resp.status_code == 200

    async def test_recover_node(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        mock_resp = make_response(200, {"node_id": "node-1", "is_healthy": True})
        coord.client.post = AsyncMock(return_value=mock_resp)

        resp = await client.post("/api/nodes/node-1/recover")
        assert resp.status_code == 200

    async def test_node_data(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        mock_resp = make_response(200, {"k1": {"key": "k1", "value": "v1"}})
        coord.client.get = AsyncMock(return_value=mock_resp)

        resp = await client.get("/api/nodes/node-1/data")
        assert resp.status_code == 200

    async def test_metrics_endpoint(self, client):
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_reads" in data
        assert "total_writes" in data

    async def test_write_alias(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coord.client.post = AsyncMock(return_value=mock_resp)

        resp = await client.post("/write", json={"key": "k1", "value": "v1"})
        assert resp.status_code == 200

    async def test_read_alias(self, client, mock_coordinator):
        coord = mock_coordinator[0]
        entry = {"key": "k1", "value": "v1", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        mock_resp = make_response(200, entry)
        coord.client.get = AsyncMock(return_value=mock_resp)

        resp = await client.post("/read", json={"key": "k1"})
        assert resp.status_code == 200
