import pytest
from unittest.mock import MagicMock, patch
from src.coordinator.server import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


class TestCoordinatorAPI:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert data["role"] == "coordinator"

    @patch("src.coordinator.server.clients")
    def test_api_status(self, mock_clients, client):
        mock_node = MagicMock()
        mock_node.node_id = "node-a"
        mock_node.node_url = "http://node-a:8001"
        mock_node.health.return_value = {"status": "healthy"}
        mock_clients.__iter__ = lambda self: iter([mock_node])

        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["coordinator"] == "healthy"

    @patch("src.coordinator.server.read_repair_handler")
    def test_api_read(self, mock_rr, client):
        mock_rr.read_with_repair.return_value = {
            "key": "test",
            "value": "val",
            "version": 1,
            "timestamp": 1.0,
        }
        resp = client.get("/api/data/test")
        assert resp.status_code == 200
        assert resp.get_json()["value"] == "val"

    @patch("src.coordinator.server.read_repair_handler")
    def test_api_read_not_found(self, mock_rr, client):
        mock_rr.read_with_repair.return_value = None
        resp = client.get("/api/data/nope")
        assert resp.status_code == 404

    @patch("src.coordinator.server.clients")
    def test_api_write(self, mock_clients, client):
        mock_node = MagicMock()
        mock_node.node_id = "node-a"
        mock_node.put_data.return_value = True
        mock_clients.__iter__ = lambda self: iter([mock_node])

        resp = client.put("/api/data/mykey", json={"value": "myval"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["key"] == "mykey"

    @patch("src.coordinator.server.run_scan_cycle")
    def test_api_trigger_scan(self, mock_scan, client):
        mock_scan.return_value = {
            "inconsistencies": 0,
            "repairs_completed": 0,
            "repairs_failed": 0,
            "duration": 0.01,
        }
        resp = client.post("/api/scan/trigger")
        assert resp.status_code == 200

    @patch("src.coordinator.server.metrics")
    def test_api_metrics(self, mock_metrics, client):
        mock_metrics.to_dict.return_value = {
            "comparisons": 5,
            "inconsistencies_detected": 2,
        }
        resp = client.get("/api/metrics")
        assert resp.status_code == 200
        assert resp.get_json()["comparisons"] == 5

    @patch("src.coordinator.server.clients")
    def test_api_inject(self, mock_clients, client):
        mock_node = MagicMock()
        mock_node.node_id = "node-a"
        mock_node.put_data.return_value = True
        mock_clients.__iter__ = lambda self: iter([mock_node])

        resp = client.post(
            "/api/inject", json={"node_id": "node-a", "key": "test", "value": "bad"}
        )
        assert resp.status_code == 200
        assert resp.get_json()["injected"] is True

    @patch("src.coordinator.server.clients")
    def test_api_inject_node_not_found(self, mock_clients, client):
        mock_node = MagicMock()
        mock_node.node_id = "node-a"
        mock_clients.__iter__ = lambda self: iter([mock_node])

        resp = client.post(
            "/api/inject",
            json={"node_id": "nonexistent", "key": "test", "value": "bad"},
        )
        assert resp.status_code == 404

    @patch("src.coordinator.server.clients")
    def test_api_replicas(self, mock_clients, client):
        mock_node = MagicMock()
        mock_node.node_id = "node-a"
        mock_node.node_url = "http://node-a:8001"
        mock_node.health.return_value = {"status": "healthy"}
        mock_clients.__iter__ = lambda self: iter([mock_node])

        resp = client.get("/api/replicas")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "replicas" in data
        assert len(data["replicas"]) == 1
        assert data["replicas"][0]["node_id"] == "node-a"
        assert data["replicas"][0]["healthy"] is True
