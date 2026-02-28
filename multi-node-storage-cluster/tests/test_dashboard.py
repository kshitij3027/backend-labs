"""Tests for the monitoring dashboard."""

from unittest.mock import MagicMock, patch

import pytest

from src.dashboard import create_dashboard_app


@pytest.fixture
def dashboard_client():
    cluster_nodes = [
        {"id": "node1", "host": "localhost", "port": 5001},
        {"id": "node2", "host": "localhost", "port": 5002},
    ]
    app = create_dashboard_app(cluster_nodes=cluster_nodes)
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


def test_index_returns_html(dashboard_client):
    """GET / returns 200 with HTML content."""
    resp = dashboard_client.get("/")
    assert resp.status_code == 200
    assert b"Storage Cluster Dashboard" in resp.data
    assert resp.content_type.startswith("text/html")


def test_api_health(dashboard_client):
    """GET /api/health returns 200 with status."""
    resp = dashboard_client.get("/api/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "dashboard running"


@patch("src.dashboard.requests.get")
def test_api_cluster_polls_nodes(mock_get, dashboard_client):
    """GET /api/cluster polls nodes and returns structured response."""

    def mock_response(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        if "/health" in url:
            mock_resp.json.return_value = {
                "status": "healthy",
                "node_id": "node1",
                "port": 5001,
            }
        elif "/stats" in url:
            mock_resp.json.return_value = {
                "node_id": "node1",
                "stats": {"writes": 5, "reads": 3, "replications_received": 2},
                "files_count": 4,
            }
        elif "/replication/status" in url:
            mock_resp.json.return_value = {
                "replications_sent": 10,
                "replications_failed": 0,
                "hints_queued": 1,
                "hints_replayed": 1,
                "hints_pending": 0,
            }
        elif "/files" in url:
            mock_resp.json.return_value = {
                "files": ["log_001.json", "log_002.json"],
                "count": 2,
            }
        return mock_resp

    mock_get.side_effect = mock_response

    resp = dashboard_client.get("/api/cluster")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["healthy_count"] == 2
    assert data["total_count"] == 2
    assert data["quorum"] is True
    assert len(data["nodes"]) == 2
    assert data["nodes"][0]["status"] == "healthy"
    assert data["nodes"][0]["stats"]["stats"]["writes"] == 5


@patch("src.dashboard.requests.get")
def test_api_cluster_handles_unreachable_nodes(mock_get, dashboard_client):
    """Unreachable nodes are marked unhealthy."""
    import requests as real_requests

    mock_get.side_effect = real_requests.ConnectionError("Connection refused")

    resp = dashboard_client.get("/api/cluster")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["healthy_count"] == 0
    assert data["quorum"] is False
    for node in data["nodes"]:
        assert node["status"] == "unhealthy"
        assert node["health"]["error"] == "unreachable"
        assert node["stats"] == {}
        assert node["files"] == {"files": [], "count": 0}
        assert node["replication"] == {}


@patch("src.dashboard.requests.get")
def test_api_files_aggregates(mock_get, dashboard_client):
    """GET /api/files aggregates files from all nodes."""

    def mock_response(url, **kwargs):
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        if "5001" in url:
            mock_resp.json.return_value = {
                "files": ["log_001.json", "log_002.json"],
                "count": 2,
            }
        elif "5002" in url:
            mock_resp.json.return_value = {
                "files": ["log_001.json", "log_003.json"],
                "count": 2,
            }
        return mock_resp

    mock_get.side_effect = mock_response

    resp = dashboard_client.get("/api/files")
    assert resp.status_code == 200
    data = resp.get_json()

    assert data["total"] == 3

    files_map = {f["path"]: f["replicas"] for f in data["files"]}
    assert "log_001.json" in files_map
    assert set(files_map["log_001.json"]) == {"node1", "node2"}
    assert files_map["log_002.json"] == ["node1"]
    assert files_map["log_003.json"] == ["node2"]
