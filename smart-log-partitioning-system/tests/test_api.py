"""Integration tests for the Flask API endpoints."""

import json

import pytest

from src.app import create_app
from src.config import PartitionConfig


@pytest.fixture
def api_app(tmp_path):
    """Create a test Flask app with isolated data directory."""
    config = PartitionConfig(strategy="source", num_nodes=3, data_dir=str(tmp_path))
    app = create_app(config)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def api_client(api_app):
    """Create a test client from the test app."""
    return api_app.test_client()


class TestHealthEndpoint:
    def test_health_endpoint(self, api_client):
        """GET /health returns 200 with status healthy."""
        resp = api_client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert data["strategy"] == "source"
        assert data["num_nodes"] == 3


class TestIngestEndpoint:
    def test_ingest_single_entry(self, api_client):
        """POST /api/ingest with a single entry returns 201 with partition_id."""
        entry = {
            "source": "web_server",
            "level": "info",
            "message": "Request received",
            "timestamp": "2026-02-28T10:00:00",
        }
        resp = api_client.post(
            "/api/ingest", data=json.dumps(entry), content_type="application/json"
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["ingested"] == 1
        assert len(data["details"]) == 1
        assert "partition_id" in data["details"][0]
        assert data["details"][0]["source"] == "web_server"

    def test_ingest_batch(self, api_client):
        """POST /api/ingest with a list of entries returns 201 with correct count."""
        entries = [
            {"source": "web_server", "level": "info", "message": "req1", "timestamp": "2026-02-28T10:00:00"},
            {"source": "database", "level": "error", "message": "timeout", "timestamp": "2026-02-28T10:01:00"},
            {"source": "auth", "level": "warn", "message": "bad token", "timestamp": "2026-02-28T10:02:00"},
        ]
        resp = api_client.post(
            "/api/ingest", data=json.dumps(entries), content_type="application/json"
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["ingested"] == 3
        assert len(data["details"]) == 3

    def test_ingest_missing_source(self, api_client):
        """POST /api/ingest without a source field returns 400."""
        entry = {"level": "info", "message": "no source"}
        resp = api_client.post(
            "/api/ingest", data=json.dumps(entry), content_type="application/json"
        )
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data

    def test_ingest_no_body(self, api_client):
        """POST /api/ingest with empty JSON body returns 400."""
        resp = api_client.post(
            "/api/ingest", data="", content_type="application/json"
        )
        assert resp.status_code == 400


class TestQueryEndpoint:
    def _ingest_test_data(self, client):
        """Helper to ingest a set of test entries."""
        entries = [
            {"source": "web_server", "level": "info", "message": "req1", "timestamp": "2026-02-28T10:00:00"},
            {"source": "web_server", "level": "error", "message": "fail", "timestamp": "2026-02-28T10:05:00"},
            {"source": "database", "level": "warn", "message": "slow", "timestamp": "2026-02-28T11:00:00"},
            {"source": "auth", "level": "info", "message": "login", "timestamp": "2026-02-28T12:00:00"},
        ]
        client.post(
            "/api/ingest", data=json.dumps(entries), content_type="application/json"
        )

    def test_query_all(self, api_client):
        """Ingest entries then GET /api/query returns all with optimization info."""
        self._ingest_test_data(api_client)
        resp = api_client.get("/api/query")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 4
        assert len(data["results"]) == 4
        assert "optimization" in data

    def test_query_by_source(self, api_client):
        """Ingest mixed sources, GET /api/query?source=X returns filtered results."""
        self._ingest_test_data(api_client)
        resp = api_client.get("/api/query?source=web_server")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["count"] == 2
        for entry in data["results"]:
            assert entry["source"] == "web_server"

    def test_query_shows_optimization(self, api_client):
        """Query response includes optimization dict with improvement_factor."""
        self._ingest_test_data(api_client)
        resp = api_client.get("/api/query?source=web_server")
        assert resp.status_code == 200
        data = resp.get_json()
        opt = data["optimization"]
        assert "improvement_factor" in opt
        assert "partition_ids" in opt
        assert "total_partitions" in opt
        assert "partitions_scanned" in opt
        assert "pruned" in opt


class TestStatsEndpoint:
    def test_stats_endpoint(self, api_client):
        """GET /api/stats returns strategy, partitions info, and query_efficiency."""
        # Ingest some data first so stats are non-empty
        entries = [
            {"source": "web_server", "level": "info", "message": "test", "timestamp": "2026-02-28T10:00:00"},
            {"source": "database", "level": "error", "message": "test", "timestamp": "2026-02-28T11:00:00"},
        ]
        api_client.post(
            "/api/ingest", data=json.dumps(entries), content_type="application/json"
        )

        resp = api_client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["strategy"] == "source"
        assert data["num_nodes"] == 3
        assert "partitions" in data
        assert "total_entries" in data["partitions"]
        assert "query_efficiency" in data


class TestDashboard:
    def test_dashboard_returns_html(self, api_client):
        """GET / returns 200 with text/html content type."""
        resp = api_client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.content_type
