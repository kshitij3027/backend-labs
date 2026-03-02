"""Tests for the Flask application API routes."""

import json
import pytest


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_endpoint(self, app_client):
        resp = app_client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert "cluster_name" in data
        assert data["cluster_name"] == "test-cluster"
        assert "node_count" in data
        assert data["node_count"] == 3
        assert "total_logs" in data


class TestDashboard:
    """Tests for GET /."""

    def test_dashboard_returns_html(self, app_client):
        resp = app_client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.content_type


class TestStoreLogs:
    """Tests for POST /api/logs."""

    def test_store_single_log(self, app_client):
        resp = app_client.post(
            "/api/logs",
            data=json.dumps({"source": "web-server", "message": "test log"}),
            content_type="application/json",
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["stored"] == 1
        assert len(data["details"]) == 1
        assert "node_id" in data["details"][0]
        assert "log_key" in data["details"][0]

    def test_store_batch_logs(self, app_client):
        entries = [
            {"source": "web-server", "message": "log 1"},
            {"source": "api-gateway", "message": "log 2"},
            {"source": "auth-service", "message": "log 3"},
        ]
        resp = app_client.post(
            "/api/logs",
            data=json.dumps(entries),
            content_type="application/json",
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["stored"] == 3
        assert len(data["details"]) == 3

    def test_store_log_no_body(self, app_client):
        resp = app_client.post(
            "/api/logs",
            data=json.dumps(None),
            content_type="application/json",
        )
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data

    def test_store_log_missing_source(self, app_client):
        resp = app_client.post(
            "/api/logs",
            data=json.dumps({"message": "no source field"}),
            content_type="application/json",
        )
        assert resp.status_code == 400
        data = resp.get_json()
        assert "source" in data["error"].lower()


class TestStats:
    """Tests for GET /api/stats."""

    def test_get_stats(self, app_client):
        resp = app_client.get("/api/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "total_logs" in data
        assert "node_count" in data
        assert "nodes" in data
        assert "ring_metrics" in data
        assert "balance_variance" in data


class TestNodeManagement:
    """Tests for POST /api/nodes and DELETE /api/nodes/<node_id>."""

    def test_add_node(self, app_client):
        resp = app_client.post(
            "/api/nodes",
            data=json.dumps({"node_id": "node4"}),
            content_type="application/json",
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["node_id"] == "node4"
        assert "ring_update" in data
        assert "logs_migrated" in data

    def test_add_duplicate_node(self, app_client):
        resp = app_client.post(
            "/api/nodes",
            data=json.dumps({"node_id": "node1"}),
            content_type="application/json",
        )
        assert resp.status_code == 409
        data = resp.get_json()
        assert "error" in data

    def test_remove_node(self, app_client):
        # Store some logs first so there is data to migrate
        app_client.post(
            "/api/logs",
            data=json.dumps([
                {"source": "web-server", "message": f"log {i}"}
                for i in range(20)
            ]),
            content_type="application/json",
        )

        # Add node4
        app_client.post(
            "/api/nodes",
            data=json.dumps({"node_id": "node4"}),
            content_type="application/json",
        )

        # Remove node4
        resp = app_client.delete("/api/nodes/node4")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["node_id"] == "node4"
        assert "logs_migrated" in data

    def test_remove_nonexistent_node(self, app_client):
        resp = app_client.delete("/api/nodes/nonexistent")
        assert resp.status_code == 404
        data = resp.get_json()
        assert "error" in data


class TestRingInfo:
    """Tests for GET /api/ring."""

    def test_ring_info(self, app_client):
        resp = app_client.get("/api/ring")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "vnodes" in data
        assert "node_colors" in data
        assert "ring_metrics" in data
        assert isinstance(data["vnodes"], list)
        assert len(data["vnodes"]) > 0
        # Check vnode structure
        vn = data["vnodes"][0]
        assert "position_pct" in vn
        assert "node_id" in vn
        assert "color" in vn


class TestSimulate:
    """Tests for POST /api/simulate."""

    def test_simulate(self, app_client):
        resp = app_client.post(
            "/api/simulate",
            data=json.dumps({"count": 50}),
            content_type="application/json",
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["generated"] == 50
        assert "total_logs" in data
        assert "distribution" in data
        # Distribution should have entries for our 3 nodes
        assert len(data["distribution"]) == 3
