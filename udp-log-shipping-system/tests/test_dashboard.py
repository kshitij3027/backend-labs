"""Tests for the Flask monitoring dashboard."""

import json

import pytest

from src.dashboard import create_dashboard_app
from src.error_tracker import ErrorTracker
from src.metrics import Metrics


@pytest.fixture
def app():
    metrics = Metrics()
    error_tracker = ErrorTracker(max_size=100)

    metrics.increment("INFO")
    metrics.increment("INFO")
    metrics.increment("ERROR")
    error_tracker.add({"level": "ERROR", "message": "disk full", "sequence": 1})

    app = create_dashboard_app(metrics, error_tracker)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["status"] == "ok"


class TestStatsEndpoint:
    def test_stats_returns_metrics(self, client):
        resp = client.get("/stats")
        assert resp.status_code == 200
        data = json.loads(resp.data)

        assert data["total_received"] == 3
        assert data["level_distribution"]["INFO"] == 2
        assert data["level_distribution"]["ERROR"] == 1
        assert data["logs_per_second"] >= 0
        assert data["elapsed_seconds"] >= 0

    def test_stats_includes_recent_errors(self, client):
        resp = client.get("/stats")
        data = json.loads(resp.data)

        assert len(data["recent_errors"]) == 1
        assert data["recent_errors"][0]["message"] == "disk full"


class TestDashboardPage:
    def test_index_returns_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"UDP Log Server Dashboard" in resp.data
        assert b"Total Received" in resp.data
        assert b"Logs/sec" in resp.data
