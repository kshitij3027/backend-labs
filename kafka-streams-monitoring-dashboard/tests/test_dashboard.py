"""Tests for src.dashboard module."""

import json


class TestHealthEndpoint:
    """Verify the /health endpoint."""

    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_healthy_status(self, client):
        response = client.get("/health")
        data = json.loads(response.data)
        assert data["status"] == "healthy"
        assert data["service"] == "kafka-streams-monitoring-dashboard"


class TestIndexEndpoint:
    """Verify the / endpoint."""

    def test_index_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_index_contains_title(self, client):
        response = client.get("/")
        assert b"Kafka Streams Monitoring Dashboard" in response.data


class TestApiMetricsEndpoint:
    """Verify the /api/metrics endpoint."""

    def test_api_metrics_returns_json(self, client):
        response = client.get("/api/metrics")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "total_events" in data
        assert "per_topic_counts" in data
        assert "error_rate" in data
        assert "avg_response_time" in data
        assert "p95_response_time" in data
        assert "events_per_second" in data


class TestApiHistoricalEndpoint:
    """Verify the /api/historical endpoint."""

    def test_api_historical_returns_json(self, client):
        response = client.get("/api/historical")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "labels" in data
        assert "events" in data
        assert "error_rate" in data
        assert "response_times" in data


class TestApiAlertsEndpoint:
    """Verify the /api/alerts endpoint."""

    def test_api_alerts_returns_json(self, client):
        response = client.get("/api/alerts")
        assert response.status_code == 200
        data = response.get_json()
        assert "active" in data
        assert "history" in data
