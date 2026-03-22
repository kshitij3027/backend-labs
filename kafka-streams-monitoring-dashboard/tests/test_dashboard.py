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
