"""Tests for the Flask web interface and API endpoints."""

import json

import pytest

from src.config import AppConfig
from src.web.app import create_app


@pytest.fixture
def client():
    config = AppConfig()
    app = create_app(config)
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


class TestIndex:
    def test_index_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200


class TestHealth:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_reports_healthy(self, client):
        response = client.get("/health")
        data = json.loads(response.data)
        assert data["status"] == "healthy"

    def test_health_includes_service_and_version(self, client):
        response = client.get("/health")
        data = json.loads(response.data)
        assert "service" in data
        assert "version" in data


class TestEnrichEndpoint:
    def test_enrich_with_valid_json(self, client):
        response = client.post(
            "/api/enrich",
            data=json.dumps({"log_message": "INFO: test message", "source": "test"}),
            content_type="application/json",
        )
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "message" in data
        assert "source" in data
        assert "timestamp" in data
        assert "hostname" in data

    def test_enrich_with_empty_body(self, client):
        response = client.post(
            "/api/enrich",
            content_type="application/json",
        )
        assert response.status_code == 400

    def test_enrich_with_missing_log_message(self, client):
        response = client.post(
            "/api/enrich",
            data=json.dumps({"source": "test"}),
            content_type="application/json",
        )
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_enrich_error_message_content(self, client):
        response = client.post(
            "/api/enrich",
            data=json.dumps({"source": "test"}),
            content_type="application/json",
        )
        data = json.loads(response.data)
        assert data["error"] == "log_message is required"


class TestStatsEndpoint:
    def test_stats_returns_200(self, client):
        response = client.get("/api/stats")
        assert response.status_code == 200

    def test_stats_has_processed_count(self, client):
        response = client.get("/api/stats")
        data = json.loads(response.data)
        assert "processed_count" in data


class TestSampleLogsEndpoint:
    def test_sample_logs_returns_200(self, client):
        response = client.get("/api/sample-logs")
        assert response.status_code == 200

    def test_sample_logs_returns_five_items(self, client):
        response = client.get("/api/sample-logs")
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 5


class TestStatsIncrement:
    def test_stats_increment_after_enrich(self, client):
        client.post(
            "/api/enrich",
            data=json.dumps({"log_message": "ERROR: something broke"}),
            content_type="application/json",
        )
        response = client.get("/api/stats")
        data = json.loads(response.data)
        assert data["processed_count"] > 0
