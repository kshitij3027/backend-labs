"""Tests for the Flask HTTP API."""

import json
import pytest


class TestPostLogs:
    """Test the POST /logs endpoint."""

    def test_post_logs_single(self, client):
        """POST a single log dict returns 202 with accepted: 1."""
        response = client.post(
            "/logs",
            data=json.dumps({"level": "info", "message": "test", "source": "app"}),
            content_type="application/json",
        )
        assert response.status_code == 202
        body = response.get_json()
        assert body["accepted"] == 1

    def test_post_logs_batch(self, client):
        """POST a list of 3 log dicts returns 202 with accepted: 3."""
        logs = [
            {"level": "info", "message": "first", "source": "app"},
            {"level": "warn", "message": "second", "source": "worker"},
            {"level": "error", "message": "third", "source": "api"},
        ]
        response = client.post(
            "/logs",
            data=json.dumps(logs),
            content_type="application/json",
        )
        assert response.status_code == 202
        body = response.get_json()
        assert body["accepted"] == 3

    def test_post_logs_invalid_json(self, client):
        """POST with invalid JSON body returns 400."""
        response = client.post(
            "/logs",
            data="not valid json {{{",
            content_type="application/json",
        )
        assert response.status_code == 400
        body = response.get_json()
        assert "error" in body

    def test_post_logs_missing_fields(self, client):
        """POST with missing required fields returns 400 with error about missing fields."""
        response = client.post(
            "/logs",
            data=json.dumps({"level": "info"}),
            content_type="application/json",
        )
        assert response.status_code == 400
        body = response.get_json()
        assert "error" in body
        assert "Missing fields" in body["error"]

    def test_post_logs_empty_list(self, client):
        """POST an empty list returns 202 with accepted: 0."""
        response = client.post(
            "/logs",
            data=json.dumps([]),
            content_type="application/json",
        )
        assert response.status_code == 202
        body = response.get_json()
        assert body["accepted"] == 0


class TestHealthAndMetrics:
    """Test the health and metrics endpoints."""

    def test_health(self, client):
        """GET /health returns 200 with healthy and status keys."""
        response = client.get("/health")
        assert response.status_code == 200
        body = response.get_json()
        assert "healthy" in body
        assert "status" in body

    def test_metrics(self, client):
        """GET /metrics returns 200 with status key."""
        response = client.get("/metrics")
        assert response.status_code == 200
        body = response.get_json()
        assert "status" in body
