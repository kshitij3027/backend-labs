"""Tests for the Flask web UI."""
import json
import pytest
from src.web.app import create_app


@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


class TestWebApp:
    def test_health(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "healthy"

    def test_index(self, client):
        response = client.get("/")
        assert response.status_code == 200
        assert b"Log Format" in response.data

    def test_api_sample(self, client):
        response = client.get("/api/sample?type=mixed")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "results" in data
        assert "metrics" in data
        assert data["count"] >= 8

    def test_api_config(self, client):
        response = client.get("/api/config")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert "adapters" in data
        assert len(data["adapters"]) == 4

    def test_api_upload_text(self, client):
        log_text = '{"timestamp": "2024-01-15T10:30:00Z", "level": "ERROR", "message": "Test"}'
        response = client.post("/api/upload", data={
            "text": log_text,
            "format": "json",
        })
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["count"] >= 1
        assert data["results"][0]["message"] == "Test"

    def test_api_upload_empty(self, client):
        response = client.post("/api/upload", data={"text": ""})
        assert response.status_code == 400
