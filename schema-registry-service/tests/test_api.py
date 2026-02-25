"""Tests for API endpoints."""


class TestHealth:
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_returns_healthy(self, client):
        resp = client.get("/health")
        data = resp.get_json()
        assert data["status"] == "healthy"

    def test_health_includes_counts(self, client):
        resp = client.get("/health")
        data = resp.get_json()
        assert "schema_count" in data
        assert "subject_count" in data
        assert data["schema_count"] == 0
        assert data["subject_count"] == 0
