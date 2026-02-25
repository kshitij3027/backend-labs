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


class TestRegistration:
    def test_register_schema(self, client):
        resp = client.post("/schemas", json={
            "subject": "user-events",
            "schema": {"type": "object", "properties": {"name": {"type": "string"}}},
        })
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["subject"] == "user-events"
        assert data["data"]["version"] == 1

    def test_register_dedup_returns_200(self, client):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        client.post("/schemas", json={"subject": "dedup-test", "schema": schema})
        resp = client.post("/schemas", json={"subject": "dedup-test", "schema": schema})
        assert resp.status_code == 200

    def test_register_missing_fields(self, client):
        resp = client.post("/schemas", json={"subject": "test"})
        assert resp.status_code == 400

    def test_register_invalid_schema_type(self, client):
        resp = client.post("/schemas", json={
            "subject": "test",
            "schema": {"type": "object"},
            "schema_type": "xml",
        })
        assert resp.status_code == 400


class TestRetrieval:
    def _register(self, client, subject, schema):
        return client.post("/schemas", json={"subject": subject, "schema": schema})

    def test_list_subjects(self, client):
        self._register(client, "b-subject", {"type": "object"})
        self._register(client, "a-subject", {"type": "object"})
        resp = client.get("/schemas/subjects")
        data = resp.get_json()["data"]
        assert data == ["a-subject", "b-subject"]

    def test_get_latest(self, client):
        self._register(client, "s", {"type": "object", "properties": {"a": {"type": "string"}}})
        self._register(client, "s", {"type": "object", "properties": {"b": {"type": "string"}}})
        resp = client.get("/schemas/subjects/s")
        assert resp.get_json()["data"]["version"] == 2

    def test_get_latest_404(self, client):
        resp = client.get("/schemas/subjects/nonexistent")
        assert resp.status_code == 404

    def test_list_versions(self, client):
        self._register(client, "v", {"type": "object", "properties": {"x": {"type": "string"}}})
        self._register(client, "v", {"type": "object", "properties": {"y": {"type": "string"}}})
        resp = client.get("/schemas/subjects/v/versions")
        assert resp.get_json()["data"] == [1, 2]

    def test_get_specific_version(self, client):
        self._register(client, "v", {"type": "object", "properties": {"x": {"type": "string"}}})
        self._register(client, "v", {"type": "object", "properties": {"y": {"type": "string"}}})
        resp = client.get("/schemas/subjects/v/versions/1")
        assert resp.get_json()["data"]["version"] == 1

    def test_get_version_404(self, client):
        self._register(client, "v", {"type": "object"})
        resp = client.get("/schemas/subjects/v/versions/99")
        assert resp.status_code == 404
