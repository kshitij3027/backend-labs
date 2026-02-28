"""Tests for the Flask storage node REST API."""

import pytest

from src.config import ClusterConfig
from src.storage_node import create_app


@pytest.fixture
def app_client(tmp_path):
    """Provide a Flask test client backed by a temporary storage directory."""
    config = ClusterConfig(
        node_id="test-node",
        port=5099,
        storage_dir=str(tmp_path / "data"),
    )
    app = create_app(config)
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


class TestHealthEndpoint:
    """Verify the /health endpoint."""

    def test_health_endpoint(self, app_client):
        resp = app_client.get("/health")

        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "healthy"
        assert body["node_id"] == "test-node"
        assert body["port"] == 5099


class TestWriteAndRead:
    """Verify write/read round-trip through the REST API."""

    def test_write_and_read_roundtrip(self, app_client):
        write_resp = app_client.post(
            "/write",
            json={"message": "hello", "level": "info"},
        )

        assert write_resp.status_code == 201
        write_body = write_resp.get_json()
        assert "file_path" in write_body

        read_resp = app_client.get(f"/read/{write_body['file_path']}")

        assert read_resp.status_code == 200
        read_body = read_resp.get_json()
        assert read_body["data"]["message"] == "hello"
        assert read_body["metadata"]["checksum"] == write_body["checksum"]

    def test_write_no_body_returns_400(self, app_client):
        resp = app_client.post("/write")

        assert resp.status_code in (400, 415)
        body = resp.get_json()
        if body is not None:
            assert "error" in body

    def test_read_nonexistent_returns_404(self, app_client):
        resp = app_client.get("/read/nonexistent_file.json")

        assert resp.status_code == 404
        assert "error" in resp.get_json()


class TestListFilesEndpoint:
    """Verify the /files endpoint."""

    def test_list_files_endpoint(self, app_client):
        app_client.post("/write", json={"a": 1})
        app_client.post("/write", json={"b": 2})

        resp = app_client.get("/files")

        assert resp.status_code == 200
        body = resp.get_json()
        assert body["count"] == 2
        assert len(body["files"]) == 2


class TestReplicateEndpoint:
    """Verify the /replicate endpoint."""

    def test_replicate_endpoint(self, app_client):
        payload = {
            "file_path": "replica_test.json",
            "data": {"msg": "replicated"},
            "metadata": {"version": 1, "node_id": "other-node"},
        }
        resp = app_client.post("/replicate", json=payload)

        assert resp.status_code == 201
        body = resp.get_json()
        assert body["status"] == "replicated"

        read_resp = app_client.get("/read/replica_test.json")
        assert read_resp.status_code == 200
        assert read_resp.get_json()["data"]["msg"] == "replicated"


class TestStatsEndpoint:
    """Verify the /stats endpoint."""

    def test_stats_endpoint(self, app_client):
        app_client.post("/write", json={"x": 1})

        resp = app_client.get("/stats")

        assert resp.status_code == 200
        body = resp.get_json()
        assert body["node_id"] == "test-node"
        assert body["stats"]["writes"] == 1
        assert body["files_count"] == 1
