import json
import os
import tempfile

import pytest


@pytest.fixture
def sample_log_path():
    """Create a temporary log file for coordinator tests."""
    lines = [json.dumps({"timestamp": "2025-01-15T08:00:00Z", "level": "INFO", "message": f"line {i}"}) for i in range(10)]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write("\n".join(lines) + "\n")
        path = f.name
    yield path
    os.unlink(path)


@pytest.mark.asyncio(loop_scope="session")
class TestHealth:
    async def test_health_returns_ok(self, test_client):
        resp = await test_client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio(loop_scope="session")
class TestJobs:
    async def test_create_job(self, test_client, sample_log_path):
        resp = await test_client.post(
            "/jobs",
            json={
                "input_path": sample_log_path,
                "map_fn": "word_count",
                "reduce_fn": "sum",
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "id" in data
        assert data["status"] == "MAPPING"
        assert data["input_path"] == sample_log_path
        assert data["map_fn"] == "word_count"
        assert data["reduce_fn"] == "sum"

    async def test_list_jobs(self, test_client):
        resp = await test_client.get("/jobs")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    async def test_get_job_by_id(self, test_client):
        # Create a job first
        create_resp = await test_client.post(
            "/jobs",
            json={
                "input_path": "/data/test.jsonl",
                "map_fn": "extract_errors",
                "reduce_fn": "count",
            },
        )
        job_id = create_resp.json()["id"]

        # Retrieve it
        resp = await test_client.get(f"/jobs/{job_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == job_id
        assert data["map_fn"] == "extract_errors"

    async def test_get_nonexistent_job(self, test_client):
        resp = await test_client.get("/jobs/nonexistent-id-12345")
        assert resp.status_code == 404

    async def test_get_job_result(self, test_client):
        # Create a job first
        create_resp = await test_client.post(
            "/jobs",
            json={
                "input_path": "/data/test.jsonl",
                "map_fn": "word_count",
                "reduce_fn": "sum",
            },
        )
        job_id = create_resp.json()["id"]

        resp = await test_client.get(f"/jobs/{job_id}/result")
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == job_id
        assert data["results"] == []

    async def test_cancel_job(self, test_client):
        create_resp = await test_client.post(
            "/jobs",
            json={
                "input_path": "/data/test.jsonl",
                "map_fn": "word_count",
                "reduce_fn": "sum",
            },
        )
        job_id = create_resp.json()["id"]

        resp = await test_client.delete(f"/jobs/{job_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "CANCELLED"

    async def test_get_result_nonexistent_job(self, test_client):
        resp = await test_client.get("/jobs/nonexistent-id-12345/result")
        assert resp.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
class TestWorkers:
    async def test_list_workers_empty(self, test_client):
        resp = await test_client.get("/workers")
        assert resp.status_code == 200
        assert resp.json() == []
