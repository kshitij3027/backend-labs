import json
import os
import tempfile
import uuid

import pytest

from src.config import settings


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
class TestTaskFailedEndpoint:
    async def test_task_failed_resets_to_pending_on_first_failure(self, test_client):
        """POST /tasks/{id}/failed resets task to PENDING on first failure."""
        from src.db import pool

        job_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 2, 2)""",
                job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, retry_count)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'some-worker', 0, 0, 10, 0)""",
                task_id, job_id,
            )

        resp = await test_client.post(f"/tasks/{task_id}/failed")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "PENDING"
        assert data["retry_count"] == 1

        # Verify in DB
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)
        assert row["status"] == "PENDING"
        assert row["worker_id"] is None
        assert row["retry_count"] == 1

    async def test_task_failed_marks_failed_after_max_retries(self, test_client):
        """POST /tasks/{id}/failed marks FAILED when retries exhausted."""
        from src.db import pool

        job_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 2, 2)""",
                job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, retry_count)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'some-worker', 0, 0, 10, $3)""",
                task_id, job_id, settings.MAX_RETRIES - 1,
            )

        resp = await test_client.post(f"/tasks/{task_id}/failed")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "FAILED"
        assert data["retry_count"] == settings.MAX_RETRIES


@pytest.mark.asyncio(loop_scope="session")
class TestRecovery:
    async def test_recovery_resets_stale_running_tasks(self, setup_services):
        """Create a job with stale RUNNING tasks, call recovery, verify tasks reset."""
        from src.db import pool
        from src.coordinator.recovery import on_startup_recovery

        job_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 2, 2)""",
                job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, retry_count)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'dead-worker-recovery', 0, 0, 10, 0)""",
                task_id, job_id,
            )

        await on_startup_recovery()

        # Verify task was reset to PENDING
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task_id)
        assert row["status"] == "PENDING"
        assert row["worker_id"] is None


@pytest.mark.asyncio(loop_scope="session")
class TestWorkers:
    async def test_list_workers_empty(self, test_client):
        resp = await test_client.get("/workers")
        assert resp.status_code == 200
        assert resp.json() == []
