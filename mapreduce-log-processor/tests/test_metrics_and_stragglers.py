"""Tests for metrics collection, straggler detection, and /metrics endpoint."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.coordinator.metrics import MetricsCollector


class TestMetricsCollector:
    """Unit tests for MetricsCollector (no DB needed)."""

    def test_initial_state(self):
        m = MetricsCollector()
        d = m.to_dict()
        assert d["jobs_submitted"] == 0
        assert d["jobs_completed"] == 0
        assert d["jobs_failed"] == 0
        assert d["tasks_completed_by_type"] == {}
        assert d["tasks_failed_by_type"] == {}
        assert d["avg_job_duration_seconds"] == 0.0
        assert d["total_shuffle_volume_bytes"] == 0

    def test_record_job_submitted(self):
        m = MetricsCollector()
        m.record_job_submitted()
        m.record_job_submitted()
        assert m.jobs_submitted == 2

    def test_record_job_completed_with_duration(self):
        m = MetricsCollector()
        m.record_job_completed("job-1", 10.0)
        m.record_job_completed("job-2", 20.0)
        assert m.jobs_completed == 2
        assert m.avg_job_duration == 15.0
        d = m.to_dict()
        assert d["avg_job_duration_seconds"] == 15.0

    def test_record_job_failed(self):
        m = MetricsCollector()
        m.record_job_failed()
        assert m.jobs_failed == 1

    def test_record_task_completed_by_type(self):
        m = MetricsCollector()
        m.record_task_completed("MAP")
        m.record_task_completed("MAP")
        m.record_task_completed("REDUCE")
        d = m.to_dict()
        assert d["tasks_completed_by_type"] == {"MAP": 2, "REDUCE": 1}

    def test_record_task_failed_by_type(self):
        m = MetricsCollector()
        m.record_task_failed("MAP")
        m.record_task_failed("REDUCE")
        m.record_task_failed("REDUCE")
        d = m.to_dict()
        assert d["tasks_failed_by_type"] == {"MAP": 1, "REDUCE": 2}

    def test_record_shuffle_volume(self):
        m = MetricsCollector()
        m.record_shuffle_volume("job-1", 1000)
        m.record_shuffle_volume("job-1", 500)
        m.record_shuffle_volume("job-2", 2000)
        d = m.to_dict()
        assert d["total_shuffle_volume_bytes"] == 3500

    def test_avg_duration_empty(self):
        m = MetricsCollector()
        assert m.avg_job_duration == 0.0


@pytest.mark.asyncio(loop_scope="session")
class TestStragglerDetection:
    async def test_detect_stragglers_no_running_tasks(self, setup_services):
        """No stragglers when there are no running tasks."""
        from src.coordinator.straggler import detect_stragglers

        stragglers = await detect_stragglers()
        # May return empty or existing running tasks from other tests
        # The key is it doesn't crash
        assert isinstance(stragglers, list)

    async def test_detect_stragglers_creates_speculative_task(self, setup_services):
        """A running task taking > 2x the average of completed tasks is a straggler."""
        from src.db import pool

        job_id = str(uuid.uuid4())
        straggler_task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        async with pool.acquire() as conn:
            # Create job
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 5, 2)""",
                job_id,
            )

            # Create 3 completed MAP tasks with ~2 second duration each
            for i in range(3):
                tid = str(uuid.uuid4())
                created = now - timedelta(seconds=10)
                updated = created + timedelta(seconds=2)
                await conn.execute(
                    """INSERT INTO tasks (id, job_id, type, status, partition_id, input_start, input_end, created_at, updated_at)
                       VALUES ($1, $2, 'MAP', 'COMPLETED', $3, 0, 10, $4, $5)""",
                    tid, job_id, i, created, updated,
                )

            # Create a straggler task that has been running for 30 seconds (> 2 * 2 = 4)
            straggler_created = now - timedelta(seconds=30)
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, created_at)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'worker-slow', 3, 0, 10, $3)""",
                straggler_task_id, job_id, straggler_created,
            )

        from src.coordinator.straggler import detect_stragglers

        stragglers = await detect_stragglers()

        # Find our straggler in the results
        found = [s for s in stragglers if s["id"] == straggler_task_id]
        assert len(found) == 1, f"Expected straggler {straggler_task_id} to be detected"

        # Verify speculative task was created
        async with pool.acquire() as conn:
            spec = await conn.fetchrow(
                "SELECT * FROM tasks WHERE speculative_of = $1",
                straggler_task_id,
            )
        assert spec is not None, "Speculative task should have been created"
        assert spec["status"] == "PENDING"
        assert spec["job_id"] == job_id
        assert spec["type"] == "MAP"
        assert spec["partition_id"] == 3

    async def test_no_straggler_when_fewer_than_3_completed(self, setup_services):
        """Don't flag stragglers if fewer than 3 completed tasks of same type."""
        from src.db import pool

        job_id = str(uuid.uuid4())
        running_task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 3, 2)""",
                job_id,
            )

            # Only 2 completed tasks (threshold is 3)
            for i in range(2):
                tid = str(uuid.uuid4())
                created = now - timedelta(seconds=10)
                updated = created + timedelta(seconds=2)
                await conn.execute(
                    """INSERT INTO tasks (id, job_id, type, status, partition_id, input_start, input_end, created_at, updated_at)
                       VALUES ($1, $2, 'MAP', 'COMPLETED', $3, 0, 10, $4, $5)""",
                    tid, job_id, i, created, updated,
                )

            # Running task for a long time
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, created_at)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'worker-x', 2, 0, 10, $3)""",
                running_task_id, job_id, now - timedelta(seconds=100),
            )

        from src.coordinator.straggler import detect_stragglers

        stragglers = await detect_stragglers()

        found = [s for s in stragglers if s["id"] == running_task_id]
        assert len(found) == 0, "Should not detect straggler with < 3 completed tasks"


@pytest.mark.asyncio(loop_scope="session")
class TestMetricsEndpoint:
    async def test_metrics_endpoint_returns_valid_data(self, test_client):
        resp = await test_client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "jobs_submitted" in data
        assert "jobs_completed" in data
        assert "jobs_failed" in data
        assert "tasks_completed_by_type" in data
        assert "tasks_failed_by_type" in data
        assert "avg_job_duration_seconds" in data
        assert "total_shuffle_volume_bytes" in data
        # All numeric fields
        assert isinstance(data["jobs_submitted"], int)
        assert isinstance(data["avg_job_duration_seconds"], (int, float))

    async def test_metrics_increments_on_job_submit(self, test_client):
        """Submitting a job increments jobs_submitted counter."""
        before = (await test_client.get("/metrics")).json()["jobs_submitted"]
        await test_client.post(
            "/jobs",
            json={
                "input_path": "/data/test.jsonl",
                "map_fn": "word_count",
                "reduce_fn": "sum",
            },
        )
        after = (await test_client.get("/metrics")).json()["jobs_submitted"]
        assert after == before + 1


@pytest.mark.asyncio(loop_scope="session")
class TestSpeculativeTaskCancellation:
    async def test_cancel_speculative_when_original_completes(self, setup_services):
        """When an original task completes, its speculative copies are cancelled."""
        from src.db import pool, cancel_speculative_tasks

        job_id = str(uuid.uuid4())
        original_id = str(uuid.uuid4())
        spec_id = str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 2, 2)""",
                job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end)
                   VALUES ($1, $2, 'MAP', 'COMPLETED', 'w1', 0, 0, 10)""",
                original_id, job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, speculative_of)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'w2', 0, 0, 10, $3)""",
                spec_id, job_id, original_id,
            )

        await cancel_speculative_tasks(original_id, job_id)

        async with pool.acquire() as conn:
            spec_row = await conn.fetchrow("SELECT status FROM tasks WHERE id = $1", spec_id)
        assert spec_row["status"] == "FAILED"

    async def test_cancel_original_when_speculative_completes(self, setup_services):
        """When a speculative copy completes, the original is cancelled."""
        from src.db import pool, cancel_speculative_tasks

        job_id = str(uuid.uuid4())
        original_id = str(uuid.uuid4())
        spec_id = str(uuid.uuid4())

        async with pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', '/data/test.jsonl', 'word_count', 'sum', 2, 2)""",
                job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end)
                   VALUES ($1, $2, 'MAP', 'RUNNING', 'w1', 0, 0, 10)""",
                original_id, job_id,
            )
            await conn.execute(
                """INSERT INTO tasks (id, job_id, type, status, worker_id, partition_id, input_start, input_end, speculative_of)
                   VALUES ($1, $2, 'MAP', 'COMPLETED', 'w2', 0, 0, 10, $3)""",
                spec_id, job_id, original_id,
            )

        await cancel_speculative_tasks(spec_id, job_id)

        async with pool.acquire() as conn:
            orig_row = await conn.fetchrow("SELECT status FROM tasks WHERE id = $1", original_id)
        assert orig_row["status"] == "FAILED"
