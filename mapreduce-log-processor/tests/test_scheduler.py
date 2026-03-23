"""Tests for the coordinator scheduler logic."""

import json
import os
import tempfile

import pytest
import pytest_asyncio

from src.coordinator.scheduler import assign_task, complete_task, create_map_tasks, partition_input
import src.db as db


@pytest.fixture
def sample_log_file():
    """Create a temporary log file with known content for testing."""
    lines = []
    for i in range(100):
        lines.append(json.dumps({
            "timestamp": f"2025-01-15T08:00:{i:02d}Z",
            "level": "INFO",
            "message": f"Test log line {i}",
            "url": "/api/users",
            "error_code": None,
            "user_id": f"user_{i:03d}",
        }))

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write("\n".join(lines) + "\n")
        path = f.name

    yield path
    os.unlink(path)


@pytest_asyncio.fixture(loop_scope="session")
async def clean_db(setup_services):
    """Clean tasks and jobs tables before each test."""
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM tasks")
        await conn.execute("DELETE FROM jobs")
        await conn.execute("DELETE FROM workers")
    yield
    async with db.pool.acquire() as conn:
        await conn.execute("DELETE FROM tasks")
        await conn.execute("DELETE FROM jobs")
        await conn.execute("DELETE FROM workers")


@pytest.mark.asyncio(loop_scope="session")
class TestPartitionInput:
    """Tests for the partition_input function."""

    async def test_splits_correctly(self, sample_log_file):
        partitions = await partition_input(sample_log_file, num_mappers=4)
        assert len(partitions) == 4

        # First partition starts at 0
        assert partitions[0][0] == 0

        # Last partition ends at total lines
        assert partitions[-1][1] == 100

        # Partitions are contiguous
        for i in range(len(partitions) - 1):
            assert partitions[i][1] == partitions[i + 1][0]

    async def test_single_mapper(self, sample_log_file):
        partitions = await partition_input(sample_log_file, num_mappers=1)
        assert len(partitions) == 1
        assert partitions[0] == (0, 100)

    async def test_two_mappers(self, sample_log_file):
        partitions = await partition_input(sample_log_file, num_mappers=2)
        assert len(partitions) == 2
        assert partitions[0] == (0, 50)
        assert partitions[1] == (50, 100)

    async def test_covers_all_lines(self, sample_log_file):
        partitions = await partition_input(sample_log_file, num_mappers=3)
        total_covered = sum(end - start for start, end in partitions)
        assert total_covered == 100


@pytest.mark.asyncio(loop_scope="session")
class TestCreateMapTasks:
    """Tests for create_map_tasks function."""

    async def test_creates_correct_number(self, setup_services, clean_db, sample_log_file):
        # Create a job first
        import uuid
        job_id = str(uuid.uuid4())
        async with db.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'PENDING', $2, 'word_count', 'sum', 2, 2)""",
                job_id, sample_log_file,
            )

        tasks = await create_map_tasks(job_id, sample_log_file, num_mappers=3)

        assert len(tasks) == 3

        # Verify tasks in DB
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM tasks WHERE job_id = $1 ORDER BY partition_id",
                job_id,
            )
        assert len(rows) == 3
        for row in rows:
            assert row["type"] == "MAP"
            assert row["status"] == "PENDING"

    async def test_partition_ids_sequential(self, setup_services, clean_db, sample_log_file):
        import uuid
        job_id = str(uuid.uuid4())
        async with db.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'PENDING', $2, 'word_count', 'sum', 2, 2)""",
                job_id, sample_log_file,
            )

        tasks = await create_map_tasks(job_id, sample_log_file, num_mappers=2)
        partition_ids = [t["partition_id"] for t in tasks]
        assert partition_ids == [0, 1]


@pytest.mark.asyncio(loop_scope="session")
class TestAssignTask:
    """Tests for assign_task function."""

    async def test_assigns_pending_task(self, setup_services, clean_db, sample_log_file):
        import uuid
        job_id = str(uuid.uuid4())
        worker_id = "test-worker-1"

        async with db.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
                   VALUES ($1, 'MAPPING', $2, 'word_count', 'sum', 2, 2)""",
                job_id, sample_log_file,
            )
            await conn.execute(
                """INSERT INTO workers (id, status, last_heartbeat, tasks_completed)
                   VALUES ($1, 'ALIVE', NOW(), 0)""",
                worker_id,
            )

        await create_map_tasks(job_id, sample_log_file, num_mappers=2)

        task = await assign_task(worker_id)
        assert task is not None
        assert task["job_id"] == job_id
        assert task["type"] == "MAP"
        assert task["input_path"] == sample_log_file
        assert task["map_fn"] == "word_count"
        assert task["num_reducers"] == 2

        # Verify it's marked RUNNING in DB
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tasks WHERE id = $1", task["id"])
        assert row["status"] == "RUNNING"
        assert row["worker_id"] == worker_id

    async def test_returns_none_when_no_pending(self, setup_services, clean_db):
        task = await assign_task("some-worker")
        assert task is None
