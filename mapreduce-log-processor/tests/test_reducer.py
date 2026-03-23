"""Tests for reduce functions and reduce task execution."""

import json
import uuid

import msgpack
import pytest
import pytest_asyncio

import src.db as db
from src.mapfunctions.reducers import collect_reduce, count_reduce, sum_reduce
from src.redis_client import get_redis, init_redis
from src.worker.reducer import _get_binary_redis, execute_reduce_task


# ── Reduce function unit tests ──────────────────────────────────


class TestSumReduce:
    def test_sum_integers(self):
        assert sum_reduce([1, 1, 1]) == "3"

    def test_sum_strings(self):
        assert sum_reduce(["2", "3", "5"]) == "10"

    def test_sum_single_value(self):
        assert sum_reduce([42]) == "42"

    def test_sum_floats(self):
        assert sum_reduce([1.5, 2.5]) == "4.0"


class TestCountReduce:
    def test_count_values(self):
        assert count_reduce([1, 1, 1]) == "3"

    def test_count_single(self):
        assert count_reduce(["x"]) == "1"

    def test_count_empty(self):
        assert count_reduce([]) == "0"


class TestCollectReduce:
    def test_collect_distinct(self):
        result = json.loads(collect_reduce(["a", "b", "a", "c"]))
        assert sorted(result) == ["a", "b", "c"]

    def test_collect_single(self):
        result = json.loads(collect_reduce(["only"]))
        assert result == ["only"]

    def test_collect_numeric(self):
        result = json.loads(collect_reduce([1, 2, 1]))
        assert sorted(result) == ["1", "2"]


# ── Reduce task integration tests ───────────────────────────────


@pytest_asyncio.fixture(loop_scope="session")
async def setup_db_redis(setup_services):
    """Ensure DB and Redis are ready (reuses session-level setup)."""
    yield


@pytest.mark.anyio
async def test_execute_reduce_task_sum(setup_db_redis):
    """Write msgpack data to Redis, run reducer with sum, verify results in DB."""
    # Register reduce functions
    import src.mapfunctions.reducers  # noqa: F401

    job_id = str(uuid.uuid4())

    # Create a job in DB
    async with db.pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
               VALUES ($1, 'REDUCING', '/data/test.log', 'word_count', 'sum', 2, 1)""",
            job_id,
        )

    # Write mock intermediate data to Redis
    redis = await _get_binary_redis()
    redis_key = f"job:{job_id}:reduce:0"

    pairs = [("hello", 1), ("hello", 1), ("hello", 1), ("world", 1), ("world", 1)]
    for pair in pairs:
        await redis.rpush(redis_key, msgpack.packb(pair))

    # Execute reduce task
    task = {
        "job_id": job_id,
        "partition_id": 0,
        "reduce_fn": "sum",
    }
    await execute_reduce_task(task)

    # Verify results in DB
    results = await db.get_job_results(job_id)
    result_dict = {r["key"]: r["value"] for r in results}

    assert result_dict["hello"] == "3"
    assert result_dict["world"] == "2"

    # Verify Redis key was cleaned up
    remaining = await redis.lrange(redis_key, 0, -1)
    assert len(remaining) == 0


@pytest.mark.anyio
async def test_execute_reduce_task_count(setup_db_redis):
    """Test reducer with count function."""
    import src.mapfunctions.reducers  # noqa: F401

    job_id = str(uuid.uuid4())

    async with db.pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
               VALUES ($1, 'REDUCING', '/data/test.log', 'word_count', 'count', 2, 1)""",
            job_id,
        )

    redis = await _get_binary_redis()
    redis_key = f"job:{job_id}:reduce:0"

    pairs = [("error", "404"), ("error", "500"), ("error", "404"), ("ok", "200")]
    for pair in pairs:
        await redis.rpush(redis_key, msgpack.packb(pair))

    task = {
        "job_id": job_id,
        "partition_id": 0,
        "reduce_fn": "count",
    }
    await execute_reduce_task(task)

    results = await db.get_job_results(job_id)
    result_dict = {r["key"]: r["value"] for r in results}

    assert result_dict["error"] == "3"
    assert result_dict["ok"] == "1"


@pytest.mark.anyio
async def test_execute_reduce_task_empty(setup_db_redis):
    """Test reducer with no data in Redis (empty partition)."""
    import src.mapfunctions.reducers  # noqa: F401

    job_id = str(uuid.uuid4())

    async with db.pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
               VALUES ($1, 'REDUCING', '/data/test.log', 'word_count', 'sum', 2, 1)""",
            job_id,
        )

    task = {
        "job_id": job_id,
        "partition_id": 0,
        "reduce_fn": "sum",
    }
    await execute_reduce_task(task)

    results = await db.get_job_results(job_id)
    assert len(results) == 0


@pytest.mark.anyio
async def test_execute_reduce_task_collect(setup_db_redis):
    """Test reducer with collect function."""
    import src.mapfunctions.reducers  # noqa: F401

    job_id = str(uuid.uuid4())

    async with db.pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO jobs (id, status, input_path, map_fn, reduce_fn, num_mappers, num_reducers)
               VALUES ($1, 'REDUCING', '/data/test.log', 'url_path', 'collect', 2, 1)""",
            job_id,
        )

    redis = await _get_binary_redis()
    redis_key = f"job:{job_id}:reduce:0"

    pairs = [("user1", "/home"), ("user1", "/about"), ("user1", "/home"), ("user2", "/login")]
    for pair in pairs:
        await redis.rpush(redis_key, msgpack.packb(pair))

    task = {
        "job_id": job_id,
        "partition_id": 0,
        "reduce_fn": "collect",
    }
    await execute_reduce_task(task)

    results = await db.get_job_results(job_id)
    result_dict = {r["key"]: r["value"] for r in results}

    user1_values = json.loads(result_dict["user1"])
    assert sorted(user1_values) == ["/about", "/home"]

    user2_values = json.loads(result_dict["user2"])
    assert user2_values == ["/login"]
