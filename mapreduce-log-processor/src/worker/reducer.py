"""Reduce task execution logic."""

import json
from collections import defaultdict

import msgpack
import redis.asyncio as aioredis
import structlog

from src.config import settings
from src.mapfunctions.registry import get_reduce_fn

logger = structlog.get_logger()

# Separate Redis connection for binary data (msgpack needs bytes, not decoded strings)
_binary_redis: aioredis.Redis | None = None


async def _get_binary_redis() -> aioredis.Redis:
    """Get a Redis client that does NOT decode responses (needed for msgpack binary data)."""
    global _binary_redis
    if _binary_redis is None:
        _binary_redis = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
        await _binary_redis.ping()
    return _binary_redis


async def close_binary_redis() -> None:
    """Close the binary Redis connection."""
    global _binary_redis
    if _binary_redis is not None:
        await _binary_redis.close()
        _binary_redis = None


async def execute_reduce_task(task: dict) -> None:
    """Execute a reduce task:
    1. Read all KV pairs from Redis for job:{job_id}:reduce:{partition_id}
    2. Deserialize msgpack
    3. Group by key
    4. Apply reduce function
    5. Persist results to PostgreSQL
    6. Delete Redis keys
    """
    import src.db as db

    job_id = task["job_id"]
    partition_id = task["partition_id"]
    reduce_fn_name = task["reduce_fn"]

    redis = await _get_binary_redis()

    # Collect data from all mapper keys for this reducer partition.
    # Each mapper writes to job:{job_id}:map:{task_id}:reduce:{partition_id}
    pattern = f"job:{job_id}:map:*:reduce:{partition_id}"
    mapper_keys = []
    async for key in redis.scan_iter(match=pattern):
        mapper_keys.append(key)

    raw_items = []
    for key in mapper_keys:
        items = await redis.lrange(key, 0, -1)
        raw_items.extend(items)
        await redis.delete(key)

    if not raw_items:
        logger.info(
            "reduce_task_empty",
            job_id=job_id,
            partition_id=partition_id,
        )
        return

    # Deserialize and group by key
    grouped: dict[str, list] = defaultdict(list)
    for raw in raw_items:
        key, value = msgpack.unpackb(raw, raw=False)
        grouped[key].append(value)

    # Get reduce function and apply to each key's values
    reduce_fn = get_reduce_fn(reduce_fn_name)
    results: list[tuple[str, str]] = []
    for key, values in grouped.items():
        reduced_value = reduce_fn(values)
        results.append((key, reduced_value))

    # Delete existing results for this partition before inserting (idempotency)
    await db.delete_results_for_partition(job_id, partition_id)

    # Batch insert results into PostgreSQL
    if results:
        await db.insert_results_batch(job_id, results, partition_id=partition_id)

    logger.info(
        "reduce_task_executed",
        job_id=job_id,
        partition_id=partition_id,
        keys_processed=len(grouped),
        results_written=len(results),
    )
