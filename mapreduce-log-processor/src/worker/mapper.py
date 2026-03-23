"""Map task execution logic."""

import json

import msgpack
import redis.asyncio as aioredis
import structlog

from src.config import settings
from src.mapfunctions.registry import get_map_fn

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


async def execute_map_task(task: dict) -> None:
    """Execute a map task:
    1. Read log lines from input_path[input_start:input_end]
    2. Apply map function to each line
    3. Hash-partition output into num_reducers buckets
    4. Write each bucket to Redis
    """
    input_path = task["input_path"]
    input_start = task["input_start"]
    input_end = task["input_end"]
    job_id = task["job_id"]
    num_reducers = task["num_reducers"]
    map_fn_name = task["map_fn"]

    map_fn = get_map_fn(map_fn_name)
    redis = await _get_binary_redis()

    # Initialize buckets for each reducer partition
    buckets: dict[int, list[tuple]] = {i: [] for i in range(num_reducers)}

    lines_processed = 0
    pairs_emitted = 0

    # Read and process log lines
    with open(input_path) as f:
        for line_num, line in enumerate(f):
            if line_num < input_start:
                continue
            if line_num >= input_end:
                break

            try:
                log_line = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            lines_processed += 1

            # Apply map function
            for key, value in map_fn(log_line):
                # Hash partition: determine which reducer gets this key
                reducer_id = hash(key) % num_reducers
                buckets[reducer_id].append((key, value))
                pairs_emitted += 1

    # Write each bucket to Redis
    for reducer_id, pairs in buckets.items():
        if not pairs:
            continue
        redis_key = f"job:{job_id}:reduce:{reducer_id}"
        # Delete existing key to ensure clean slate on retry (idempotency)
        await redis.delete(redis_key)
        # Serialize all pairs as msgpack and RPUSH to Redis list
        pipeline = redis.pipeline()
        for pair in pairs:
            pipeline.rpush(redis_key, msgpack.packb(pair))
        pipeline.expire(redis_key, settings.REDIS_TTL)
        await pipeline.execute()

    logger.info(
        "map_task_executed",
        job_id=job_id,
        input_start=input_start,
        input_end=input_end,
        lines_processed=lines_processed,
        pairs_emitted=pairs_emitted,
    )
