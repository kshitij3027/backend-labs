"""Async Redis wrapper for the Dead Letter Queue Log Processor."""

from typing import Optional

import redis.asyncio as aioredis

from src.config import Settings


class RedisClient:
    """Thin async wrapper around redis.asyncio for queue operations."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._redis: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Open a connection to Redis and verify it with a PING."""
        self._redis = aioredis.from_url(
            self._settings.redis_url, decode_responses=True
        )
        await self._redis.ping()

    async def close(self) -> None:
        """Gracefully close the Redis connection."""
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    # ------------------------------------------------------------------
    # Queue primitives
    # ------------------------------------------------------------------

    async def enqueue(self, queue: str, message: str) -> None:
        """LPUSH a message onto a queue."""
        await self._redis.lpush(queue, message)

    async def dequeue(self, queue: str, timeout: float = 1.0) -> Optional[str]:
        """BRPOP a message from a queue.

        Returns the value as a decoded string, or None on timeout.
        """
        result = await self._redis.brpop(queue, timeout=timeout)
        if result is None:
            return None
        # brpop returns (key, value)
        return result[1]

    # ------------------------------------------------------------------
    # Retry scheduling
    # ------------------------------------------------------------------

    async def schedule_retry(self, message: str, retry_at: float) -> None:
        """Add a message to the retry sorted set with the given score."""
        await self._redis.zadd(self._settings.retry_set, {message: retry_at})

    async def get_due_retries(self, now: float) -> list[str]:
        """Return and remove all retry entries whose score is <= *now*."""
        members = await self._redis.zrangebyscore(
            self._settings.retry_set, 0, now
        )
        for member in members:
            await self._redis.zrem(self._settings.retry_set, member)
        return members

    # ------------------------------------------------------------------
    # Dead-letter queue
    # ------------------------------------------------------------------

    async def move_to_dlq(self, message: str) -> None:
        """LPUSH a message into the dead-letter queue."""
        await self._redis.lpush(self._settings.dlq_queue, message)

    async def get_dlq_messages(
        self, start: int = 0, end: int = -1
    ) -> list[str]:
        """LRANGE over the dead-letter queue."""
        return await self._redis.lrange(self._settings.dlq_queue, start, end)

    async def clear_dlq(self) -> int:
        """Delete the DLQ and return the number of messages that were in it."""
        count = await self._redis.llen(self._settings.dlq_queue)
        await self._redis.delete(self._settings.dlq_queue)
        return count

    # ------------------------------------------------------------------
    # Processed messages
    # ------------------------------------------------------------------

    async def store_processed(self, message: str) -> None:
        """LPUSH a successfully processed message."""
        await self._redis.lpush(self._settings.processed_store, message)

    # ------------------------------------------------------------------
    # Queue inspection
    # ------------------------------------------------------------------

    async def get_queue_length(self, queue: str) -> int:
        """Return the LLEN of the given queue."""
        return await self._redis.llen(queue)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    async def increment_stat(self, field: str, amount: int = 1) -> None:
        """HINCRBY on the stats hash."""
        await self._redis.hincrby(self._settings.stats_hash, field, amount)

    async def get_all_stats(self) -> dict:
        """HGETALL on the stats hash."""
        return await self._redis.hgetall(self._settings.stats_hash)

    # ------------------------------------------------------------------
    # Failure history
    # ------------------------------------------------------------------

    async def push_failure_history(self, entry: str) -> None:
        """LPUSH an entry and trim to the configured maximum length."""
        await self._redis.lpush(self._settings.failure_history, entry)
        await self._redis.ltrim(
            self._settings.failure_history,
            0,
            self._settings.failure_history_max - 1,
        )

    async def get_failure_history(self, count: int = 100) -> list[str]:
        """LRANGE the most recent *count* failure-history entries."""
        return await self._redis.lrange(
            self._settings.failure_history, 0, count - 1
        )

    # ------------------------------------------------------------------
    # Admin / testing
    # ------------------------------------------------------------------

    async def flush_all(self) -> None:
        """FLUSHDB — wipe every key in the current database."""
        await self._redis.flushdb()
