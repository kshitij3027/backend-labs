"""Retry scheduler for the Dead Letter Queue Log Processor."""

import asyncio
import time

from src.config import Settings
from src.redis_client import RedisClient


class RetryScheduler:
    """Polls the retry sorted set and moves due items back to the main queue."""

    def __init__(self, redis_client: RedisClient, settings: Settings) -> None:
        self.redis = redis_client
        self.settings = settings

    async def check_and_requeue(self) -> int:
        """Check for due retries and move them back to the main queue.

        Returns the number of messages requeued.
        """
        now = time.time()
        due = await self.redis.get_due_retries(now)
        for msg in due:
            await self.redis.enqueue(self.settings.main_queue, msg)
        return len(due)

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll loop: check every retry_poll_interval seconds."""
        while not stop_event.is_set():
            await self.check_and_requeue()
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.settings.retry_poll_interval
                )
            except asyncio.TimeoutError:
                pass
