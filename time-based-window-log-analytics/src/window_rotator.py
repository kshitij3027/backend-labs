"""Background task that transitions windows through their lifecycle."""

from __future__ import annotations

import time

from src.config import AppConfig


class WindowRotator:
    """Scan active windows and transition them through active -> grace -> closed."""

    def __init__(self, redis_client, config: AppConfig) -> None:
        self._redis = redis_client
        self._config = config

    async def check_windows(self) -> None:
        """Scan active windows, transition those past end_ts to grace, past grace to closed."""
        now = int(time.time())

        for wt in self._config.window_types:
            active_key = f"windows:active:{wt.name}"
            members = await self._redis.zrange(active_key, 0, -1, withscores=True)

            for member, score in members:
                window_key = member.decode() if isinstance(member, bytes) else member
                start_ts = int(score)
                end_ts = start_ts + wt.size_seconds
                grace_deadline = end_ts + wt.grace_period_seconds

                if now >= grace_deadline:
                    # Window is closed: update status, remove from active set
                    await self._redis.hset(window_key, "status", "closed")
                    await self._redis.zrem(active_key, window_key)
                elif now >= end_ts:
                    # Window in grace period: update status
                    await self._redis.hset(window_key, "status", "grace")

    async def cleanup_expired(self) -> None:
        """Remove references to windows whose Redis keys have expired (TTL-based)."""
        for wt in self._config.window_types:
            active_key = f"windows:active:{wt.name}"
            members = await self._redis.zrange(active_key, 0, -1)
            for member in members:
                window_key = member.decode() if isinstance(member, bytes) else member
                exists = await self._redis.exists(window_key)
                if not exists:
                    await self._redis.zrem(active_key, window_key)
