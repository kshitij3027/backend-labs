"""Window lifecycle management: alignment, state tracking, and event routing."""

from __future__ import annotations

import time
from datetime import datetime

from src.config import AppConfig, WindowTypeConfig
from src.models import LogEvent, WindowState


class WindowManager:
    """Manages time-based windows in Redis, routing events to the correct windows."""

    def __init__(self, redis_client, config: AppConfig) -> None:
        self._redis = redis_client
        self._config = config

    def align_to_window(self, ts: datetime, window_size: int) -> int:
        """Floor timestamp to nearest window boundary. Returns unix timestamp."""
        ts_unix = int(ts.timestamp())
        return (ts_unix // window_size) * window_size

    def get_window_key(self, window_type: str, window_size: int, start_ts: int) -> str:
        """Build Redis key for a window."""
        return f"window:{window_type}:{window_size}:{start_ts}"

    def get_window_state(self, start_ts: int, window_config: WindowTypeConfig) -> WindowState:
        """Determine window state based on current time."""
        now = int(time.time())
        end_ts = start_ts + window_config.size_seconds
        grace_deadline = end_ts + window_config.grace_period_seconds

        if now < end_ts:
            return WindowState.ACTIVE
        elif now < grace_deadline:
            return WindowState.GRACE
        else:
            return WindowState.CLOSED

    async def assign_event(self, event: LogEvent, parsed_ts: datetime) -> list[dict]:
        """Route event to all matching windows.

        Returns list of assignment results. Each result:
            {"window_key": str, "window_type": str, "state": WindowState,
             "accepted": bool, "late": bool}
        """
        results: list[dict] = []

        for wt in self._config.window_types:
            start_ts = self.align_to_window(parsed_ts, wt.size_seconds)
            window_key = self.get_window_key(wt.name, wt.size_seconds, start_ts)
            state = self.get_window_state(start_ts, wt)

            if state == WindowState.CLOSED:
                results.append({
                    "window_key": window_key,
                    "window_type": wt.name,
                    "state": state,
                    "accepted": False,
                    "late": False,
                })
            else:
                end_ts = start_ts + wt.size_seconds
                ttl = wt.size_seconds + wt.grace_period_seconds + wt.retention_seconds
                await self.ensure_window_exists(window_key, start_ts, end_ts, wt.name, ttl)

                # Track in active sorted set
                await self._redis.zadd(f"windows:active:{wt.name}", {window_key: start_ts})

                late = state == WindowState.GRACE
                results.append({
                    "window_key": window_key,
                    "window_type": wt.name,
                    "state": state,
                    "accepted": True,
                    "late": late,
                })

        return results

    async def ensure_window_exists(
        self, window_key: str, start_ts: int, end_ts: int, window_type: str, ttl: int
    ) -> None:
        """Create window hash if it doesn't exist, set TTL."""
        created = await self._redis.hsetnx(window_key, "count", 0)
        if created:
            await self._redis.hset(window_key, mapping={
                "error_count": 0,
                "total_response_time": 0,
                "sum_response_time_sq": 0,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "window_type": window_type,
                "status": WindowState.ACTIVE.value,
                "levels": "{}",
                "services": "{}",
            })
            await self._redis.expire(window_key, ttl)
