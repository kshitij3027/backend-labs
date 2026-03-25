"""Redis-backed aggregation engine using atomic operations."""

from __future__ import annotations

import json
from datetime import datetime

from src.models import LogEvent, WindowMetrics


class Aggregator:
    """Atomically update and read window aggregations in Redis."""

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    async def _json_field_incr(self, hash_key: str, field: str, sub_key: str) -> None:
        """Atomically increment a sub-key inside a JSON hash field.

        Uses WATCH-based optimistic locking for safe read-modify-write.
        """
        while True:
            try:
                await self._redis.watch(hash_key)
                raw = await self._redis.hget(hash_key, field)
                data: dict = json.loads(raw) if raw else {}
                data[sub_key] = data.get(sub_key, 0) + 1

                pipe = self._redis.pipeline(transaction=True)
                pipe.hset(hash_key, field, json.dumps(data))
                await pipe.execute()
                break
            except Exception:
                # WatchError — retry
                continue

    async def record_event(self, window_key: str, event: LogEvent, parsed_ts: datetime) -> None:
        """Atomically update window aggregations for an event."""
        pipe = self._redis.pipeline(transaction=True)
        pipe.hincrby(window_key, "count", 1)

        if event.level.upper() in ("ERROR", "CRITICAL"):
            pipe.hincrby(window_key, "error_count", 1)

        if event.response_time is not None:
            pipe.hincrbyfloat(window_key, "total_response_time", event.response_time)
            pipe.hincrbyfloat(window_key, "sum_response_time_sq", event.response_time ** 2)

        await pipe.execute()

        # JSON field updates with optimistic locking
        await self._json_field_incr(window_key, "levels", event.level.upper())
        await self._json_field_incr(window_key, "services", event.source)

    async def get_window_metrics(self, window_key: str, window_size: int) -> WindowMetrics | None:
        """Read and compute derived metrics for a window."""
        data = await self._redis.hgetall(window_key)
        if not data:
            return None

        count = int(data.get(b"count", 0))
        error_count = int(data.get(b"error_count", 0))
        total_rt = float(data.get(b"total_response_time", 0))

        levels = json.loads(data.get(b"levels", b"{}"))
        services = json.loads(data.get(b"services", b"{}"))

        error_rate = (error_count / count * 100) if count > 0 else 0.0
        avg_rt = (total_rt / count) if count > 0 else None
        throughput = count / window_size if window_size > 0 else 0.0

        return WindowMetrics(
            count=count,
            error_count=error_count,
            error_rate=round(error_rate, 2),
            avg_response_time=round(avg_rt, 2) if avg_rt is not None else None,
            throughput=round(throughput, 4),
            levels=levels,
            services=services,
        )

    async def get_active_windows(self, window_type: str) -> list[str]:
        """Get all tracked window keys for a type from the sorted set."""
        members = await self._redis.zrange(f"windows:active:{window_type}", 0, -1)
        return [m.decode() if isinstance(m, bytes) else m for m in members]
