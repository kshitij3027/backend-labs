"""Redis-backed rate limiter for alert generation.

Uses a sliding-minute bucket with INCR + EXPIRE to cap how many
alerts a single pattern can produce per minute.  This prevents
noisy patterns from overwhelming notification channels.
"""

import time

import structlog
from redis.asyncio import Redis

logger = structlog.get_logger(__name__)


class RateLimiter:
    """Per-pattern rate limiter backed by Redis."""

    def __init__(self, redis_client: Redis, max_per_minute: int = 10) -> None:
        self._redis = redis_client
        self._max = max_per_minute

    def _key(self, pattern_name: str) -> str:
        """Build the Redis key for the current minute bucket."""
        minute_bucket = int(time.time() // 60)
        return f"ratelimit:{pattern_name}:{minute_bucket}"

    async def is_allowed(self, pattern_name: str) -> bool:
        """Check whether another alert for *pattern_name* is allowed.

        Increments the counter for the current minute bucket.  Returns
        ``True`` if the count is within the configured limit, ``False``
        otherwise.
        """
        key = self._key(pattern_name)
        count = await self._redis.incr(key)

        # Set expiry on first increment so the key self-cleans.
        # Use 120s (2 minutes) to safely cover minute-boundary edges.
        if count == 1:
            await self._redis.expire(key, 120)

        allowed = count <= self._max
        if not allowed:
            logger.warning(
                "rate_limit_exceeded",
                pattern=pattern_name,
                count=count,
                limit=self._max,
            )
        return allowed

    async def get_count(self, pattern_name: str) -> int:
        """Return the current hit count for *pattern_name* this minute."""
        key = self._key(pattern_name)
        value = await self._redis.get(key)
        return int(value) if value is not None else 0
