"""Tests for the Redis-backed rate limiter."""

import pytest

from src.engine.rate_limiter import RateLimiter


class TestRateLimiter:
    """Tests for RateLimiter using real Redis."""

    async def test_allows_up_to_limit(self, redis_client):
        """All calls up to max_per_minute should be allowed."""
        limiter = RateLimiter(redis_client, max_per_minute=5)

        results = []
        for _ in range(5):
            results.append(await limiter.is_allowed("test_pattern"))

        assert all(results), "All 5 calls should return True"

    async def test_blocks_after_limit(self, redis_client):
        """The (max_per_minute + 1)-th call should be blocked."""
        limiter = RateLimiter(redis_client, max_per_minute=5)

        for _ in range(5):
            await limiter.is_allowed("test_pattern")

        blocked = await limiter.is_allowed("test_pattern")
        assert blocked is False, "6th call should be blocked"

    async def test_different_patterns_independent(self, redis_client):
        """Exhausting limit for pattern A should not affect pattern B."""
        limiter = RateLimiter(redis_client, max_per_minute=3)

        # Exhaust pattern A
        for _ in range(3):
            await limiter.is_allowed("pattern_a")

        blocked_a = await limiter.is_allowed("pattern_a")
        assert blocked_a is False, "pattern_a should be blocked"

        # Pattern B should still be allowed
        allowed_b = await limiter.is_allowed("pattern_b")
        assert allowed_b is True, "pattern_b should still be allowed"

    async def test_get_count(self, redis_client):
        """get_count returns the correct number after several calls."""
        limiter = RateLimiter(redis_client, max_per_minute=10)

        for _ in range(7):
            await limiter.is_allowed("counted_pattern")

        count = await limiter.get_count("counted_pattern")
        assert count == 7
