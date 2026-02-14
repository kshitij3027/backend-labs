"""Tests for the rate limiter module."""

import pytest
from src.rate_limiter import TokenBucket, RateLimiter


class TestTokenBucket:
    def test_allows_up_to_max(self):
        bucket = TokenBucket(max_requests=3, window_seconds=60)
        assert bucket.allow() is True
        assert bucket.allow() is True
        assert bucket.allow() is True

    def test_rejects_over_max(self):
        bucket = TokenBucket(max_requests=3, window_seconds=60)
        for _ in range(3):
            bucket.allow()
        assert bucket.allow() is False

    def test_window_reset(self):
        fake_time = [0.0]
        bucket = TokenBucket(
            max_requests=2,
            window_seconds=10,
            time_func=lambda: fake_time[0],
        )
        assert bucket.allow() is True
        assert bucket.allow() is True
        assert bucket.allow() is False

        # Advance past window
        fake_time[0] = 10.0
        assert bucket.allow() is True

    def test_boundary_just_before_window(self):
        fake_time = [0.0]
        bucket = TokenBucket(
            max_requests=1,
            window_seconds=10,
            time_func=lambda: fake_time[0],
        )
        assert bucket.allow() is True
        assert bucket.allow() is False

        # 9.99 seconds — still in same window
        fake_time[0] = 9.99
        assert bucket.allow() is False

    def test_boundary_exactly_at_window(self):
        fake_time = [0.0]
        bucket = TokenBucket(
            max_requests=1,
            window_seconds=10,
            time_func=lambda: fake_time[0],
        )
        assert bucket.allow() is True
        assert bucket.allow() is False

        # Exactly at window boundary
        fake_time[0] = 10.0
        assert bucket.allow() is True

    def test_single_request_limit(self):
        bucket = TokenBucket(max_requests=1, window_seconds=60)
        assert bucket.allow() is True
        assert bucket.allow() is False


class TestRateLimiter:
    def test_disabled_always_allows(self):
        rl = RateLimiter(enabled=False, max_requests=1, window_seconds=60)
        for _ in range(100):
            assert rl.allow("10.0.0.1") is True

    def test_enabled_enforces_limit(self):
        rl = RateLimiter(enabled=True, max_requests=3, window_seconds=60)
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is False

    def test_per_ip_isolation(self):
        rl = RateLimiter(enabled=True, max_requests=2, window_seconds=60)
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is False

        # Different IP has its own bucket
        assert rl.allow("10.0.0.2") is True
        assert rl.allow("10.0.0.2") is True
        assert rl.allow("10.0.0.2") is False

    def test_window_reset_with_rate_limiter(self):
        fake_time = [0.0]
        rl = RateLimiter(
            enabled=True,
            max_requests=1,
            window_seconds=10,
            time_func=lambda: fake_time[0],
        )
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is False

        fake_time[0] = 10.0
        assert rl.allow("10.0.0.1") is True

    def test_multiple_ips_independent_windows(self):
        fake_time = [0.0]
        rl = RateLimiter(
            enabled=True,
            max_requests=1,
            window_seconds=10,
            time_func=lambda: fake_time[0],
        )
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.1") is False

        fake_time[0] = 5.0
        # IP2 starts later, so its window started at t=5
        assert rl.allow("10.0.0.2") is True
        assert rl.allow("10.0.0.2") is False

        # At t=10, IP1's window resets but IP2's hasn't yet
        fake_time[0] = 10.0
        assert rl.allow("10.0.0.1") is True
        assert rl.allow("10.0.0.2") is False

        # At t=15, IP2's window resets
        fake_time[0] = 15.0
        assert rl.allow("10.0.0.2") is True
