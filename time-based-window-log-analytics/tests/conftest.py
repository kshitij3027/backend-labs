"""Shared test fixtures."""

from __future__ import annotations

import fakeredis.aioredis
import pytest
from httpx import ASGITransport, AsyncClient

from src.aggregator import Aggregator
from src.api import app
from src.config import AppConfig, WindowTypeConfig
from src.timestamp_parser import TimestampParser
from src.window_manager import WindowManager


@pytest.fixture
def app_config() -> AppConfig:
    """Return an AppConfig with test defaults."""
    return AppConfig(
        redis_host="localhost",
        redis_port=6379,
        api_host="0.0.0.0",
        api_port=8080,
        log_level="DEBUG",
        window_types=[
            WindowTypeConfig(
                name="5m",
                size_seconds=300,
                grace_period_seconds=60,
                retention_seconds=3600,
            ),
        ],
        dashboard_refresh_interval=5,
        lifecycle_check_interval=10,
        cleanup_interval=60,
    )


@pytest.fixture
def fake_redis():
    """Return a fakeredis async instance."""
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def window_manager(fake_redis, app_config) -> WindowManager:
    """Return a WindowManager backed by fakeredis."""
    return WindowManager(fake_redis, app_config)


@pytest.fixture
def aggregator(fake_redis) -> Aggregator:
    """Return an Aggregator backed by fakeredis."""
    return Aggregator(fake_redis)


@pytest.fixture
async def test_client() -> AsyncClient:
    """Provide an async HTTP client bound to the FastAPI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def timestamp_parser() -> TimestampParser:
    """Return a TimestampParser instance."""
    return TimestampParser()
