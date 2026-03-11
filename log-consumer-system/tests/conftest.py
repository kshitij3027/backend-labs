"""Shared test fixtures."""

from __future__ import annotations

import os
import tempfile

import pytest
import fakeredis.aioredis

from src.config import Config


@pytest.fixture
def config(tmp_path):
    """Create a Config with test defaults."""
    config_yaml = tmp_path / "config.yaml"
    config_yaml.write_text(
        "redis_url: redis://localhost:6379\n"
        "stream_key: logs:test\n"
        "consumer_group: test-group\n"
        "num_workers: 2\n"
        "batch_size: 10\n"
        "block_ms: 100\n"
        "max_retries: 3\n"
        "retry_base_delay: 0.1\n"
        "retry_max_delay: 1.0\n"
        "dlq_stream_key: logs:test:dlq\n"
        "idempotency_ttl: 60\n"
    )
    return Config.load(str(config_yaml))


@pytest.fixture
def fake_redis():
    """Create a fakeredis async instance."""
    return fakeredis.aioredis.FakeRedis(decode_responses=True)
