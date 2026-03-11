"""Tests for retry and DLQ logic."""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import fakeredis.aioredis

from src.config import Config
from src.consumer import LogConsumer
from src.metrics import MetricsAggregator
from src.processor import LogProcessor


@pytest.fixture
def retry_config(tmp_path):
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "stream_key: logs:test\n"
        "consumer_group: test-group\n"
        "max_retries: 2\n"
        "retry_base_delay: 0.01\n"
        "retry_max_delay: 0.1\n"
        "dlq_stream_key: logs:test:dlq\n"
        "block_ms: 100\n"
        "batch_size: 10\n"
    )
    return Config.load(str(yaml_file))


@pytest.fixture
def fake_redis_instance():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.mark.asyncio
async def test_retry_on_failure(retry_config, fake_redis_instance):
    """Failed processing retries up to max_retries then DLQs."""
    redis = fake_redis_instance
    processor = LogProcessor()
    metrics = MetricsAggregator()

    consumer = LogConsumer("test-worker", redis, retry_config, processor, metrics)
    await consumer._ensure_group()

    # Mock processor to always fail
    processor.process_message = MagicMock(side_effect=ValueError("parse error"))

    # Add a message
    msg_id = await redis.xadd(retry_config.stream_key, {"log": "bad data"})

    # Process it — should retry and eventually DLQ
    await consumer._process_message(msg_id, {"log": "bad data"})

    # Verify it went to DLQ
    dlq_msgs = await redis.xrange(retry_config.dlq_stream_key)
    assert len(dlq_msgs) == 1
    dlq_data = dlq_msgs[0][1]
    assert dlq_data["original_id"] == msg_id
    assert "parse error" in dlq_data["error"]


@pytest.mark.asyncio
async def test_dlq_metadata(retry_config, fake_redis_instance):
    """DLQ entries contain proper metadata."""
    redis = fake_redis_instance
    processor = LogProcessor()
    metrics = MetricsAggregator()

    consumer = LogConsumer("test-worker", redis, retry_config, processor, metrics)
    await consumer._ensure_group()

    processor.process_message = MagicMock(side_effect=RuntimeError("crash"))

    msg_id = await redis.xadd(retry_config.stream_key, {"log": "crash data"})
    await consumer._process_message(msg_id, {"log": "crash data"})

    dlq_msgs = await redis.xrange(retry_config.dlq_stream_key)
    assert len(dlq_msgs) == 1
    dlq_entry = dlq_msgs[0][1]
    assert "original_id" in dlq_entry
    assert "error" in dlq_entry
    assert "attempt_count" in dlq_entry
    assert "timestamp" in dlq_entry


@pytest.mark.asyncio
async def test_successful_processing_no_retry(retry_config, fake_redis_instance):
    """Successful processing doesn't trigger retry or DLQ."""
    redis = fake_redis_instance
    processor = LogProcessor()
    metrics = MetricsAggregator()

    consumer = LogConsumer("test-worker", redis, retry_config, processor, metrics)
    await consumer._ensure_group()

    log_line = '10.0.0.1 - - [10/Mar/2026:12:00:00 +0000] "GET /api/test HTTP/1.1" 200 100 "-" "test" 10.0'
    msg_id = await redis.xadd(retry_config.stream_key, {"log": log_line})
    await consumer._process_message(msg_id, {"log": log_line})

    # No DLQ entries
    dlq_msgs = await redis.xrange(retry_config.dlq_stream_key)
    assert len(dlq_msgs) == 0
    assert consumer.processed_count == 1
    assert consumer.error_count == 0
