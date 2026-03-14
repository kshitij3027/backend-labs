"""Tests for MessageProcessor."""

import json

import pytest

from src.classifier import FailureClassifier
from src.config import Settings
from src.models import FailedMessage, FailureType, LogLevel, LogMessage
from src.processor import MessageProcessor


def _make_processor(redis_client, **overrides) -> MessageProcessor:
    """Helper to build a MessageProcessor with sensible defaults."""
    settings = Settings(backoff_base=1.0, **overrides)
    classifier = FailureClassifier()
    return MessageProcessor(redis_client, settings, classifier)


def _valid_log_json(**overrides) -> str:
    """Return a valid LogMessage serialised as JSON."""
    defaults = {
        "id": "test-id-1",
        "timestamp": "2026-03-13T00:00:00+00:00",
        "level": LogLevel.INFO,
        "source": "test-service",
        "message": "everything is fine",
    }
    defaults.update(overrides)
    return LogMessage(**defaults).to_json()


class TestProcessValidMessage:
    """Processing a well-formed message stores it in processed_store."""

    @pytest.mark.asyncio
    async def test_process_valid_message(self, redis_client):
        processor = _make_processor(redis_client)
        raw = _valid_log_json()

        await processor.process_one(raw)

        # It should be in the processed store
        stored = await redis_client._redis.lrange(
            processor.settings.processed_store, 0, -1
        )
        assert len(stored) == 1
        assert stored[0] == raw

        # Nothing should be in the DLQ
        dlq = await redis_client.get_dlq_messages()
        assert len(dlq) == 0


class TestProcessInvalidJson:
    """Non-JSON input triggers failure handling."""

    @pytest.mark.asyncio
    async def test_process_invalid_json(self, redis_client):
        processor = _make_processor(redis_client)
        raw = "not json at all{{{"

        await processor.process_one(raw)

        # PARSING type gets max 1 retry, so first failure schedules a retry
        retry_members = await redis_client._redis.zrange(
            processor.settings.retry_set, 0, -1
        )
        assert len(retry_members) == 1
        assert retry_members[0] == raw


class TestProcessMissingFields:
    """JSON that is missing required LogMessage fields triggers failure."""

    @pytest.mark.asyncio
    async def test_process_missing_fields(self, redis_client):
        processor = _make_processor(redis_client)
        raw = json.dumps({"id": "1"})

        await processor.process_one(raw)

        # Should be scheduled for retry (first failure, PARSING allows 1 retry)
        retry_members = await redis_client._redis.zrange(
            processor.settings.retry_set, 0, -1
        )
        assert len(retry_members) == 1


class TestProcessTimeoutKeyword:
    """A message containing 'timeout' triggers NETWORK classification."""

    @pytest.mark.asyncio
    async def test_process_message_with_timeout_keyword(self, redis_client):
        processor = _make_processor(redis_client)
        raw = _valid_log_json(
            id="timeout-msg",
            message="Connection timeout after 5000ms",
        )

        await processor.process_one(raw)

        # NETWORK type allows 5 retries, so first failure -> retry set
        retry_members = await redis_client._redis.zrange(
            processor.settings.retry_set, 0, -1
        )
        assert len(retry_members) == 1

        # Verify it was classified as NETWORK
        key = processor._message_key(raw)
        assert processor._failure_types[key] == FailureType.NETWORK


class TestProcessOversizedMessage:
    """A message larger than 50 KB triggers RESOURCE classification."""

    @pytest.mark.asyncio
    async def test_process_oversized_message(self, redis_client):
        processor = _make_processor(redis_client)
        # Create a raw string that exceeds 50000 bytes
        raw = "x" * 100_001

        await processor.process_one(raw)

        # RESOURCE allows 3 retries, so first failure -> retry set
        retry_members = await redis_client._redis.zrange(
            processor.settings.retry_set, 0, -1
        )
        assert len(retry_members) == 1

        # Verify classification
        key = processor._message_key(raw)
        assert processor._failure_types[key] == FailureType.RESOURCE


class TestExhaustRetriesGoesToDlq:
    """A message that exhausts its retries ends up in the DLQ."""

    @pytest.mark.asyncio
    async def test_exhaust_retries_goes_to_dlq(self, redis_client):
        processor = _make_processor(redis_client)
        raw = "not json at all{{{"

        # PARSING failure type allows max 1 retry.
        # First call: retry_count=0 < 1 -> schedule retry (count becomes 1)
        await processor.process_one(raw)
        retry_members = await redis_client._redis.zrange(
            processor.settings.retry_set, 0, -1
        )
        assert len(retry_members) == 1

        # Remove from retry set to simulate the scheduler having moved it back
        await redis_client._redis.zrem(processor.settings.retry_set, raw)

        # Second call: retry_count=1 >= 1 -> DLQ
        await processor.process_one(raw)

        dlq = await redis_client.get_dlq_messages()
        assert len(dlq) == 1

        # Verify the DLQ entry is a valid FailedMessage
        failed = FailedMessage.from_json(dlq[0])
        assert failed.failure_type == FailureType.PARSING
        assert failed.retry_count == 1
        assert failed.max_retries == 1

        # Retry set should be empty after DLQ move
        retry_members = await redis_client._redis.zrange(
            processor.settings.retry_set, 0, -1
        )
        assert len(retry_members) == 0


class TestBackoffComputation:
    """Verify _compute_backoff returns exponential values."""

    def test_backoff_computation(self, redis_client):
        processor = _make_processor(redis_client)

        assert processor._compute_backoff(0) == 1.0  # 1 * 2^0
        assert processor._compute_backoff(1) == 2.0  # 1 * 2^1
        assert processor._compute_backoff(2) == 4.0  # 1 * 2^2
        assert processor._compute_backoff(3) == 8.0  # 1 * 2^3
