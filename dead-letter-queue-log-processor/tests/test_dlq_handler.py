"""Tests for DLQHandler."""

import pytest

from src.config import Settings
from src.dlq_handler import DLQHandler
from src.models import FailedMessage, FailureType, LogLevel, LogMessage


def _make_handler(redis_client) -> DLQHandler:
    """Helper to build a DLQHandler with default settings."""
    settings = Settings()
    return DLQHandler(redis_client, settings)


def _make_failed_message(
    source: str = "test-service",
    failure_type: FailureType = FailureType.UNKNOWN,
    retry_count: int = 1,
    message_id: str | None = None,
    first_failure: str = "2026-03-13T00:00:00+00:00",
) -> FailedMessage:
    """Create a FailedMessage with configurable fields."""
    log_msg = LogMessage(
        id=message_id or f"msg-{source}-{failure_type.value}",
        timestamp="2026-03-13T00:00:00+00:00",
        level=LogLevel.ERROR,
        source=source,
        message=f"Test error from {source}",
    )
    return FailedMessage(
        original_message=log_msg,
        failure_type=failure_type,
        error_details=f"{failure_type.value} error",
        retry_count=retry_count,
        max_retries=3,
        first_failure=first_failure,
        last_failure="2026-03-13T00:01:00+00:00",
    )


async def _populate_dlq(redis_client, messages: list[FailedMessage]) -> None:
    """Push FailedMessage objects into the DLQ."""
    for msg in messages:
        await redis_client.move_to_dlq(msg.to_json())


class TestGetDlqMessagesEmpty:
    """Empty DLQ returns empty list."""

    @pytest.mark.asyncio
    async def test_get_dlq_messages_empty(self, redis_client):
        handler = _make_handler(redis_client)

        result = await handler.get_dlq_messages()

        assert result == []


class TestGetDlqMessagesWithData:
    """Pre-populated DLQ returns all messages."""

    @pytest.mark.asyncio
    async def test_get_dlq_messages_with_data(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(source="svc-1", message_id="m1"),
            _make_failed_message(source="svc-2", message_id="m2"),
            _make_failed_message(source="svc-3", message_id="m3"),
            _make_failed_message(source="svc-4", message_id="m4"),
            _make_failed_message(source="svc-5", message_id="m5"),
        ]
        await _populate_dlq(redis_client, messages)

        result = await handler.get_dlq_messages()

        assert len(result) == 5
        # Verify they are FailedMessage objects
        for msg in result:
            assert isinstance(msg, FailedMessage)


class TestGetDlqCount:
    """Count matches the number of messages in the DLQ."""

    @pytest.mark.asyncio
    async def test_get_dlq_count(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(message_id="c1"),
            _make_failed_message(message_id="c2"),
            _make_failed_message(message_id="c3"),
        ]
        await _populate_dlq(redis_client, messages)

        count = await handler.get_dlq_count()

        assert count == 3


class TestAnalyzeDlq:
    """Analyze returns correct breakdown of DLQ contents."""

    @pytest.mark.asyncio
    async def test_analyze_dlq(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(
                source="api-gateway",
                failure_type=FailureType.PARSING,
                retry_count=1,
                message_id="a1",
                first_failure="2026-03-13T01:00:00+00:00",
            ),
            _make_failed_message(
                source="api-gateway",
                failure_type=FailureType.PARSING,
                retry_count=2,
                message_id="a2",
                first_failure="2026-03-13T02:00:00+00:00",
            ),
            _make_failed_message(
                source="auth-service",
                failure_type=FailureType.NETWORK,
                retry_count=5,
                message_id="a3",
                first_failure="2026-03-13T00:30:00+00:00",
            ),
            _make_failed_message(
                source="auth-service",
                failure_type=FailureType.PARSING,
                retry_count=1,
                message_id="a4",
                first_failure="2026-03-13T03:00:00+00:00",
            ),
            _make_failed_message(
                source="data-pipeline",
                failure_type=FailureType.RESOURCE,
                retry_count=3,
                message_id="a5",
                first_failure="2026-03-13T00:15:00+00:00",
            ),
        ]
        await _populate_dlq(redis_client, messages)

        analysis = await handler.analyze_dlq()

        assert analysis["total"] == 5
        assert analysis["by_failure_type"] == {
            "PARSING": 3,
            "NETWORK": 1,
            "RESOURCE": 1,
        }
        assert analysis["by_source"] == {
            "api-gateway": 2,
            "auth-service": 2,
            "data-pipeline": 1,
        }
        # avg retry count: (1+2+5+1+3) / 5 = 2.4
        assert analysis["avg_retry_count"] == pytest.approx(2.4)
        assert analysis["oldest_failure"] == "2026-03-13T00:15:00+00:00"
        assert analysis["newest_failure"] == "2026-03-13T03:00:00+00:00"


class TestAnalyzeDlqEmpty:
    """Empty DLQ analysis returns zero defaults."""

    @pytest.mark.asyncio
    async def test_analyze_dlq_empty(self, redis_client):
        handler = _make_handler(redis_client)

        analysis = await handler.analyze_dlq()

        assert analysis["total"] == 0
        assert analysis["by_failure_type"] == {}
        assert analysis["by_source"] == {}
        assert analysis["avg_retry_count"] == 0.0
        assert analysis["oldest_failure"] is None
        assert analysis["newest_failure"] is None


class TestReprocessAll:
    """Reprocess moves all DLQ messages back to main queue."""

    @pytest.mark.asyncio
    async def test_reprocess_all(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(message_id="r1"),
            _make_failed_message(message_id="r2"),
            _make_failed_message(message_id="r3"),
        ]
        await _populate_dlq(redis_client, messages)

        reprocessed = await handler.reprocess_all()

        assert reprocessed == 3

        # DLQ should be empty
        dlq_count = await handler.get_dlq_count()
        assert dlq_count == 0

        # Main queue should have 3 messages
        main_count = await redis_client.get_queue_length(
            handler.settings.main_queue
        )
        assert main_count == 3

        # Verify the main queue contains original LogMessage JSON, not FailedMessage
        main_msgs = await redis_client._redis.lrange(
            handler.settings.main_queue, 0, -1
        )
        for raw in main_msgs:
            log_msg = LogMessage.from_json(raw)
            assert isinstance(log_msg, LogMessage)


class TestReprocessByType:
    """Reprocess by type moves only matching messages, keeps the rest."""

    @pytest.mark.asyncio
    async def test_reprocess_by_type(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(
                failure_type=FailureType.PARSING, message_id="p1"
            ),
            _make_failed_message(
                failure_type=FailureType.PARSING, message_id="p2"
            ),
            _make_failed_message(
                failure_type=FailureType.NETWORK, message_id="n1"
            ),
            _make_failed_message(
                failure_type=FailureType.RESOURCE, message_id="res1"
            ),
        ]
        await _populate_dlq(redis_client, messages)

        reprocessed = await handler.reprocess_by_type(FailureType.PARSING)

        assert reprocessed == 2

        # DLQ should contain only NETWORK and RESOURCE messages
        remaining = await handler.get_dlq_messages()
        assert len(remaining) == 2
        remaining_types = {m.failure_type for m in remaining}
        assert remaining_types == {FailureType.NETWORK, FailureType.RESOURCE}

        # Main queue should have 2 messages (the PARSING ones)
        main_count = await redis_client.get_queue_length(
            handler.settings.main_queue
        )
        assert main_count == 2


class TestPurge:
    """Purge deletes all DLQ messages and returns the count."""

    @pytest.mark.asyncio
    async def test_purge(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(message_id="d1"),
            _make_failed_message(message_id="d2"),
            _make_failed_message(message_id="d3"),
            _make_failed_message(message_id="d4"),
        ]
        await _populate_dlq(redis_client, messages)

        purged = await handler.purge()

        assert purged == 4

        dlq_count = await handler.get_dlq_count()
        assert dlq_count == 0


class TestDetectPoisonMessages:
    """Detect poison messages returns only high-retry entries."""

    @pytest.mark.asyncio
    async def test_detect_poison_messages(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(retry_count=1, message_id="low1"),
            _make_failed_message(retry_count=2, message_id="low2"),
            _make_failed_message(retry_count=3, message_id="exact"),
            _make_failed_message(retry_count=5, message_id="high1"),
            _make_failed_message(retry_count=10, message_id="high2"),
        ]
        await _populate_dlq(redis_client, messages)

        # Default threshold is 3
        poison = await handler.detect_poison_messages()

        assert len(poison) == 3
        retry_counts = {m.retry_count for m in poison}
        assert retry_counts == {3, 5, 10}

    @pytest.mark.asyncio
    async def test_detect_poison_messages_custom_threshold(self, redis_client):
        handler = _make_handler(redis_client)
        messages = [
            _make_failed_message(retry_count=1, message_id="t1"),
            _make_failed_message(retry_count=5, message_id="t2"),
            _make_failed_message(retry_count=8, message_id="t3"),
        ]
        await _populate_dlq(redis_client, messages)

        poison = await handler.detect_poison_messages(threshold=5)

        assert len(poison) == 2
        retry_counts = {m.retry_count for m in poison}
        assert retry_counts == {5, 8}
