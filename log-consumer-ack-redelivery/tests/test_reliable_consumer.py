"""Tests for ReliableConsumer message handling (ack / retry / DLQ paths)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.ack_tracker import AckTracker
from src.config import Settings
from src.log_processor import FatalProcessingError, LogProcessor, ProcessingError
from src.models import MessageState
from src.redelivery_handler import RedeliveryHandler
from src.reliable_consumer import ReliableConsumer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_method(delivery_tag: int = 1) -> MagicMock:
    method = MagicMock()
    method.delivery_tag = delivery_tag
    return method


def _make_properties(headers: dict | None = None) -> MagicMock:
    props = MagicMock()
    props.headers = headers
    props.content_type = "application/json"
    return props


def _make_body(msg_id: str = "test-123", **extra: object) -> bytes:
    data = {"id": msg_id, "message": "test log", "level": "info", **extra}
    return json.dumps(data).encode()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def consumer_setup():
    """Build a ReliableConsumer wired to real tracker/handler but mocked pika."""
    config = Settings()
    tracker = AckTracker()
    handler = RedeliveryHandler(config)
    # No random failures so tests are deterministic
    processor = LogProcessor(failure_rate=0.0, timeout_rate=0.0)
    consumer = ReliableConsumer(config, tracker, handler, processor)

    # Mock pika channel & connection so we never touch a real broker
    consumer._channel = MagicMock()
    consumer._connection = MagicMock()

    return consumer, tracker, config


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOnMessageSuccess:
    """Happy-path: message processed successfully and acked."""

    def test_on_message_success_acks(self, consumer_setup):
        consumer, tracker, _config = consumer_setup

        method = _make_method(delivery_tag=42)
        properties = _make_properties()
        body = _make_body(msg_id="msg-ok-1")

        consumer._on_message(consumer._channel, method, properties, body)

        # basic_ack must have been called with the correct delivery tag
        consumer._channel.basic_ack.assert_called_once_with(delivery_tag=42)

        # Tracker should show ACKNOWLEDGED
        record = tracker.get_record("msg-ok-1")
        assert record is not None
        assert record.state == MessageState.ACKNOWLEDGED


class TestOnMessageRetryableFailure:
    """Retryable ProcessingError triggers ack-then-republish to retry exchange."""

    def test_on_message_retryable_failure_publishes_to_retry(self, consumer_setup):
        consumer, tracker, config = consumer_setup

        method = _make_method(delivery_tag=7)
        properties = _make_properties()
        body = _make_body(msg_id="msg-retry-1")

        with patch.object(
            consumer.processor,
            "process",
            side_effect=ProcessingError("simulated_failure"),
        ):
            consumer._on_message(consumer._channel, method, properties, body)

        # Ack-then-republish: basic_ack must still be called (not basic_nack)
        consumer._channel.basic_ack.assert_called_once_with(delivery_tag=7)
        consumer._channel.basic_nack.assert_not_called()

        # Message should have been published to the retry exchange
        consumer._channel.basic_publish.assert_called_once()
        publish_kwargs = consumer._channel.basic_publish.call_args
        assert publish_kwargs[1]["exchange"] == config.RETRY_EXCHANGE

        # Tracker should show RETRYING
        record = tracker.get_record("msg-retry-1")
        assert record is not None
        assert record.state == MessageState.RETRYING


class TestOnMessageFatalFailure:
    """Fatal error goes straight to DLQ, skipping retry entirely."""

    def test_on_message_fatal_failure_publishes_to_dlq(self, consumer_setup):
        consumer, tracker, config = consumer_setup

        method = _make_method(delivery_tag=13)
        properties = _make_properties()
        body = _make_body(msg_id="msg-fatal-1")

        with patch.object(
            consumer.processor,
            "process",
            side_effect=FatalProcessingError("fatal_message"),
        ):
            consumer._on_message(consumer._channel, method, properties, body)

        # basic_ack called (not nack)
        consumer._channel.basic_ack.assert_called_once_with(delivery_tag=13)

        # Published to DLQ exchange
        consumer._channel.basic_publish.assert_called_once()
        publish_kwargs = consumer._channel.basic_publish.call_args
        assert publish_kwargs[1]["exchange"] == config.DLQ_EXCHANGE

        # Tracker should show DEAD_LETTERED
        record = tracker.get_record("msg-fatal-1")
        assert record is not None
        assert record.state == MessageState.DEAD_LETTERED


class TestOnMessageMaxRetriesExceeded:
    """When retry count has reached MAX_RETRIES, the message goes to DLQ."""

    def test_on_message_max_retries_exceeded_goes_to_dlq(self, consumer_setup):
        consumer, tracker, config = consumer_setup

        # Simulate a message that has already been retried MAX_RETRIES times
        headers = {"x-retry-count": config.MAX_RETRIES}
        method = _make_method(delivery_tag=99)
        properties = _make_properties(headers=headers)
        body = _make_body(msg_id="msg-maxretry-1")

        with patch.object(
            consumer.processor,
            "process",
            side_effect=ProcessingError("still_failing"),
        ):
            consumer._on_message(consumer._channel, method, properties, body)

        # basic_ack still called
        consumer._channel.basic_ack.assert_called_once_with(delivery_tag=99)

        # Should go to DLQ, not retry
        consumer._channel.basic_publish.assert_called_once()
        publish_kwargs = consumer._channel.basic_publish.call_args
        assert publish_kwargs[1]["exchange"] == config.DLQ_EXCHANGE

        # Tracker should show DEAD_LETTERED
        record = tracker.get_record("msg-maxretry-1")
        assert record is not None
        assert record.state == MessageState.DEAD_LETTERED
