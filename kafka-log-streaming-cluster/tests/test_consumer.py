"""Unit tests for DashboardConsumer with mocked confluent_kafka.Consumer."""

import time
import threading
from collections import deque
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from src.config import Settings
from src.consumer import DashboardConsumer
from src.models import LogLevel, LogMessage, ServiceName


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_log_message(
    service: ServiceName = ServiceName.WEB_API,
    level: LogLevel = LogLevel.INFO,
) -> LogMessage:
    return LogMessage(
        timestamp="2026-03-15T10:30:00+00:00",
        service=service,
        level=level,
        endpoint="/api/users",
        status_code=200,
        user_id="test-user-001",
        message="Request processed",
        sequence_number=1,
    )


def _make_kafka_msg(
    log_msg: LogMessage,
    topic: str = "web-api-logs",
    partition: int = 0,
    offset: int = 0,
):
    """Return a MagicMock that quacks like a confluent_kafka.Message."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = log_msg.to_kafka_value()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = log_msg.partition_key
    return msg


def _make_error_kafka_msg(error_code):
    """Return a mock Kafka message that carries an error."""
    error_obj = MagicMock()
    error_obj.code.return_value = error_code
    error_obj.__str__ = lambda self: f"KafkaError({error_code})"
    msg = MagicMock()
    msg.error.return_value = error_obj
    return msg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDashboardConsumerStoresMessages:
    """DashboardConsumer correctly stores parsed messages."""

    @patch("src.consumer.Consumer")
    def test_stores_single_message(self, mock_consumer_cls, settings):
        log_msg = _make_log_message()
        kafka_msg = _make_kafka_msg(log_msg)

        # The mock Consumer.poll() returns our message once, then None forever
        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [kafka_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        messages = consumer.recent_messages
        assert len(messages) == 1
        assert messages[0]["topic"] == "web-api-logs"
        assert messages[0]["partition"] == 0
        assert messages[0]["offset"] == 0
        assert messages[0]["data"]["service"] == "web-api"
        assert messages[0]["data"]["level"] == "INFO"

    @patch("src.consumer.Consumer")
    def test_stores_multiple_messages(self, mock_consumer_cls, settings):
        msgs = []
        for i, svc in enumerate(ServiceName):
            log_msg = _make_log_message(service=svc, level=LogLevel.INFO)
            msgs.append(_make_kafka_msg(log_msg, topic=f"{svc.value}-logs", offset=i))

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        assert len(consumer.recent_messages) == 3


class TestDashboardConsumerMaxBuffer:
    """DashboardConsumer respects the sse_max_buffer limit."""

    @patch("src.consumer.Consumer")
    def test_buffer_bounded_by_sse_max_buffer(self, mock_consumer_cls):
        small_settings = Settings(sse_max_buffer=3)
        log_msg = _make_log_message()
        kafka_msgs = [_make_kafka_msg(log_msg, offset=i) for i in range(5)]

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = kafka_msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(small_settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        # Only the 3 most recent should remain
        messages = consumer.recent_messages
        assert len(messages) == 3
        offsets = [m["offset"] for m in messages]
        assert offsets == [2, 3, 4]


class TestDashboardConsumerStats:
    """DashboardConsumer.stats updates correctly."""

    @patch("src.consumer.Consumer")
    def test_stats_total_count(self, mock_consumer_cls, settings):
        log_msg = _make_log_message()
        kafka_msgs = [_make_kafka_msg(log_msg, offset=i) for i in range(4)]

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = kafka_msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        assert consumer.stats["total"] == 4

    @patch("src.consumer.Consumer")
    def test_stats_by_service(self, mock_consumer_cls, settings):
        msgs = []
        for svc in [ServiceName.WEB_API, ServiceName.WEB_API, ServiceName.PAYMENT_SERVICE]:
            log_msg = _make_log_message(service=svc)
            msgs.append(_make_kafka_msg(log_msg))

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        stats = consumer.stats
        assert stats["by_service"]["web-api"] == 2
        assert stats["by_service"]["payment-service"] == 1

    @patch("src.consumer.Consumer")
    def test_stats_by_level(self, mock_consumer_cls, settings):
        msgs = []
        for lvl in [LogLevel.INFO, LogLevel.INFO, LogLevel.ERROR]:
            log_msg = _make_log_message(level=lvl)
            msgs.append(_make_kafka_msg(log_msg))

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        stats = consumer.stats
        assert stats["by_level"]["INFO"] == 2
        assert stats["by_level"]["ERROR"] == 1


class TestDashboardConsumerMalformedMessages:
    """DashboardConsumer handles malformed Kafka messages gracefully."""

    @patch("src.consumer.Consumer")
    def test_malformed_message_does_not_crash(self, mock_consumer_cls, settings):
        bad_msg = MagicMock()
        bad_msg.error.return_value = None
        bad_msg.value.return_value = b"this is not valid json"
        bad_msg.topic.return_value = "web-api-logs"
        bad_msg.partition.return_value = 0
        bad_msg.offset.return_value = 0
        bad_msg.key.return_value = b"some-key"

        # Good message after the bad one
        good_log = _make_log_message()
        good_msg = _make_kafka_msg(good_log, offset=1)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [bad_msg, good_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        # Only the good message should be stored
        assert len(consumer.recent_messages) == 1
        assert consumer.stats["total"] == 1


class TestDashboardConsumerKafkaErrors:
    """DashboardConsumer handles Kafka-level errors from poll()."""

    @patch("src.consumer.Consumer")
    def test_partition_eof_is_silently_skipped(self, mock_consumer_cls, settings):
        from confluent_kafka import KafkaError

        eof_msg = _make_error_kafka_msg(KafkaError._PARTITION_EOF)
        good_log = _make_log_message()
        good_msg = _make_kafka_msg(good_log)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [eof_msg, good_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        assert len(consumer.recent_messages) == 1

    @patch("src.consumer.Consumer")
    def test_other_kafka_error_is_logged_and_skipped(self, mock_consumer_cls, settings):
        from confluent_kafka import KafkaError

        err_msg = _make_error_kafka_msg(KafkaError._ALL_BROKERS_DOWN)
        good_log = _make_log_message()
        good_msg = _make_kafka_msg(good_log)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [err_msg, good_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        assert len(consumer.recent_messages) == 1


class TestDashboardConsumerLifecycle:
    """DashboardConsumer start/stop lifecycle."""

    @patch("src.consumer.Consumer")
    def test_start_creates_thread_and_subscribes(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        assert not consumer.is_running

        consumer.start()
        time.sleep(0.1)
        assert consumer.is_running

        # Verify subscribe was called with correct topics
        mock_instance.subscribe.assert_called_once_with(settings.all_service_topics)

        consumer.stop()
        time.sleep(0.1)
        assert not consumer.is_running

    @patch("src.consumer.Consumer")
    def test_stop_closes_kafka_consumer(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.1)
        consumer.stop()

        mock_instance.close.assert_called_once()

    @patch("src.consumer.Consumer")
    def test_consumer_uses_correct_group_id(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.1)
        consumer.stop()

        call_args = mock_consumer_cls.call_args[0][0]
        assert call_args["group.id"] == "dashboard-consumer"


class TestDashboardConsumerThreadSafety:
    """recent_messages and stats return copies, not internal mutable state."""

    @patch("src.consumer.Consumer")
    def test_recent_messages_returns_copy(self, mock_consumer_cls, settings):
        log_msg = _make_log_message()
        kafka_msg = _make_kafka_msg(log_msg)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [kafka_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        snapshot1 = consumer.recent_messages
        snapshot2 = consumer.recent_messages
        # They should be equal but not the same object
        assert snapshot1 == snapshot2
        assert snapshot1 is not snapshot2
        # Mutating the snapshot must not affect the internal state
        snapshot1.clear()
        assert len(consumer.recent_messages) == 1

    @patch("src.consumer.Consumer")
    def test_stats_returns_copy(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        consumer = DashboardConsumer(settings)
        consumer.start()
        time.sleep(0.1)
        consumer.stop()

        stats1 = consumer.stats
        stats2 = consumer.stats
        assert stats1 is not stats2
