"""Unit tests for ErrorAggregator with mocked confluent_kafka.Consumer."""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.error_aggregator import ErrorAggregator, _ERROR_RATE_WINDOW
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
        status_code=200 if level != LogLevel.ERROR else 500,
        user_id="test-user-001",
        message="Request processed" if level != LogLevel.ERROR else "Internal error",
        sequence_number=1,
    )


def _make_kafka_msg(
    log_msg: LogMessage,
    topic: str = "web-api-logs",
    partition: int = 0,
    offset: int = 0,
):
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = log_msg.to_kafka_value()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = log_msg.partition_key
    return msg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestErrorAggregatorFiltering:
    """ErrorAggregator keeps only ERROR-level messages."""

    @patch("src.error_aggregator.Consumer")
    def test_keeps_error_messages(self, mock_consumer_cls, settings):
        error_log = _make_log_message(level=LogLevel.ERROR)
        kafka_msg = _make_kafka_msg(error_log)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [kafka_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        assert len(agg.recent_errors) == 1
        assert agg.recent_errors[0]["data"]["level"] == "ERROR"

    @patch("src.error_aggregator.Consumer")
    def test_rejects_info_messages(self, mock_consumer_cls, settings):
        info_log = _make_log_message(level=LogLevel.INFO)
        kafka_msg = _make_kafka_msg(info_log)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [kafka_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        assert len(agg.recent_errors) == 0

    @patch("src.error_aggregator.Consumer")
    def test_rejects_warn_messages(self, mock_consumer_cls, settings):
        warn_log = _make_log_message(level=LogLevel.WARN)
        kafka_msg = _make_kafka_msg(warn_log)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [kafka_msg, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        assert len(agg.recent_errors) == 0

    @patch("src.error_aggregator.Consumer")
    def test_mixed_levels_only_errors_kept(self, mock_consumer_cls, settings):
        msgs = []
        levels = [LogLevel.INFO, LogLevel.ERROR, LogLevel.WARN, LogLevel.ERROR, LogLevel.INFO]
        for i, lvl in enumerate(levels):
            log_msg = _make_log_message(level=lvl)
            msgs.append(_make_kafka_msg(log_msg, offset=i))

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        assert len(agg.recent_errors) == 2


class TestErrorAggregatorCounts:
    """error_counts tracks per-service counts correctly."""

    @patch("src.error_aggregator.Consumer")
    def test_per_service_error_counts(self, mock_consumer_cls, settings):
        msgs = []
        services = [
            ServiceName.WEB_API,
            ServiceName.WEB_API,
            ServiceName.PAYMENT_SERVICE,
        ]
        for i, svc in enumerate(services):
            log_msg = _make_log_message(service=svc, level=LogLevel.ERROR)
            topic = f"{svc.value}-logs"
            msgs.append(_make_kafka_msg(log_msg, topic=topic, offset=i))

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        counts = agg.error_counts
        assert counts["web-api"] == 2
        assert counts["payment-service"] == 1

    @patch("src.error_aggregator.Consumer")
    def test_non_error_messages_not_in_counts(self, mock_consumer_cls, settings):
        info_log = _make_log_message(level=LogLevel.INFO)
        error_log = _make_log_message(level=LogLevel.ERROR, service=ServiceName.PAYMENT_SERVICE)

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = [
            _make_kafka_msg(info_log, offset=0),
            _make_kafka_msg(error_log, topic="payment-service-logs", offset=1),
            None,
            None,
        ]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        counts = agg.error_counts
        assert "web-api" not in counts
        assert counts["payment-service"] == 1


class TestErrorAggregatorErrorRate:
    """error_rate calculation over rolling window."""

    @patch("src.error_aggregator.Consumer")
    def test_error_rate_within_window(self, mock_consumer_cls, settings):
        error_log = _make_log_message(level=LogLevel.ERROR)
        kafka_msgs = [_make_kafka_msg(error_log, offset=i) for i in range(10)]

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = kafka_msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        rate = agg.error_rate
        # 10 errors within the last 60 seconds -> 10 / 60 ~ 0.17
        assert rate > 0
        assert rate == round(10 / _ERROR_RATE_WINDOW, 2)

    @patch("src.error_aggregator.Consumer")
    def test_error_rate_zero_when_no_errors(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.1)
        agg.stop()

        assert agg.error_rate == 0.0


class TestErrorAggregatorBuffer:
    """recent_errors respects the max buffer size."""

    @patch("src.error_aggregator.Consumer")
    def test_buffer_bounded(self, mock_consumer_cls):
        small_settings = Settings(sse_max_buffer=3)
        error_log = _make_log_message(level=LogLevel.ERROR)
        kafka_msgs = [_make_kafka_msg(error_log, offset=i) for i in range(5)]

        mock_instance = MagicMock()
        mock_instance.poll.side_effect = kafka_msgs + [None, None, None]
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(small_settings)
        agg.start()
        time.sleep(0.3)
        agg.stop()

        errors = agg.recent_errors
        assert len(errors) == 3
        offsets = [e["offset"] for e in errors]
        assert offsets == [2, 3, 4]


class TestErrorAggregatorGroupId:
    """ErrorAggregator uses a different group_id than DashboardConsumer."""

    @patch("src.error_aggregator.Consumer")
    def test_uses_error_aggregator_group_id(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        agg = ErrorAggregator(settings)
        agg.start()
        time.sleep(0.1)
        agg.stop()

        call_args = mock_consumer_cls.call_args[0][0]
        assert call_args["group.id"] == "error-aggregator-consumer"

    def test_group_ids_are_different(self, settings):
        assert settings.dashboard_group_id != settings.error_aggregator_group_id
