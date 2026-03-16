"""Tests for KafkaLogProducer with mocked confluent_kafka.Producer."""

from unittest.mock import MagicMock, patch, call

import pytest

from src.models import LogEntry, LogLevel
from src.producer import KafkaLogProducer


@pytest.fixture
def mock_producer_class():
    """Patch confluent_kafka.Producer so no real broker is needed."""
    with patch("src.producer.Producer") as MockProducer:
        yield MockProducer


@pytest.fixture
def producer(config, mock_producer_class):
    """Return a KafkaLogProducer backed by a mocked confluent_kafka.Producer."""
    return KafkaLogProducer(config)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestSendLog:
    """Tests for the send_log method."""

    def test_send_log_calls_produce(self, producer, mock_producer_class, sample_log_entry):
        """produce() is called with the correct topic, key, and value."""
        inner = mock_producer_class.return_value

        producer.send_log(sample_log_entry)

        inner.produce.assert_called_once()
        kwargs = inner.produce.call_args.kwargs

        assert kwargs["topic"] == sample_log_entry.route_topic()
        assert kwargs["key"] == sample_log_entry.to_kafka_key().encode("utf-8")
        assert kwargs["value"] == sample_log_entry.to_kafka_value().encode("utf-8")
        assert callable(kwargs["callback"])

        # poll(0) should also be called to trigger callbacks
        inner.poll.assert_called_once_with(0)


class TestDeliveryCallback:
    """Tests for the _delivery_callback method."""

    def test_delivery_callback_success(self, producer):
        """Successful delivery increments _sent and topic/partition counts."""
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "logs-application"
        mock_msg.partition.return_value = 1

        producer._delivery_callback(None, mock_msg)

        assert producer._sent == 1
        assert producer._failed == 0
        assert producer._topic_counts["logs-application"] == 1
        assert producer._partition_counts["logs-application-1"] == 1

    def test_delivery_callback_failure(self, producer):
        """Error in delivery increments _failed, not _sent."""
        mock_err = MagicMock()
        mock_msg = MagicMock()

        producer._delivery_callback(mock_err, mock_msg)

        assert producer._failed == 1
        assert producer._sent == 0


class TestSendBatch:
    """Tests for the send_logs_batch method."""

    def test_send_batch_returns_counts(self, producer, mock_producer_class):
        """send_logs_batch sends all entries and returns sent/failed counts."""
        inner = mock_producer_class.return_value
        inner.flush.return_value = 0

        entries = [
            LogEntry(level=LogLevel.INFO, message="msg1", service="svc-a"),
            LogEntry(level=LogLevel.ERROR, message="msg2", service="svc-b"),
            LogEntry(level=LogLevel.WARNING, message="msg3", service="auth-service"),
        ]

        # Simulate successful delivery via the callback during produce
        def fake_produce(**kwargs):
            cb = kwargs.get("callback")
            if cb:
                mock_msg = MagicMock()
                mock_msg.topic.return_value = "logs-application"
                mock_msg.partition.return_value = 0
                cb(None, mock_msg)

        inner.produce.side_effect = fake_produce

        result = producer.send_logs_batch(entries)

        assert result["sent"] == 3
        assert result["failed"] == 0
        assert inner.produce.call_count == 3
        inner.flush.assert_called_once_with(10.0)


class TestStats:
    """Tests for the stats property."""

    def test_stats_property(self, producer):
        """stats returns a dict with all expected keys and correct values."""
        # Simulate some deliveries
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "logs-errors"
        mock_msg.partition.return_value = 2

        producer._delivery_callback(None, mock_msg)
        producer._delivery_callback(None, mock_msg)
        producer._delivery_callback(MagicMock(), mock_msg)  # one failure

        stats = producer.stats

        assert stats["total_sent"] == 2
        assert stats["total_failed"] == 1
        assert stats["topic_counts"] == {"logs-errors": 2}
        assert stats["partition_counts"] == {"logs-errors-2": 2}
        assert stats["success_rate"] == pytest.approx(66.666, rel=1e-2)

    def test_reset_stats(self, producer):
        """reset_stats zeroes all counters."""
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "logs-application"
        mock_msg.partition.return_value = 0

        producer._delivery_callback(None, mock_msg)
        assert producer._sent == 1

        producer.reset_stats()

        stats = producer.stats
        assert stats["total_sent"] == 0
        assert stats["total_failed"] == 0
        assert stats["topic_counts"] == {}
        assert stats["partition_counts"] == {}
        assert stats["success_rate"] == 0.0


class TestFlush:
    """Tests for the flush method."""

    def test_flush_delegates(self, producer, mock_producer_class):
        """flush() delegates to the underlying producer.flush()."""
        inner = mock_producer_class.return_value
        inner.flush.return_value = 0

        result = producer.flush(timeout=5.0)

        inner.flush.assert_called_once_with(5.0)
        assert result == 0
