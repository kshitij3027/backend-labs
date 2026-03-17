"""Tests for the Kafka consumer."""
import threading
import time
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from src.consumer import LogConsumer
from src.config import Settings


class FakeMessage:
    """Minimal mock of a confluent_kafka.Message."""

    def __init__(self, value: bytes, topic: str = "web-logs", error=None):
        self._value = value
        self._topic = topic
        self._error = error

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def error(self):
        return self._error

    def partition(self):
        return 0

    def offset(self):
        return 0


class TestLogConsumerInit:
    def test_defaults(self, settings):
        consumer = LogConsumer(settings)
        assert consumer.is_running is False
        assert consumer.stats["total_consumed"] == 0
        assert consumer.stats["total_errors"] == 0

    def test_stats_snapshot(self, settings):
        consumer = LogConsumer(settings)
        stats = consumer.stats
        assert "is_running" in stats
        assert "total_consumed" in stats
        assert "throughput" in stats
        assert "assigned_partitions" in stats


class TestLogConsumerBatchProcessing:
    def test_batch_callback_called(self, settings):
        """Verify the on_batch callback receives accumulated messages."""
        received_batches = []

        def on_batch(messages):
            received_batches.append(list(messages))

        settings.batch_size = 3
        consumer = LogConsumer(settings, on_batch=on_batch)

        # Simulate internal batch processing
        consumer._batch = [
            FakeMessage(b'{"log_type":"web_access"}'),
            FakeMessage(b'{"log_type":"app_log"}'),
            FakeMessage(b'{"log_type":"error_log"}'),
        ]
        consumer._start_time = time.time()

        # Mock the kafka consumer's commit
        consumer._consumer = MagicMock()

        consumer._process_batch()

        assert len(received_batches) == 1
        assert len(received_batches[0]) == 3
        assert consumer.stats["total_consumed"] == 3
        assert consumer.stats["batches_processed"] == 1

    def test_empty_batch_noop(self, settings):
        consumer = LogConsumer(settings)
        consumer._start_time = time.time()
        consumer._consumer = MagicMock()
        consumer._process_batch()
        assert consumer.stats["batches_processed"] == 0


class TestLogConsumerRebalance:
    def test_on_assign(self, settings):
        consumer = LogConsumer(settings)
        mock_partitions = [MagicMock(), MagicMock()]
        consumer._on_assign(None, mock_partitions)
        assert len(consumer.assigned_partitions) == 2

    def test_on_revoke_flushes_batch(self, settings):
        """Revoke should flush any partial batch."""
        flushed = []

        def on_batch(messages):
            flushed.extend(messages)

        consumer = LogConsumer(settings, on_batch=on_batch)
        consumer._start_time = time.time()
        consumer._consumer = MagicMock()
        consumer._batch = [FakeMessage(b'{"log_type":"web_access"}')]
        consumer._on_revoke(None, [])
        assert len(flushed) == 1
        assert consumer.stats["total_consumed"] == 1


class TestLogConsumerLifecycle:
    @patch("src.consumer.Consumer")
    def test_start_creates_thread(self, mock_consumer_cls, settings):
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        consumer = LogConsumer(settings)
        consumer.start()

        assert consumer.is_running is True
        mock_instance.subscribe.assert_called_once()

        consumer.stop()
        assert consumer.is_running is False

    def test_stop_idempotent(self, settings):
        consumer = LogConsumer(settings)
        consumer.stop()  # Should not raise
        consumer.stop()  # Should not raise
