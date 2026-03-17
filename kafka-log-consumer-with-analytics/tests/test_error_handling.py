"""Tests for error handling features."""
import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.consumer import LogConsumer
from src.config import Settings


class FakeMessage:
    def __init__(self, value=b'{}', topic="web-logs"):
        self._value = value
        self._topic = topic

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return 0

    def offset(self):
        return 0


class TestRetryWithBackoff:
    def test_retry_succeeds_on_second_attempt(self):
        settings = Settings(bootstrap_servers="localhost:9092")
        call_count = 0

        def on_batch(messages):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient error")

        consumer = LogConsumer(settings, on_batch=on_batch)
        consumer._consumer = MagicMock()
        consumer._start_time = time.time()
        consumer._batch = [FakeMessage()]
        consumer._last_batch_duration = 0.0

        consumer._process_with_retry()
        # Should have retried and succeeded
        assert consumer.stats["total_consumed"] == 1

    def test_retry_exhaustion_sends_to_dead_letter(self):
        settings = Settings(bootstrap_servers="localhost:9092")

        def on_batch(messages):
            raise RuntimeError("persistent error")

        consumer = LogConsumer(settings, on_batch=on_batch)
        consumer._consumer = MagicMock()
        consumer._start_time = time.time()
        consumer._batch = [FakeMessage()]
        consumer._last_batch_duration = 0.0

        with patch.object(consumer, "_send_to_dead_letter") as mock_dlq:
            consumer._process_with_retry()
            mock_dlq.assert_called_once()


class TestSchemaEvolution:
    def test_malformed_message_handled(self):
        """Malformed messages should be counted as failures, not crash."""
        from src.batch_processor import BatchProcessor
        processor = BatchProcessor()
        msg = FakeMessage(value=b"not-json-at-all", topic="web-logs")
        parsed = processor.process_batch([msg])
        assert len(parsed) == 0
        assert processor.stats["total_failed"] == 1

    def test_partial_schema_handled(self):
        """Message with missing fields should still parse."""
        from src.batch_processor import BatchProcessor
        processor = BatchProcessor()
        partial = json.dumps({"log_type": "web_access"}).encode()
        msg = FakeMessage(value=partial, topic="web-logs")
        parsed = processor.process_batch([msg])
        assert len(parsed) == 1  # Should parse with defaults


class TestDeadLetterProducer:
    @patch("confluent_kafka.Producer")
    def test_dead_letter_sends_messages(self, mock_producer_cls):
        settings = Settings(bootstrap_servers="localhost:9092")
        consumer = LogConsumer(settings)
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        messages = [FakeMessage(b'{"test": true}'), FakeMessage(b'{"test": false}')]
        consumer._send_to_dead_letter(messages)

        assert mock_producer.produce.call_count == 2
        mock_producer.flush.assert_called_once()


class TestDynamicThrottling:
    def test_throttle_attribute_exists(self):
        settings = Settings(bootstrap_servers="localhost:9092")
        consumer = LogConsumer(settings)
        assert hasattr(consumer, "_last_batch_duration")
        assert consumer._last_batch_duration == 0.0


class TestConsumerLagTracking:
    def test_lag_update(self):
        from src.analytics import AnalyticsEngine
        engine = AnalyticsEngine()
        engine.update_consumer_lag({"partition-0": 100, "partition-1": 200})
        stats = engine.get_stats()
        assert stats["total_lag"] == 300
        assert stats["high_lag_alert"] is False

    def test_high_lag_alert(self):
        from src.analytics import AnalyticsEngine
        engine = AnalyticsEngine()
        engine.update_consumer_lag({"partition-0": 5000, "partition-1": 6000})
        stats = engine.get_stats()
        assert stats["high_lag_alert"] is True
