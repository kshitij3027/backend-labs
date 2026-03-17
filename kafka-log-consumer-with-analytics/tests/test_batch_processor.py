"""Tests for the batch processor."""
import json

import pytest

from src.batch_processor import BatchProcessor
from src.models import WebAccessLog, AppLog, ErrorLog


class FakeMessage:
    """Minimal mock of a confluent_kafka.Message."""

    def __init__(self, value: bytes, topic: str = "web-logs"):
        self._value = value
        self._topic = topic

    def value(self):
        return self._value

    def topic(self):
        return self._topic


class TestBatchProcessor:
    def test_process_web_logs(self, sample_web_log):
        processor = BatchProcessor()
        msg = FakeMessage(json.dumps(sample_web_log).encode(), topic="web-logs")
        parsed = processor.process_batch([msg])
        assert len(parsed) == 1
        assert isinstance(parsed[0], WebAccessLog)
        assert processor.stats["web_count"] == 1

    def test_process_app_logs(self, sample_app_log):
        processor = BatchProcessor()
        msg = FakeMessage(json.dumps(sample_app_log).encode(), topic="app-logs")
        parsed = processor.process_batch([msg])
        assert len(parsed) == 1
        assert isinstance(parsed[0], AppLog)
        assert processor.stats["app_count"] == 1

    def test_process_error_logs(self, sample_error_log):
        processor = BatchProcessor()
        msg = FakeMessage(json.dumps(sample_error_log).encode(), topic="error-logs")
        parsed = processor.process_batch([msg])
        assert len(parsed) == 1
        assert isinstance(parsed[0], ErrorLog)
        assert processor.stats["error_count"] == 1

    def test_process_mixed_batch(self, sample_web_log, sample_app_log, sample_error_log):
        processor = BatchProcessor()
        messages = [
            FakeMessage(json.dumps(sample_web_log).encode(), topic="web-logs"),
            FakeMessage(json.dumps(sample_app_log).encode(), topic="app-logs"),
            FakeMessage(json.dumps(sample_error_log).encode(), topic="error-logs"),
        ]
        parsed = processor.process_batch(messages)
        assert len(parsed) == 3
        stats = processor.stats
        assert stats["web_count"] == 1
        assert stats["app_count"] == 1
        assert stats["error_count"] == 1
        assert stats["total_processed"] == 3

    def test_invalid_message_counted_as_failure(self):
        processor = BatchProcessor()
        msg = FakeMessage(b"not json", topic="web-logs")
        parsed = processor.process_batch([msg])
        assert len(parsed) == 0
        assert processor.stats["total_failed"] == 1
        assert processor.stats["success_rate"] == 0.0

    def test_success_rate_calculation(self, sample_web_log):
        processor = BatchProcessor()
        good = FakeMessage(json.dumps(sample_web_log).encode(), topic="web-logs")
        bad = FakeMessage(b"invalid", topic="web-logs")
        processor.process_batch([good, good, bad])
        stats = processor.stats
        assert stats["total_processed"] == 2
        assert stats["total_failed"] == 1
        assert stats["success_rate"] == pytest.approx(66.67, abs=0.01)

    def test_empty_batch(self):
        processor = BatchProcessor()
        parsed = processor.process_batch([])
        assert len(parsed) == 0
        assert processor.stats["total_processed"] == 0
        assert processor.stats["success_rate"] == 100.0

    def test_cumulative_stats(self, sample_web_log, sample_app_log):
        processor = BatchProcessor()
        msg1 = FakeMessage(json.dumps(sample_web_log).encode(), topic="web-logs")
        msg2 = FakeMessage(json.dumps(sample_app_log).encode(), topic="app-logs")
        processor.process_batch([msg1])
        processor.process_batch([msg2])
        assert processor.stats["total_processed"] == 2
        assert processor.stats["web_count"] == 1
        assert processor.stats["app_count"] == 1
