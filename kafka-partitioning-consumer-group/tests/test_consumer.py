"""Tests for consumer components."""
import json
import pytest
from unittest.mock import MagicMock
from src.monitoring.metrics import MetricsCollector
from src.consumer.rebalance_handler import RebalanceHandler
from src.consumer.log_processor import LogProcessor
from src.models import LogEntry, LogLevel


class TestRebalanceHandler:
    def test_on_assign_records_event(self):
        metrics = MetricsCollector()
        consumer = MagicMock()
        handler = RebalanceHandler(consumer, "c-0", metrics)

        # Simulate TopicPartition objects
        tp0 = MagicMock()
        tp0.partition = 0
        tp1 = MagicMock()
        tp1.partition = 1

        handler.on_assign(consumer, [tp0, tp1])

        snap = metrics.snapshot()
        assert len(snap["rebalance_events"]) == 1
        assert snap["rebalance_events"][0]["type"] == "assign"
        assert snap["rebalance_events"][0]["partitions"] == [0, 1]

    def test_on_revoke_commits_and_records(self):
        metrics = MetricsCollector()
        consumer = MagicMock()
        handler = RebalanceHandler(consumer, "c-0", metrics)

        tp0 = MagicMock()
        tp0.partition = 0

        handler.on_revoke(consumer, [tp0])

        consumer.commit.assert_called_once_with(asynchronous=False)
        snap = metrics.snapshot()
        assert snap["rebalance_events"][0]["type"] == "revoke"


class TestLogProcessor:
    def test_process_valid_message(self):
        metrics = MetricsCollector()
        processor = LogProcessor("c-0", metrics)

        entry = LogEntry(level=LogLevel.INFO, service="test", message="hello", user_id="1234")
        msg = MagicMock()
        msg.value.return_value = entry.to_kafka_value()
        msg.partition.return_value = 0

        result = processor.process(msg)
        assert result is not None
        assert result.service == "test"

        snap = metrics.snapshot()
        assert snap["total_consumed"] == 1

    def test_process_invalid_message(self):
        metrics = MetricsCollector()
        processor = LogProcessor("c-0", metrics)

        msg = MagicMock()
        msg.value.return_value = b"not valid json"
        msg.partition.return_value = 0

        result = processor.process(msg)
        assert result is None

        snap = metrics.snapshot()
        assert snap["total_errors"] == 1
