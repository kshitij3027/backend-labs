"""Tests for specialized log consumers."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.consumers.base_consumer import BaseConsumer
from src.consumers.error_consumer import ErrorConsumer
from src.consumers.security_consumer import SecurityConsumer
from src.consumers.database_consumer import DatabaseConsumer
from src.consumers.audit_consumer import AuditConsumer


SAMPLE_MESSAGE = {
    "timestamp": "2026-03-12T10:00:00",
    "service": "test",
    "component": "test",
    "level": "error",
    "message": "Test message",
    "routing_key": "test.test.error",
    "metadata": {"source_ip": "1.2.3.4", "request_id": "abc"},
}


@pytest.fixture
def mock_channel():
    """Return a MagicMock channel with delivery_tag support."""
    channel = MagicMock()
    return channel


@pytest.fixture
def mock_method():
    """Return a MagicMock method frame with a delivery_tag."""
    method = MagicMock()
    method.delivery_tag = 1
    return method


def _make_consumer(consumer_cls, mock_config):
    """Create a consumer instance with a mocked connection manager."""
    consumer = consumer_cls(config=mock_config)
    consumer._conn_manager = MagicMock()
    mock_ch = MagicMock()
    consumer._conn_manager.get_channel.return_value = mock_ch
    return consumer, mock_ch


class TestBaseConsumerAbstract:
    """Tests for BaseConsumer ABC enforcement."""

    def test_base_consumer_is_abstract(self):
        """Cannot instantiate BaseConsumer directly."""
        with pytest.raises(TypeError):
            BaseConsumer(queue_name="test")


class TestConsumerQueueNames:
    """Tests that each consumer sets the correct queue_name."""

    def test_error_consumer_queue_name(self, mock_config):
        consumer = ErrorConsumer(config=mock_config)
        assert consumer.queue_name == "error_logs"

    def test_security_consumer_queue_name(self, mock_config):
        consumer = SecurityConsumer(config=mock_config)
        assert consumer.queue_name == "security_logs"

    def test_database_consumer_queue_name(self, mock_config):
        consumer = DatabaseConsumer(config=mock_config)
        assert consumer.queue_name == "database_logs"

    def test_audit_consumer_queue_name(self, mock_config):
        consumer = AuditConsumer(config=mock_config)
        assert consumer.queue_name == "audit_logs"


class TestConsumerOnMessage:
    """Tests for the _on_message callback and stats tracking."""

    def test_consumer_process_calls_ack(self, mock_config, mock_channel, mock_method):
        """Verify basic_ack is called after successful processing."""
        consumer, _ = _make_consumer(ErrorConsumer, mock_config)
        body = json.dumps(SAMPLE_MESSAGE).encode()
        consumer._on_message(mock_channel, mock_method, None, body)
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)

    def test_consumer_stats_tracking(self, mock_config, mock_channel, mock_method):
        """Verify stats increment after processing a message."""
        consumer, _ = _make_consumer(ErrorConsumer, mock_config)
        body = json.dumps(SAMPLE_MESSAGE).encode()
        consumer._on_message(mock_channel, mock_method, None, body)
        assert consumer.stats["processed"] == 1
        assert consumer.stats["errors"] == 0

        # Process a second message
        mock_method_2 = MagicMock()
        mock_method_2.delivery_tag = 2
        consumer._on_message(mock_channel, mock_method_2, None, body)
        assert consumer.stats["processed"] == 2


class TestConsumerConnect:
    """Tests for the connect method and QoS configuration."""

    def test_consumer_prefetch_count(self, mock_config):
        """Verify basic_qos is called with prefetch_count=10."""
        consumer, mock_ch = _make_consumer(ErrorConsumer, mock_config)
        consumer._channel = mock_ch
        # Simulate connect behavior
        consumer.connect()
        new_ch = consumer._conn_manager.get_channel.return_value
        new_ch.basic_qos.assert_called_with(prefetch_count=10)


class TestConsumerProcess:
    """Tests that each consumer's process() runs without error."""

    def test_error_consumer_process(self, mock_config):
        consumer = ErrorConsumer(config=mock_config)
        consumer.process(SAMPLE_MESSAGE)  # should not raise

    def test_security_consumer_process(self, mock_config):
        consumer = SecurityConsumer(config=mock_config)
        consumer.process(SAMPLE_MESSAGE)  # should not raise

    def test_database_consumer_process(self, mock_config):
        consumer = DatabaseConsumer(config=mock_config)
        consumer.process(SAMPLE_MESSAGE)  # should not raise

    def test_audit_consumer_process(self, mock_config):
        consumer = AuditConsumer(config=mock_config)
        consumer.process(SAMPLE_MESSAGE)  # should not raise


class TestConsumerGetStats:
    """Tests for the get_stats method."""

    def test_get_stats_returns_expected_keys(self, mock_config):
        consumer = ErrorConsumer(config=mock_config)
        stats = consumer.get_stats()
        assert "processed" in stats
        assert "errors" in stats
        assert "uptime" in stats
        assert "messages_per_sec" in stats

    def test_get_stats_rate_after_processing(self, mock_config, mock_channel, mock_method):
        consumer, _ = _make_consumer(ErrorConsumer, mock_config)
        body = json.dumps(SAMPLE_MESSAGE).encode()
        consumer._on_message(mock_channel, mock_method, None, body)
        stats = consumer.get_stats()
        assert stats["processed"] == 1
        assert stats["messages_per_sec"] >= 0
