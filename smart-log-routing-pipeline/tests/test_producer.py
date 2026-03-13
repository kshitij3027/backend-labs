"""Tests for LogProducer with mocked pika channel."""

from unittest.mock import MagicMock, patch

import pika
import pytest

from src.models.log_message import LogMessage
from src.producer import LogProducer


@pytest.fixture
def sample_message():
    """Return a fixed LogMessage for testing."""
    return LogMessage(
        timestamp="2026-03-12T10:00:00+00:00",
        service="database",
        component="postgres",
        level="error",
        message="Connection refused to backend service",
        metadata={"source_ip": "192.168.1.10", "request_id": "test-uuid"},
    )


@pytest.fixture
def producer_with_mock_channel(mock_config):
    """Create a LogProducer with a mocked channel."""
    producer = LogProducer(config=mock_config)
    mock_channel = MagicMock()
    producer._channel = mock_channel
    return producer, mock_channel


class TestPublishToDirect:
    """Tests for direct exchange publishing."""

    def test_publish_to_direct_uses_correct_exchange(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_direct(sample_message)
        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs.kwargs.get("exchange") or call_kwargs[1].get("exchange") \
            if call_kwargs[1] else call_kwargs[0][0] if call_kwargs[0] else None
        # More robust assertion
        mock_channel.basic_publish.assert_called_once()
        _, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["exchange"] == "logs_direct"

    def test_publish_to_direct_uses_level_as_routing_key(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_direct(sample_message)
        _, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["routing_key"] == "error"


class TestPublishToTopic:
    """Tests for topic exchange publishing."""

    def test_publish_to_topic_uses_correct_exchange(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_topic(sample_message)
        _, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["exchange"] == "logs_topic"

    def test_publish_to_topic_uses_full_routing_key(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_topic(sample_message)
        _, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["routing_key"] == "database.postgres.error"


class TestPublishToFanout:
    """Tests for fanout exchange publishing."""

    def test_publish_to_fanout_uses_correct_exchange(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_fanout(sample_message)
        _, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["exchange"] == "logs_fanout"

    def test_publish_to_fanout_uses_empty_routing_key(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_fanout(sample_message)
        _, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["routing_key"] == ""


class TestPublishProperties:
    """Tests for message properties and combined publish."""

    def test_publish_delivery_mode_persistent(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_direct(sample_message)
        _, kwargs = mock_channel.basic_publish.call_args
        props = kwargs["properties"]
        assert props.delivery_mode == 2

    def test_publish_to_all_calls_all_three(
        self, producer_with_mock_channel, sample_message
    ):
        producer, mock_channel = producer_with_mock_channel
        producer.publish_to_all(sample_message)
        assert mock_channel.basic_publish.call_count == 3
        exchanges_used = [
            call.kwargs["exchange"]
            for call in mock_channel.basic_publish.call_args_list
        ]
        assert "logs_direct" in exchanges_used
        assert "logs_topic" in exchanges_used
        assert "logs_fanout" in exchanges_used
