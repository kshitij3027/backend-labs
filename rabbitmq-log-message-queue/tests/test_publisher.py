"""Tests for the LogPublisher class."""

import json
from unittest.mock import MagicMock, patch

import pika
import pytest

from src.publisher import LogPublisher


class TestLogPublisher:
    """Tests for LogPublisher.publish and publish_batch."""

    @patch("src.publisher.RabbitMQConnection")
    def test_publish_routing_key(self, mock_conn_cls, mock_config):
        """Routing key should be logs.{level}.{source}."""
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = mock_channel

        publisher = LogPublisher(config=mock_config)
        publisher.publish("error", "api", "something failed")

        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs[1]["routing_key"] == "logs.error.api"

    @patch("src.publisher.RabbitMQConnection")
    def test_publish_message_body(self, mock_conn_cls, mock_config):
        """Message body should be JSON with timestamp, level, source, message."""
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = mock_channel

        publisher = LogPublisher(config=mock_config)
        publisher.publish("info", "web", "request received")

        call_kwargs = mock_channel.basic_publish.call_args
        body = json.loads(call_kwargs[1]["body"])

        assert "timestamp" in body
        assert body["level"] == "info"
        assert body["source"] == "web"
        assert body["message"] == "request received"

    @patch("src.publisher.RabbitMQConnection")
    def test_publish_persistence(self, mock_conn_cls, mock_config):
        """Messages should be published with delivery_mode=2 (persistent)."""
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = mock_channel

        publisher = LogPublisher(config=mock_config)
        publisher.publish("debug", "worker", "processing item")

        call_kwargs = mock_channel.basic_publish.call_args
        properties = call_kwargs[1]["properties"]

        assert properties.delivery_mode == 2
        assert properties.content_type == "application/json"

    @patch("src.publisher.RabbitMQConnection")
    def test_publish_exchange_name(self, mock_conn_cls, mock_config):
        """Messages should be published to the exchange from config."""
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = mock_channel

        publisher = LogPublisher(config=mock_config)
        publisher.publish("info", "web", "hello")

        call_kwargs = mock_channel.basic_publish.call_args
        assert call_kwargs[1]["exchange"] == "logs"

    @patch("src.publisher.RabbitMQConnection")
    def test_publish_batch(self, mock_conn_cls, mock_config):
        """publish_batch should publish all messages in the list."""
        mock_channel = MagicMock()
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = mock_channel

        publisher = LogPublisher(config=mock_config)
        messages = [
            {"level": "info", "source": "web", "message": "msg1"},
            {"level": "error", "source": "api", "message": "msg2"},
            {"level": "debug", "source": "worker", "message": "msg3"},
        ]
        publisher.publish_batch(messages)

        assert mock_channel.basic_publish.call_count == 3

        # Verify each message was published with correct routing key
        calls = mock_channel.basic_publish.call_args_list
        routing_keys = [c[1]["routing_key"] for c in calls]
        assert routing_keys == ["logs.info.web", "logs.error.api", "logs.debug.worker"]

        # Verify each body is valid JSON with correct fields
        for i, call in enumerate(calls):
            body = json.loads(call[1]["body"])
            assert body["level"] == messages[i]["level"]
            assert body["source"] == messages[i]["source"]
            assert body["message"] == messages[i]["message"]
            assert "timestamp" in body

    @patch("src.publisher.RabbitMQConnection")
    def test_publish_closes_connection(self, mock_conn_cls, mock_config):
        """Connection should be closed after publishing."""
        mock_conn = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = MagicMock()

        publisher = LogPublisher(config=mock_config)
        publisher.publish("info", "web", "test")

        mock_conn.close.assert_called_once()
