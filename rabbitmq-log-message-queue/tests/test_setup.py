"""Tests for the RabbitMQSetup class (all mocked, no real RabbitMQ needed)."""

from unittest.mock import MagicMock, patch, call

import pytest

from src.setup import RabbitMQSetup


class TestSetupExchange:
    """Tests for exchange declaration."""

    def test_setup_exchange(self, mock_config):
        """Verify exchange_declare is called with correct topic/durable args."""
        channel = MagicMock()
        setup = RabbitMQSetup(config=mock_config)

        setup.setup_exchange(channel)

        channel.exchange_declare.assert_called_once_with(
            exchange="logs",
            exchange_type="topic",
            durable=True,
        )


class TestSetupQueues:
    """Tests for queue declaration and binding."""

    def test_setup_queues(self, mock_config):
        """Verify queue_declare is called 3 times with correct names and DLX arguments."""
        channel = MagicMock()
        setup = RabbitMQSetup(config=mock_config)

        setup.setup_queues(channel)

        expected_args = {
            "x-dead-letter-exchange": "logs_dlx",
            "x-dead-letter-routing-key": "failed",
        }

        assert channel.queue_declare.call_count == 3
        channel.queue_declare.assert_any_call(
            queue="log_messages", durable=True, arguments=expected_args
        )
        channel.queue_declare.assert_any_call(
            queue="error_messages", durable=True, arguments=expected_args
        )
        channel.queue_declare.assert_any_call(
            queue="debug_messages", durable=True, arguments=expected_args
        )

    def test_queue_bindings(self, mock_config):
        """Verify queue_bind is called 3 times with correct routing keys."""
        channel = MagicMock()
        setup = RabbitMQSetup(config=mock_config)

        setup.setup_queues(channel)

        assert channel.queue_bind.call_count == 3
        channel.queue_bind.assert_any_call(
            queue="log_messages", exchange="logs", routing_key="logs.info.*"
        )
        channel.queue_bind.assert_any_call(
            queue="error_messages", exchange="logs", routing_key="logs.error.*"
        )
        channel.queue_bind.assert_any_call(
            queue="debug_messages", exchange="logs", routing_key="logs.debug.*"
        )


class TestSetupDLX:
    """Tests for dead-letter exchange and queue setup."""

    def test_setup_dlx(self, mock_config):
        """Verify DLX exchange declared as direct/durable, DLQ declared and bound."""
        channel = MagicMock()
        setup = RabbitMQSetup(config=mock_config)

        setup.setup_dlx(channel)

        channel.exchange_declare.assert_called_once_with(
            exchange="logs_dlx",
            exchange_type="direct",
            durable=True,
        )

        channel.queue_declare.assert_called_once_with(
            queue="dead_letter_queue", durable=True
        )

        channel.queue_bind.assert_called_once_with(
            queue="dead_letter_queue",
            exchange="logs_dlx",
            routing_key="failed",
        )


class TestSetupAll:
    """Tests for the full setup_all orchestration."""

    @patch("src.setup.RabbitMQConnection")
    def test_setup_all(self, mock_conn_cls, mock_config):
        """Verify setup_all creates connection and calls all setup methods."""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_conn_cls.return_value = mock_conn
        mock_conn.get_channel.return_value = mock_channel

        setup = RabbitMQSetup(config=mock_config)
        setup.setup_all()

        # Connection lifecycle
        mock_conn_cls.assert_called_once_with(mock_config)
        mock_conn.connect.assert_called_once()
        mock_conn.get_channel.assert_called_once()
        mock_conn.close.assert_called_once()

        # DLX setup: exchange + queue + bind
        mock_channel.exchange_declare.assert_any_call(
            exchange="logs_dlx",
            exchange_type="direct",
            durable=True,
        )
        mock_channel.queue_declare.assert_any_call(
            queue="dead_letter_queue", durable=True
        )
        mock_channel.queue_bind.assert_any_call(
            queue="dead_letter_queue",
            exchange="logs_dlx",
            routing_key="failed",
        )

        # Main exchange
        mock_channel.exchange_declare.assert_any_call(
            exchange="logs",
            exchange_type="topic",
            durable=True,
        )

        # Queues: 3 declared + 3 bound (plus DLQ = 4 declares, 4 binds)
        assert mock_channel.queue_declare.call_count == 4
        assert mock_channel.queue_bind.call_count == 4
