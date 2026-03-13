"""Tests for RabbitMQ exchange and queue topology setup."""

from unittest.mock import MagicMock, patch

from src.setup import RabbitMQSetup


class TestSetupExchanges:
    """Tests for exchange declaration."""

    def test_setup_exchanges_declares_three(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_exchanges(channel)
        assert channel.exchange_declare.call_count == 3

    def test_setup_exchanges_types(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_exchanges(channel)

        declared = {
            call.kwargs["exchange"]: call.kwargs["exchange_type"]
            for call in channel.exchange_declare.call_args_list
        }
        assert declared["logs_direct"] == "direct"
        assert declared["logs_topic"] == "topic"
        assert declared["logs_fanout"] == "fanout"

    def test_setup_exchanges_durable(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_exchanges(channel)

        for call in channel.exchange_declare.call_args_list:
            assert call.kwargs["durable"] is True


class TestSetupQueues:
    """Tests for queue declaration and binding."""

    def test_setup_queues_declares_eight(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_queues(channel)
        assert channel.queue_declare.call_count == 8

    def test_setup_queues_binds_to_correct_exchange(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_queues(channel)

        bindings = {
            call.kwargs["queue"]: call.kwargs["exchange"]
            for call in channel.queue_bind.call_args_list
        }

        expected = {
            "error_logs": "logs_direct",
            "warning_logs": "logs_direct",
            "critical_logs": "logs_direct",
            "database_logs": "logs_topic",
            "security_logs": "logs_topic",
            "api_logs": "logs_topic",
            "audit_logs": "logs_fanout",
            "all_logs": "logs_fanout",
        }
        assert bindings == expected

    def test_setup_direct_queue_bindings(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_queues(channel)

        bindings = {
            call.kwargs["queue"]: call.kwargs["exchange"]
            for call in channel.queue_bind.call_args_list
        }
        for queue_name in ("error_logs", "warning_logs", "critical_logs"):
            assert bindings[queue_name] == "logs_direct"

    def test_setup_topic_queue_bindings(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_queues(channel)

        bindings = {
            call.kwargs["queue"]: call.kwargs["exchange"]
            for call in channel.queue_bind.call_args_list
        }
        for queue_name in ("database_logs", "security_logs", "api_logs"):
            assert bindings[queue_name] == "logs_topic"

    def test_setup_fanout_queue_bindings(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)
        channel = MagicMock()
        setup.setup_queues(channel)

        bindings = {
            call.kwargs["queue"]: call.kwargs["exchange"]
            for call in channel.queue_bind.call_args_list
        }
        for queue_name in ("audit_logs", "all_logs"):
            assert bindings[queue_name] == "logs_fanout"


class TestSetupAll:
    """Tests for the full setup orchestration."""

    def test_setup_all_calls_both(self, mock_config):
        setup = RabbitMQSetup(config=mock_config)

        with patch.object(setup, "setup_exchanges") as mock_ex, \
             patch.object(setup, "setup_queues") as mock_q, \
             patch("src.setup.RabbitMQConnection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn_cls.return_value = mock_conn
            mock_channel = MagicMock()
            mock_conn.get_channel.return_value = mock_channel

            setup.setup_all()

            mock_ex.assert_called_once_with(mock_channel)
            mock_q.assert_called_once_with(mock_channel)
            mock_conn.close.assert_called_once()
