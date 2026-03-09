"""Shared test fixtures for RabbitMQ log message queue tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sample_config_data():
    """Return a raw dict matching the YAML configuration structure."""
    return {
        "rabbitmq": {
            "host": "localhost",
            "port": 5672,
            "management_port": 15672,
            "credentials": {
                "username": "guest",
                "password": "guest",
            },
            "heartbeat": 600,
            "blocked_connection_timeout": 300,
            "connection": {
                "retry_max": 5,
                "retry_delay": 1,
            },
        },
        "exchange": {
            "name": "logs",
            "type": "topic",
            "durable": True,
        },
        "queues": [
            {"name": "log_messages", "routing_key": "logs.info.*", "durable": True},
            {"name": "error_messages", "routing_key": "logs.error.*", "durable": True},
            {"name": "debug_messages", "routing_key": "logs.debug.*", "durable": True},
        ],
        "dead_letter": {
            "exchange": "logs_dlx",
            "queue": "dead_letter_queue",
            "routing_key": "failed",
        },
    }


@pytest.fixture
def mock_config(sample_config_data):
    """Return a Config-like mock object with test values."""
    config = MagicMock()
    config.host = sample_config_data["rabbitmq"]["host"]
    config.port = sample_config_data["rabbitmq"]["port"]
    config.management_port = sample_config_data["rabbitmq"]["management_port"]
    config.username = sample_config_data["rabbitmq"]["credentials"]["username"]
    config.password = sample_config_data["rabbitmq"]["credentials"]["password"]
    config.heartbeat = sample_config_data["rabbitmq"]["heartbeat"]
    config.blocked_connection_timeout = sample_config_data["rabbitmq"]["blocked_connection_timeout"]
    config.retry_max = sample_config_data["rabbitmq"]["connection"]["retry_max"]
    config.retry_delay = sample_config_data["rabbitmq"]["connection"]["retry_delay"]

    config.get_connection_params.return_value = {
        "host": "localhost",
        "port": 5672,
        "credentials": {
            "username": "guest",
            "password": "guest",
        },
        "heartbeat": 600,
        "blocked_connection_timeout": 300,
    }

    config.get_exchange_config.return_value = sample_config_data["exchange"]
    config.get_queue_configs.return_value = sample_config_data["queues"]
    config.get_dlx_config.return_value = sample_config_data["dead_letter"]

    return config
