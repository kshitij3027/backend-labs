"""Shared test fixtures for smart log routing pipeline tests."""

import os
import tempfile
from unittest.mock import MagicMock

import pytest
import yaml

from src.config import Config


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
                "retry_delay": 2,
            },
        },
        "exchanges": [
            {"name": "logs_direct", "type": "direct", "durable": True},
            {"name": "logs_topic", "type": "topic", "durable": True},
            {"name": "logs_fanout", "type": "fanout", "durable": True},
        ],
        "queues": [
            {"name": "error_logs", "exchange": "logs_direct", "routing_key": "error", "durable": True},
            {"name": "warning_logs", "exchange": "logs_direct", "routing_key": "warning", "durable": True},
            {"name": "critical_logs", "exchange": "logs_direct", "routing_key": "critical", "durable": True},
            {"name": "database_logs", "exchange": "logs_topic", "routing_key": "database.#", "durable": True},
            {"name": "security_logs", "exchange": "logs_topic", "routing_key": "security.#", "durable": True},
            {"name": "api_logs", "exchange": "logs_topic", "routing_key": "api.*.error", "durable": True},
            {"name": "audit_logs", "exchange": "logs_fanout", "routing_key": "", "durable": True},
            {"name": "all_logs", "exchange": "logs_fanout", "routing_key": "", "durable": True},
        ],
    }


@pytest.fixture
def mock_config(sample_config_data):
    """Create a Config instance using a temp YAML file with sample_config_data."""
    # Clear env vars that would override config values
    env_vars = ["RABBITMQ_HOST", "RABBITMQ_PORT", "RABBITMQ_USER", "RABBITMQ_PASS"]
    saved = {}
    for var in env_vars:
        saved[var] = os.environ.pop(var, None)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(sample_config_data, f)
        temp_path = f.name

    config = Config(config_path=temp_path)

    yield config

    # Restore env vars
    for var, val in saved.items():
        if val is not None:
            os.environ[var] = val

    os.unlink(temp_path)


@pytest.fixture
def mock_connection():
    """Return a MagicMock of pika.BlockingConnection."""
    mock_conn = MagicMock()
    mock_conn.is_closed = False
    mock_channel = MagicMock()
    mock_channel.is_closed = False
    mock_conn.channel.return_value = mock_channel
    return mock_conn
