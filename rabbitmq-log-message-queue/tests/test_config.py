"""Tests for the Config class."""

from unittest.mock import mock_open, patch

import yaml

from src.config import Config


SAMPLE_YAML = {
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


def _create_config():
    """Helper to create a Config instance with mocked file I/O."""
    yaml_str = yaml.dump(SAMPLE_YAML)
    m = mock_open(read_data=yaml_str)
    with patch("builtins.open", m):
        return Config(config_path="fake_config.yaml")


def test_load_yaml_config(monkeypatch):
    """Verify that Config loads YAML data correctly."""
    monkeypatch.delenv("RABBITMQ_HOST", raising=False)
    config = _create_config()
    assert config.host == "localhost"
    assert config.port == 5672
    assert config.username == "guest"
    assert config.password == "guest"
    assert config.heartbeat == 600


def test_env_var_override(monkeypatch):
    """Verify that RABBITMQ_HOST env var overrides the YAML host value."""
    monkeypatch.setenv("RABBITMQ_HOST", "custom-host")
    config = _create_config()
    assert config.host == "custom-host"


def test_get_connection_params(monkeypatch):
    """Verify get_connection_params returns the correct keys and values."""
    monkeypatch.delenv("RABBITMQ_HOST", raising=False)
    config = _create_config()
    params = config.get_connection_params()
    assert params["host"] == "localhost"
    assert params["port"] == 5672
    assert params["credentials"]["username"] == "guest"
    assert params["credentials"]["password"] == "guest"
    assert params["heartbeat"] == 600
    assert params["blocked_connection_timeout"] == 300


def test_get_exchange_config():
    """Verify get_exchange_config returns the exchange configuration."""
    config = _create_config()
    exchange = config.get_exchange_config()
    assert exchange["name"] == "logs"
    assert exchange["type"] == "topic"
    assert exchange["durable"] is True


def test_get_queue_configs():
    """Verify get_queue_configs returns all 3 queue configurations."""
    config = _create_config()
    queues = config.get_queue_configs()
    assert len(queues) == 3
    names = [q["name"] for q in queues]
    assert "log_messages" in names
    assert "error_messages" in names
    assert "debug_messages" in names
