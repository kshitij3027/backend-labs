"""Tests for the Config class."""

import os
import tempfile

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


def _create_config(monkeypatch=None):
    """Helper to create a Config instance using a temp YAML file."""
    # Clear env vars to avoid interference
    if monkeypatch:
        monkeypatch.delenv("RABBITMQ_HOST", raising=False)
        monkeypatch.delenv("RABBITMQ_PORT", raising=False)
        monkeypatch.delenv("RABBITMQ_USER", raising=False)
        monkeypatch.delenv("RABBITMQ_PASS", raising=False)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_YAML, f)
        temp_path = f.name

    config = Config(config_path=temp_path)
    os.unlink(temp_path)
    return config


def test_config_loads_yaml(monkeypatch):
    """Config loads from YAML file successfully."""
    config = _create_config(monkeypatch)
    assert config._config is not None
    assert "rabbitmq" in config._config
    assert "exchanges" in config._config
    assert "queues" in config._config


def test_config_host_default(monkeypatch):
    """Host defaults to localhost."""
    config = _create_config(monkeypatch)
    assert config.host == "localhost"


def test_config_port_default(monkeypatch):
    """Port defaults to 5672."""
    config = _create_config(monkeypatch)
    assert config.port == 5672


def test_config_credentials(monkeypatch):
    """Username and password default to guest."""
    config = _create_config(monkeypatch)
    assert config.username == "guest"
    assert config.password == "guest"


def test_config_env_override_host(monkeypatch):
    """RABBITMQ_HOST env var overrides host."""
    monkeypatch.setenv("RABBITMQ_HOST", "custom-host")
    monkeypatch.delenv("RABBITMQ_PORT", raising=False)
    monkeypatch.delenv("RABBITMQ_USER", raising=False)
    monkeypatch.delenv("RABBITMQ_PASS", raising=False)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_YAML, f)
        temp_path = f.name

    config = Config(config_path=temp_path)
    os.unlink(temp_path)

    assert config.host == "custom-host"


def test_config_env_override_port(monkeypatch):
    """RABBITMQ_PORT env var overrides port as int."""
    monkeypatch.setenv("RABBITMQ_PORT", "5673")
    monkeypatch.delenv("RABBITMQ_HOST", raising=False)
    monkeypatch.delenv("RABBITMQ_USER", raising=False)
    monkeypatch.delenv("RABBITMQ_PASS", raising=False)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(SAMPLE_YAML, f)
        temp_path = f.name

    config = Config(config_path=temp_path)
    os.unlink(temp_path)

    assert config.port == 5673
    assert isinstance(config.port, int)


def test_config_get_connection_params(monkeypatch):
    """get_connection_params returns correct dict structure."""
    config = _create_config(monkeypatch)
    params = config.get_connection_params()
    assert params["host"] == "localhost"
    assert params["port"] == 5672
    assert params["credentials"]["username"] == "guest"
    assert params["credentials"]["password"] == "guest"
    assert params["heartbeat"] == 600
    assert params["blocked_connection_timeout"] == 300


def test_config_get_exchange_configs(monkeypatch):
    """get_exchange_configs returns 3 exchanges with correct types."""
    config = _create_config(monkeypatch)
    exchanges = config.get_exchange_configs()
    assert len(exchanges) == 3

    types = {e["name"]: e["type"] for e in exchanges}
    assert types["logs_direct"] == "direct"
    assert types["logs_topic"] == "topic"
    assert types["logs_fanout"] == "fanout"

    # All should be durable
    for e in exchanges:
        assert e["durable"] is True


def test_config_get_queue_configs(monkeypatch):
    """get_queue_configs returns 8 queues."""
    config = _create_config(monkeypatch)
    queues = config.get_queue_configs()
    assert len(queues) == 8

    names = [q["name"] for q in queues]
    assert "error_logs" in names
    assert "warning_logs" in names
    assert "critical_logs" in names
    assert "database_logs" in names
    assert "security_logs" in names
    assert "api_logs" in names
    assert "audit_logs" in names
    assert "all_logs" in names
