"""Tests for configuration loading."""

from __future__ import annotations

import os

from src.config import Config


def test_config_loads_defaults(tmp_path, monkeypatch):
    """Config loads sensible defaults when no YAML exists."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("CONSUMER_NAME", raising=False)
    config = Config.load(str(tmp_path / "nonexistent.yaml"))
    assert config.redis_url == "redis://localhost:6379"
    assert config.stream_key == "logs:access"
    assert config.consumer_group == "log-processors"
    assert config.num_workers == 4
    assert config.batch_size == 100
    assert config.block_ms == 2000
    assert config.dashboard_port == 8000
    assert config.metrics_window_sec == 300
    assert config.max_retries == 3
    assert config.retry_base_delay == 1.0
    assert config.retry_max_delay == 30.0
    assert config.dlq_stream_key == "logs:dlq"
    assert config.idempotency_ttl == 3600


def test_config_loads_from_yaml(tmp_path, monkeypatch):
    """Config reads values from YAML file."""
    monkeypatch.delenv("REDIS_URL", raising=False)
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text(
        "redis_url: redis://custom:6380\n"
        "num_workers: 8\n"
        "batch_size: 50\n"
    )
    config = Config.load(str(yaml_file))
    assert config.redis_url == "redis://custom:6380"
    assert config.num_workers == 8
    assert config.batch_size == 50
    # Defaults still apply for unset values
    assert config.stream_key == "logs:access"


def test_config_env_overrides(tmp_path, monkeypatch):
    """Environment variables override YAML values."""
    yaml_file = tmp_path / "config.yaml"
    yaml_file.write_text("num_workers: 2\n")

    monkeypatch.setenv("NUM_WORKERS", "16")
    monkeypatch.setenv("REDIS_URL", "redis://env-host:6379")
    monkeypatch.setenv("RETRY_BASE_DELAY", "2.5")

    config = Config.load(str(yaml_file))
    assert config.num_workers == 16
    assert config.redis_url == "redis://env-host:6379"
    assert config.retry_base_delay == 2.5


def test_config_consumer_name_default():
    """Consumer name defaults to hostname-based value."""
    config = Config()
    assert config.consumer_name.startswith("consumer-")
