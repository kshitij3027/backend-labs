"""Tests for the Config class."""

import os
import pytest
from src.config import Config


class TestConfigYAMLLoading:
    """Test that Config correctly loads values from a YAML file."""

    def test_loads_rabbitmq_section(self, config_file, monkeypatch):
        # Clear any RABBITMQ_HOST env var that Docker might set
        monkeypatch.delenv("RABBITMQ_HOST", raising=False)
        cfg = Config(config_path=config_file)
        assert cfg.rabbitmq["host"] == "localhost"
        assert cfg.rabbitmq["port"] == 5672
        assert cfg.rabbitmq["user"] == "guest"
        assert cfg.rabbitmq["password"] == "guest"
        assert cfg.rabbitmq["heartbeat"] == 600
        assert cfg.rabbitmq["blocked_connection_timeout"] == 300

    def test_loads_exchange_section(self, config_file):
        cfg = Config(config_path=config_file)
        assert cfg.exchange["name"] == "logs"
        assert cfg.exchange["type"] == "topic"
        assert cfg.exchange["durable"] is True

    def test_loads_queue_section(self, config_file):
        cfg = Config(config_path=config_file)
        assert cfg.queue["name"] == "log_queue"
        assert cfg.queue["durable"] is True
        assert cfg.queue["routing_key"] == "log.#"

    def test_loads_dead_letter_section(self, config_file):
        cfg = Config(config_path=config_file)
        assert cfg.dead_letter["exchange"] == "logs_dlx"
        assert cfg.dead_letter["queue"] == "log_queue_dlq"

    def test_loads_batch_section(self, config_file):
        cfg = Config(config_path=config_file)
        assert cfg.batch["max_size"] == 100
        assert cfg.batch["flush_interval"] == 2.0

    def test_loads_circuit_breaker_section(self, config_file):
        cfg = Config(config_path=config_file)
        assert cfg.circuit_breaker["failure_threshold"] == 5
        assert cfg.circuit_breaker["recovery_timeout"] == 30

    def test_loads_top_level_values(self, config_file):
        cfg = Config(config_path=config_file)
        assert cfg.queue_maxsize == 10000
        assert cfg.http_port == 8080


class TestConfigEnvVarOverrides:
    """Test that environment variables override YAML values."""

    def test_rabbitmq_host_override(self, config_file, monkeypatch):
        monkeypatch.setenv("RABBITMQ_HOST", "rabbit.prod.internal")
        cfg = Config(config_path=config_file)
        assert cfg.rabbitmq["host"] == "rabbit.prod.internal"

    def test_rabbitmq_port_override(self, config_file, monkeypatch):
        monkeypatch.setenv("RABBITMQ_PORT", "5673")
        cfg = Config(config_path=config_file)
        assert cfg.rabbitmq["port"] == 5673

    def test_batch_size_override(self, config_file, monkeypatch):
        monkeypatch.setenv("BATCH_SIZE", "500")
        cfg = Config(config_path=config_file)
        assert cfg.batch["max_size"] == 500

    def test_batch_flush_interval_override(self, config_file, monkeypatch):
        monkeypatch.setenv("BATCH_FLUSH_INTERVAL", "5.0")
        cfg = Config(config_path=config_file)
        assert cfg.batch["flush_interval"] == 5.0

    def test_http_port_override(self, config_file, monkeypatch):
        monkeypatch.setenv("HTTP_PORT", "9090")
        cfg = Config(config_path=config_file)
        assert cfg.http_port == 9090

    def test_queue_maxsize_override(self, config_file, monkeypatch):
        monkeypatch.setenv("QUEUE_MAXSIZE", "50000")
        cfg = Config(config_path=config_file)
        assert cfg.queue_maxsize == 50000

    def test_multiple_overrides(self, config_file, monkeypatch):
        monkeypatch.setenv("RABBITMQ_HOST", "override-host")
        monkeypatch.setenv("BATCH_SIZE", "200")
        monkeypatch.setenv("HTTP_PORT", "3000")
        cfg = Config(config_path=config_file)
        assert cfg.rabbitmq["host"] == "override-host"
        assert cfg.batch["max_size"] == 200
        assert cfg.http_port == 3000

    def test_config_path_env_var(self, config_file, monkeypatch):
        monkeypatch.setenv("CONFIG_PATH", config_file)
        monkeypatch.delenv("RABBITMQ_HOST", raising=False)
        cfg = Config()  # Should pick up CONFIG_PATH from env
        assert cfg.rabbitmq["host"] == "localhost"


class TestConfigDefaults:
    """Test that all expected properties exist on Config."""

    def test_all_properties_exist(self, config_file):
        cfg = Config(config_path=config_file)
        # Verify all properties are accessible and return non-None values
        assert cfg.rabbitmq is not None
        assert cfg.exchange is not None
        assert cfg.queue is not None
        assert cfg.dead_letter is not None
        assert cfg.batch is not None
        assert cfg.circuit_breaker is not None
        assert cfg.queue_maxsize is not None
        assert cfg.http_port is not None
