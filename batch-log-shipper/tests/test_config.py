"""Tests for the configuration module."""

import pytest
from src.config import ServerConfig, load_server_config, ClientConfig, load_client_config


def test_server_config_defaults():
    cfg = ServerConfig()
    assert cfg.host == "0.0.0.0"
    assert cfg.port == 9999
    assert cfg.buffer_size == 65535


def test_server_config_from_env(monkeypatch):
    monkeypatch.setenv("SERVER_HOST", "127.0.0.1")
    monkeypatch.setenv("SERVER_PORT", "8888")
    monkeypatch.setenv("BUFFER_SIZE", "32768")

    cfg = load_server_config()
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 8888
    assert cfg.buffer_size == 32768


def test_client_config_defaults():
    cfg = ClientConfig()
    assert cfg.target_host == "localhost"
    assert cfg.target_port == 9999
    assert cfg.batch_size == 10
    assert cfg.flush_interval == 5.0
    assert cfg.compress is True
    assert cfg.max_retries == 3
    assert cfg.logs_per_second == 5
    assert cfg.run_time == 30


def test_client_config_from_env(monkeypatch):
    monkeypatch.setenv("TARGET_HOST", "192.168.1.1")
    monkeypatch.setenv("TARGET_PORT", "7777")
    monkeypatch.setenv("BATCH_SIZE", "50")
    monkeypatch.setenv("FLUSH_INTERVAL", "2.5")
    monkeypatch.setenv("COMPRESS", "false")
    monkeypatch.setenv("MAX_RETRIES", "5")
    monkeypatch.setenv("LOGS_PER_SECOND", "10")
    monkeypatch.setenv("RUN_TIME", "60")

    cfg = load_client_config([])
    assert cfg.target_host == "192.168.1.1"
    assert cfg.target_port == 7777
    assert cfg.batch_size == 50
    assert cfg.flush_interval == 2.5
    assert cfg.compress is False
    assert cfg.max_retries == 5
    assert cfg.logs_per_second == 10
    assert cfg.run_time == 60


def test_client_config_cli_args():
    cfg = load_client_config(["--batch-size", "20", "--batch-interval", "3.0"])
    assert cfg.batch_size == 20
    assert cfg.flush_interval == 3.0


def test_client_config_cli_overrides_env(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "50")
    cfg = load_client_config(["--batch-size", "20"])
    assert cfg.batch_size == 20


def test_server_config_frozen():
    cfg = ServerConfig()
    with pytest.raises(AttributeError):
        cfg.port = 1234
