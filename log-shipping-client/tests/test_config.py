"""Tests for config module."""

import os
import pytest
from src.config import Config, load_config, _parse_bool


class TestParseBook:
    def test_true_values(self):
        for val in ("true", "True", "TRUE", "1", "yes", "YES"):
            assert _parse_bool(val) is True

    def test_false_values(self):
        for val in ("false", "False", "0", "no", "NO", ""):
            assert _parse_bool(val) is False

    def test_whitespace_stripped(self):
        assert _parse_bool("  true  ") is True


class TestConfigDefaults:
    def test_defaults(self):
        cfg = Config()
        assert cfg.log_file == "/var/log/app.log"
        assert cfg.server_host == "localhost"
        assert cfg.server_port == 9000
        assert cfg.batch_mode is True
        assert cfg.compress is False
        assert cfg.batch_size == 1
        assert cfg.metrics_interval == 0
        assert cfg.poll_interval == 0.5
        assert cfg.buffer_size == 50000
        assert cfg.resilient is False

    def test_frozen(self):
        cfg = Config()
        with pytest.raises(AttributeError):
            cfg.server_port = 1234


class TestLoadConfigCLI:
    def test_cli_overrides(self):
        argv = [
            "--server-host", "10.0.0.1",
            "--server-port", "8080",
            "--mode", "continuous",
            "--compress",
            "--batch-size", "10",
            "--resilient",
        ]
        cfg = load_config(argv)
        assert cfg.server_host == "10.0.0.1"
        assert cfg.server_port == 8080
        assert cfg.batch_mode is False
        assert cfg.compress is True
        assert cfg.batch_size == 10
        assert cfg.resilient is True

    def test_cli_equals_syntax(self):
        argv = ["--server-host=192.168.1.1", "--server-port=7777"]
        cfg = load_config(argv)
        assert cfg.server_host == "192.168.1.1"
        assert cfg.server_port == 7777

    def test_empty_argv(self):
        cfg = load_config([])
        assert cfg.server_host == "localhost"
        assert cfg.server_port == 9000

    def test_log_file_cli(self):
        cfg = load_config(["--log-file", "/tmp/test.log"])
        assert cfg.log_file == "/tmp/test.log"


class TestLoadConfigEnv:
    def test_env_vars(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "env-host")
        monkeypatch.setenv("SERVER_PORT", "5555")
        monkeypatch.setenv("SHIPPING_MODE", "continuous")
        monkeypatch.setenv("COMPRESS", "true")
        monkeypatch.setenv("LOG_FILE", "/env/log.txt")
        monkeypatch.setenv("BATCH_SIZE", "20")
        monkeypatch.setenv("METRICS_INTERVAL", "30")
        cfg = load_config([])
        assert cfg.server_host == "env-host"
        assert cfg.server_port == 5555
        assert cfg.batch_mode is False
        assert cfg.compress is True
        assert cfg.log_file == "/env/log.txt"
        assert cfg.batch_size == 20
        assert cfg.metrics_interval == 30

    def test_cli_overrides_env(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "env-host")
        monkeypatch.setenv("SERVER_PORT", "5555")
        cfg = load_config(["--server-host", "cli-host", "--server-port", "6666"])
        assert cfg.server_host == "cli-host"
        assert cfg.server_port == 6666
