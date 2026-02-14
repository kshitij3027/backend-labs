"""Tests for the config module."""

import os
import pytest
from src.config import Config, load_config, _parse_bool, LOG_LEVELS


class TestParserBool:
    def test_true_values(self):
        for val in ("true", "True", "TRUE", "1", "yes", "YES", " true "):
            assert _parse_bool(val) is True

    def test_false_values(self):
        for val in ("false", "False", "0", "no", "NO", "", "random"):
            assert _parse_bool(val) is False


class TestLogLevels:
    def test_log_levels_order(self):
        assert LOG_LEVELS == ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    def test_log_levels_length(self):
        assert len(LOG_LEVELS) == 5


class TestConfigDefaults:
    def test_default_host(self):
        cfg = Config()
        assert cfg.host == "0.0.0.0"

    def test_default_port(self):
        cfg = Config()
        assert cfg.port == 9000

    def test_default_buffer_size(self):
        cfg = Config()
        assert cfg.buffer_size == 4096

    def test_default_min_log_level(self):
        cfg = Config()
        assert cfg.min_log_level == "INFO"

    def test_default_persistence(self):
        cfg = Config()
        assert cfg.enable_log_persistence is True

    def test_default_rate_limit(self):
        cfg = Config()
        assert cfg.rate_limit_enabled is True
        assert cfg.rate_limit_max_requests == 100
        assert cfg.rate_limit_window_seconds == 60

    def test_frozen(self):
        cfg = Config()
        with pytest.raises(AttributeError):
            cfg.port = 8080


class TestLoadConfig:
    def test_defaults(self):
        env_vars = [
            "SERVER_HOST", "SERVER_PORT", "BUFFER_SIZE", "MIN_LOG_LEVEL",
            "ENABLE_LOG_PERSISTENCE", "LOG_DIR", "LOG_FILENAME",
            "RATE_LIMIT_ENABLED", "RATE_LIMIT_MAX_REQUESTS",
            "RATE_LIMIT_WINDOW_SECONDS",
        ]
        old = {k: os.environ.pop(k, None) for k in env_vars}
        try:
            cfg = load_config()
            assert cfg.host == "0.0.0.0"
            assert cfg.port == 9000
            assert cfg.buffer_size == 4096
            assert cfg.min_log_level == "INFO"
            assert cfg.enable_log_persistence is True
            assert cfg.log_dir == "./logs"
            assert cfg.log_filename == "server.log"
            assert cfg.rate_limit_enabled is True
            assert cfg.rate_limit_max_requests == 100
            assert cfg.rate_limit_window_seconds == 60
        finally:
            for k, v in old.items():
                if v is not None:
                    os.environ[k] = v

    def test_env_overrides(self):
        overrides = {
            "SERVER_HOST": "127.0.0.1",
            "SERVER_PORT": "8080",
            "BUFFER_SIZE": "2048",
            "MIN_LOG_LEVEL": "error",
            "ENABLE_LOG_PERSISTENCE": "false",
            "LOG_DIR": "/tmp/logs",
            "LOG_FILENAME": "custom.log",
            "RATE_LIMIT_ENABLED": "0",
            "RATE_LIMIT_MAX_REQUESTS": "50",
            "RATE_LIMIT_WINDOW_SECONDS": "30",
        }
        old = {k: os.environ.get(k) for k in overrides}
        os.environ.update(overrides)
        try:
            cfg = load_config()
            assert cfg.host == "127.0.0.1"
            assert cfg.port == 8080
            assert cfg.buffer_size == 2048
            assert cfg.min_log_level == "ERROR"
            assert cfg.enable_log_persistence is False
            assert cfg.log_dir == "/tmp/logs"
            assert cfg.log_filename == "custom.log"
            assert cfg.rate_limit_enabled is False
            assert cfg.rate_limit_max_requests == 50
            assert cfg.rate_limit_window_seconds == 30
        finally:
            for k, v in old.items():
                if v is not None:
                    os.environ[k] = v
                else:
                    os.environ.pop(k, None)
