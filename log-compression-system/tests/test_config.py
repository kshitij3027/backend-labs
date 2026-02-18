"""Tests for src/config.py — ServerConfig, ClientConfig, and their loaders."""

import dataclasses
import pytest

from src.config import (
    ServerConfig,
    ClientConfig,
    load_server_config,
    load_client_config,
    _parse_bool,
)


# ── _parse_bool helper ──────────────────────────────────────────────

class TestParseBool:
    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", "YES", " true "])
    def test_truthy_values(self, value):
        assert _parse_bool(value) is True

    @pytest.mark.parametrize("value", ["false", "False", "0", "no", "NO", "", "random"])
    def test_falsy_values(self, value):
        assert _parse_bool(value) is False


# ── ServerConfig defaults ────────────────────────────────────────────

class TestServerConfigDefaults:
    def test_default_host(self):
        cfg = ServerConfig()
        assert cfg.host == "0.0.0.0"

    def test_default_port(self):
        cfg = ServerConfig()
        assert cfg.port == 5000


# ── ServerConfig frozen ──────────────────────────────────────────────

class TestServerConfigFrozen:
    def test_cannot_set_host(self):
        cfg = ServerConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.host = "127.0.0.1"

    def test_cannot_set_port(self):
        cfg = ServerConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.port = 9999


# ── load_server_config ───────────────────────────────────────────────

class TestLoadServerConfig:
    def test_defaults_without_env(self, monkeypatch):
        monkeypatch.delenv("SERVER_HOST", raising=False)
        monkeypatch.delenv("SERVER_PORT", raising=False)
        cfg = load_server_config()
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 5000

    def test_env_override_host(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "192.168.1.10")
        cfg = load_server_config()
        assert cfg.host == "192.168.1.10"

    def test_env_override_port(self, monkeypatch):
        monkeypatch.setenv("SERVER_PORT", "8080")
        cfg = load_server_config()
        assert cfg.port == 8080

    def test_env_override_both(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "10.0.0.1")
        monkeypatch.setenv("SERVER_PORT", "3000")
        cfg = load_server_config()
        assert cfg.host == "10.0.0.1"
        assert cfg.port == 3000


# ── ClientConfig defaults ────────────────────────────────────────────

class TestClientConfigDefaults:
    def test_all_defaults(self):
        cfg = ClientConfig()
        assert cfg.server_host == "localhost"
        assert cfg.server_port == 5000
        assert cfg.batch_size == 50
        assert cfg.batch_interval == 5.0
        assert cfg.compression_enabled is True
        assert cfg.compression_algorithm == "gzip"
        assert cfg.compression_level == 6
        assert cfg.log_rate == 100
        assert cfg.run_time == 30
        assert cfg.bypass_threshold == 256
        assert cfg.adaptive_enabled is False
        assert cfg.adaptive_min_level == 1
        assert cfg.adaptive_max_level == 9
        assert cfg.adaptive_check_interval == 5.0


# ── ClientConfig frozen ──────────────────────────────────────────────

class TestClientConfigFrozen:
    def test_cannot_set_server_host(self):
        cfg = ClientConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.server_host = "other"

    def test_cannot_set_batch_size(self):
        cfg = ClientConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.batch_size = 999

    def test_cannot_set_compression_enabled(self):
        cfg = ClientConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.compression_enabled = False


# ── load_client_config — env var overrides ───────────────────────────

class TestLoadClientConfigEnv:
    def test_defaults_without_env_or_args(self, monkeypatch):
        # Clear all relevant env vars
        for var in [
            "SERVER_HOST", "SERVER_PORT", "BATCH_SIZE", "BATCH_INTERVAL",
            "COMPRESSION_ENABLED", "COMPRESSION_ALGORITHM", "COMPRESSION_LEVEL",
            "LOG_RATE", "RUN_TIME", "BYPASS_THRESHOLD", "ADAPTIVE_ENABLED",
            "ADAPTIVE_MIN_LEVEL", "ADAPTIVE_MAX_LEVEL", "ADAPTIVE_CHECK_INTERVAL",
        ]:
            monkeypatch.delenv(var, raising=False)

        cfg = load_client_config(argv=[])
        assert cfg.server_host == "localhost"
        assert cfg.server_port == 5000
        assert cfg.batch_size == 50
        assert cfg.batch_interval == 5.0
        assert cfg.compression_enabled is True
        assert cfg.compression_algorithm == "gzip"
        assert cfg.compression_level == 6
        assert cfg.log_rate == 100
        assert cfg.run_time == 30
        assert cfg.bypass_threshold == 256
        assert cfg.adaptive_enabled is False

    def test_env_override_server_host(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "10.0.0.5")
        cfg = load_client_config(argv=[])
        assert cfg.server_host == "10.0.0.5"

    def test_env_override_server_port(self, monkeypatch):
        monkeypatch.setenv("SERVER_PORT", "7777")
        cfg = load_client_config(argv=[])
        assert cfg.server_port == 7777

    def test_env_override_batch_size(self, monkeypatch):
        monkeypatch.setenv("BATCH_SIZE", "200")
        cfg = load_client_config(argv=[])
        assert cfg.batch_size == 200

    def test_env_override_batch_interval(self, monkeypatch):
        monkeypatch.setenv("BATCH_INTERVAL", "10.5")
        cfg = load_client_config(argv=[])
        assert cfg.batch_interval == 10.5

    def test_env_override_compression_enabled_false(self, monkeypatch):
        monkeypatch.setenv("COMPRESSION_ENABLED", "false")
        cfg = load_client_config(argv=[])
        assert cfg.compression_enabled is False

    def test_env_override_compression_algorithm(self, monkeypatch):
        monkeypatch.setenv("COMPRESSION_ALGORITHM", "lz4")
        cfg = load_client_config(argv=[])
        assert cfg.compression_algorithm == "lz4"

    def test_env_override_compression_level(self, monkeypatch):
        monkeypatch.setenv("COMPRESSION_LEVEL", "9")
        cfg = load_client_config(argv=[])
        assert cfg.compression_level == 9

    def test_env_override_log_rate(self, monkeypatch):
        monkeypatch.setenv("LOG_RATE", "500")
        cfg = load_client_config(argv=[])
        assert cfg.log_rate == 500

    def test_env_override_run_time(self, monkeypatch):
        monkeypatch.setenv("RUN_TIME", "60")
        cfg = load_client_config(argv=[])
        assert cfg.run_time == 60

    def test_env_override_bypass_threshold(self, monkeypatch):
        monkeypatch.setenv("BYPASS_THRESHOLD", "512")
        cfg = load_client_config(argv=[])
        assert cfg.bypass_threshold == 512

    def test_env_override_adaptive_enabled(self, monkeypatch):
        monkeypatch.setenv("ADAPTIVE_ENABLED", "true")
        cfg = load_client_config(argv=[])
        assert cfg.adaptive_enabled is True

    def test_env_override_adaptive_min_level(self, monkeypatch):
        monkeypatch.setenv("ADAPTIVE_MIN_LEVEL", "3")
        cfg = load_client_config(argv=[])
        assert cfg.adaptive_min_level == 3

    def test_env_override_adaptive_max_level(self, monkeypatch):
        monkeypatch.setenv("ADAPTIVE_MAX_LEVEL", "7")
        cfg = load_client_config(argv=[])
        assert cfg.adaptive_max_level == 7

    def test_env_override_adaptive_check_interval(self, monkeypatch):
        monkeypatch.setenv("ADAPTIVE_CHECK_INTERVAL", "15.0")
        cfg = load_client_config(argv=[])
        assert cfg.adaptive_check_interval == 15.0


# ── load_client_config — CLI arg overrides ───────────────────────────

class TestLoadClientConfigCLI:
    def test_cli_override_server_host(self):
        cfg = load_client_config(argv=["--server-host", "cli-host"])
        assert cfg.server_host == "cli-host"

    def test_cli_override_server_port(self):
        cfg = load_client_config(argv=["--server-port", "9999"])
        assert cfg.server_port == 9999

    def test_cli_override_batch_size(self):
        cfg = load_client_config(argv=["--batch-size", "300"])
        assert cfg.batch_size == 300

    def test_cli_override_batch_interval(self):
        cfg = load_client_config(argv=["--batch-interval", "2.5"])
        assert cfg.batch_interval == 2.5

    def test_cli_override_compression_algorithm(self):
        cfg = load_client_config(argv=["--compression-algorithm", "zstd"])
        assert cfg.compression_algorithm == "zstd"

    def test_cli_override_compression_level(self):
        cfg = load_client_config(argv=["--compression-level", "3"])
        assert cfg.compression_level == 3

    def test_cli_override_log_rate(self):
        cfg = load_client_config(argv=["--log-rate", "1000"])
        assert cfg.log_rate == 1000

    def test_cli_override_run_time(self):
        cfg = load_client_config(argv=["--run-time", "120"])
        assert cfg.run_time == 120

    def test_no_compress_flag(self):
        cfg = load_client_config(argv=["--no-compress"])
        assert cfg.compression_enabled is False

    def test_no_compress_flag_absent(self):
        cfg = load_client_config(argv=[])
        assert cfg.compression_enabled is True

    def test_cli_overrides_env(self, monkeypatch):
        """CLI args take precedence over environment variables."""
        monkeypatch.setenv("SERVER_HOST", "env-host")
        monkeypatch.setenv("SERVER_PORT", "1111")
        cfg = load_client_config(argv=["--server-host", "cli-host", "--server-port", "2222"])
        assert cfg.server_host == "cli-host"
        assert cfg.server_port == 2222

    def test_multiple_cli_args(self):
        cfg = load_client_config(argv=[
            "--server-host", "multi-host",
            "--batch-size", "75",
            "--compression-level", "1",
            "--log-rate", "250",
        ])
        assert cfg.server_host == "multi-host"
        assert cfg.batch_size == 75
        assert cfg.compression_level == 1
        assert cfg.log_rate == 250
