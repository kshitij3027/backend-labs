"""Tests for src.config — default values and environment variable overrides."""

from __future__ import annotations

from src.config import Config, get_config


class TestConfigDefaults:
    """Verify that get_config() returns the expected defaults."""

    def test_redis_host_default(self, monkeypatch) -> None:
        monkeypatch.delenv("REDIS_HOST", raising=False)
        cfg = get_config()
        assert cfg.redis_host == "localhost"

    def test_redis_port_default(self) -> None:
        cfg = get_config()
        assert cfg.redis_port == 6379

    def test_server_host_default(self) -> None:
        cfg = get_config()
        assert cfg.server_host == "0.0.0.0"

    def test_server_port_default(self) -> None:
        cfg = get_config()
        assert cfg.server_port == 8000

    def test_anomaly_zscore_threshold_default(self) -> None:
        cfg = get_config()
        assert cfg.anomaly_zscore_threshold == 2.5

    def test_trend_window_minutes_default(self) -> None:
        cfg = get_config()
        assert cfg.trend_window_minutes == 5.0

    def test_metric_ttl_seconds_default(self) -> None:
        cfg = get_config()
        assert cfg.metric_ttl_seconds == 3600

    def test_ws_heartbeat_interval_default(self) -> None:
        cfg = get_config()
        assert cfg.ws_heartbeat_interval == 30.0

    def test_ws_broadcast_interval_default(self) -> None:
        cfg = get_config()
        assert cfg.ws_broadcast_interval == 5.0

    def test_config_is_frozen(self) -> None:
        cfg = get_config()
        assert isinstance(cfg, Config)
        # Frozen dataclass should reject attribute assignment.
        try:
            cfg.redis_host = "other"  # type: ignore[misc]
            assert False, "Expected FrozenInstanceError"
        except AttributeError:
            pass


class TestConfigEnvOverrides:
    """Verify that environment variables override defaults."""

    def test_redis_host_override(self, monkeypatch) -> None:
        monkeypatch.setenv("REDIS_HOST", "redis-cluster")
        cfg = get_config()
        assert cfg.redis_host == "redis-cluster"

    def test_redis_port_override(self, monkeypatch) -> None:
        monkeypatch.setenv("REDIS_PORT", "6380")
        cfg = get_config()
        assert cfg.redis_port == 6380

    def test_server_port_override(self, monkeypatch) -> None:
        monkeypatch.setenv("SERVER_PORT", "9000")
        cfg = get_config()
        assert cfg.server_port == 9000

    def test_anomaly_zscore_threshold_override(self, monkeypatch) -> None:
        monkeypatch.setenv("ANOMALY_ZSCORE_THRESHOLD", "3.0")
        cfg = get_config()
        assert cfg.anomaly_zscore_threshold == 3.0

    def test_trend_window_minutes_override(self, monkeypatch) -> None:
        monkeypatch.setenv("TREND_WINDOW_MINUTES", "10")
        cfg = get_config()
        assert cfg.trend_window_minutes == 10.0

    def test_metric_ttl_seconds_override(self, monkeypatch) -> None:
        monkeypatch.setenv("METRIC_TTL_SECONDS", "7200")
        cfg = get_config()
        assert cfg.metric_ttl_seconds == 7200

    def test_ws_heartbeat_interval_override(self, monkeypatch) -> None:
        monkeypatch.setenv("WS_HEARTBEAT_INTERVAL", "15")
        cfg = get_config()
        assert cfg.ws_heartbeat_interval == 15.0

    def test_ws_broadcast_interval_override(self, monkeypatch) -> None:
        monkeypatch.setenv("WS_BROADCAST_INTERVAL", "2")
        cfg = get_config()
        assert cfg.ws_broadcast_interval == 2.0

    def test_empty_env_var_uses_default(self, monkeypatch) -> None:
        monkeypatch.setenv("REDIS_HOST", "")
        cfg = get_config()
        assert cfg.redis_host == "localhost"
