"""Tests for application configuration."""

from src.config import Settings, load_config


def test_default_values(config: Settings):
    """load_config() with no env vars should return expected defaults."""
    assert config.bootstrap_servers == "localhost:9092"
    assert config.bootstrap_servers_internal == "kafka:29092"
    assert config.use_internal_listeners is False
    assert config.topic_name == "user-profiles"
    assert config.num_users == 10
    assert config.update_interval_seconds == 1.0
    assert config.consumer_group_id == "state-consumer-group"
    assert config.dashboard_host == "0.0.0.0"
    assert config.dashboard_port == 5555
    assert config.segment_bytes == 1048576
    assert config.min_cleanable_dirty_ratio == 0.1
    assert config.delete_retention_ms == 60000
    assert config.max_compaction_lag_ms == 60000


def test_env_override(monkeypatch):
    """Environment variables should override default settings."""
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
    monkeypatch.setenv("USE_INTERNAL_LISTENERS", "true")
    monkeypatch.setenv("NUM_USERS", "50")
    monkeypatch.setenv("TOPIC_NAME", "custom-topic")
    monkeypatch.setenv("UPDATE_INTERVAL_SECONDS", "0.5")
    monkeypatch.setenv("DASHBOARD_PORT", "8080")

    cfg = load_config()
    assert cfg.bootstrap_servers == "broker1:9092,broker2:9092"
    assert cfg.use_internal_listeners is True
    assert cfg.num_users == 50
    assert cfg.topic_name == "custom-topic"
    assert cfg.update_interval_seconds == 0.5
    assert cfg.dashboard_port == 8080


def test_active_bootstrap_servers():
    """active_bootstrap_servers should return the correct servers based on listener mode."""
    external = Settings(use_internal_listeners=False)
    assert external.active_bootstrap_servers == "localhost:9092"

    internal = Settings(use_internal_listeners=True)
    assert internal.active_bootstrap_servers == "kafka:29092"
