"""Unit tests for ``src.config.AppConfig`` defaults and env overrides."""

from __future__ import annotations

from src.config import (
    DEFAULT_ALLOW_KILL_ENDPOINT,
    DEFAULT_FAILOVER_TIMEOUT_SEC,
    DEFAULT_HEALTH_CHECK_INTERVAL_SEC,
    DEFAULT_HOST,
    DEFAULT_LOG_LEVEL,
    DEFAULT_MAX_LOGS_RETURNED,
    DEFAULT_PORT,
    DEFAULT_PRIMARY_PREFERENCE,
    DEFAULT_REGIONS,
    DEFAULT_REPLICATION_LAG_TARGET_MS,
    DEFAULT_WEBSOCKET_PUSH_INTERVAL_SEC,
    AppConfig,
)


def test_defaults_match_spec(app_config: AppConfig):
    assert app_config.host == DEFAULT_HOST == "0.0.0.0"
    assert app_config.port == DEFAULT_PORT == 8000
    assert app_config.regions == DEFAULT_REGIONS == ["us-east", "europe", "asia"]
    assert (
        app_config.primary_preference
        == DEFAULT_PRIMARY_PREFERENCE
        == ["us-east", "europe", "asia"]
    )
    assert (
        app_config.replication_lag_target_ms
        == DEFAULT_REPLICATION_LAG_TARGET_MS
        == 100
    )
    assert (
        app_config.websocket_push_interval_sec
        == DEFAULT_WEBSOCKET_PUSH_INTERVAL_SEC
        == 5.0
    )
    assert (
        app_config.health_check_interval_sec
        == DEFAULT_HEALTH_CHECK_INTERVAL_SEC
        == 1.0
    )
    assert (
        app_config.failover_timeout_sec == DEFAULT_FAILOVER_TIMEOUT_SEC == 5.0
    )
    assert app_config.max_logs_returned == DEFAULT_MAX_LOGS_RETURNED == 25
    assert app_config.log_level == DEFAULT_LOG_LEVEL == "INFO"
    assert app_config.allow_kill_endpoint == DEFAULT_ALLOW_KILL_ENDPOINT is True


def test_regions_parsed_from_env():
    cfg = AppConfig.from_env(env={"REGIONS": "us,eu,ap"})
    assert cfg.regions == ["us", "eu", "ap"]


def test_primary_preference_parsed_from_env():
    cfg = AppConfig.from_env(
        env={"PRIMARY_PREFERENCE": "europe,asia,us-east"}
    )
    assert cfg.primary_preference == ["europe", "asia", "us-east"]


def test_log_level_overridden_from_env():
    cfg = AppConfig.from_env(env={"LOG_LEVEL": "debug"})
    # LOG_LEVEL is uppercased — uvicorn's own level is lowercased separately.
    assert cfg.log_level == "DEBUG"


def test_port_parsed_from_env_string():
    cfg = AppConfig.from_env(env={"PORT": "9090"})
    assert cfg.port == 9090


def test_invalid_port_falls_back_to_default():
    cfg = AppConfig.from_env(env={"PORT": "not-a-number"})
    assert cfg.port == DEFAULT_PORT


def test_csv_with_whitespace_and_empty_tokens_trimmed():
    cfg = AppConfig.from_env(env={"REGIONS": " us-east , europe ,, asia "})
    assert cfg.regions == ["us-east", "europe", "asia"]


def test_empty_regions_env_falls_back_to_default():
    cfg = AppConfig.from_env(env={"REGIONS": "   "})
    assert cfg.regions == DEFAULT_REGIONS


def test_allow_kill_endpoint_parsed_as_bool():
    truthy = AppConfig.from_env(env={"ALLOW_KILL_ENDPOINT": "true"})
    falsy = AppConfig.from_env(env={"ALLOW_KILL_ENDPOINT": "false"})
    assert truthy.allow_kill_endpoint is True
    assert falsy.allow_kill_endpoint is False


def test_replication_lag_target_overridden():
    cfg = AppConfig.from_env(env={"REPLICATION_LAG_TARGET_MS": "250"})
    assert cfg.replication_lag_target_ms == 250


def test_websocket_push_interval_float_parsing():
    cfg = AppConfig.from_env(env={"WEBSOCKET_PUSH_INTERVAL_SEC": "2.5"})
    assert cfg.websocket_push_interval_sec == 2.5
