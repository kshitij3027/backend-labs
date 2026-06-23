"""Unit tests for the configuration loader (defaults -> YAML -> env precedence)."""

from __future__ import annotations

import pytest

from src.config import AppConfig, get_config, load_config


def test_defaults_match_spec(config: AppConfig) -> None:
    """Model defaults reflect project_requirements §7."""
    assert config.kmeans.n_clusters == 8
    assert config.kmeans.max_iter == 300
    assert config.kmeans.random_state == 42

    assert config.dbscan.eps == 0.3
    assert config.dbscan.min_samples == 5

    assert config.hdbscan.min_cluster_size == 10
    assert config.hdbscan.min_samples == 5

    assert config.text_features.max_features == 1000
    assert config.text_features.ngram_range == [1, 2]
    assert config.text_features.ngram_tuple == (1, 2)

    assert config.temporal_features.time_windows == [1, 5, 15, 60]
    assert config.behavioral_features.frequency_threshold == 0.01

    assert config.realtime.batch_size == 100
    assert config.realtime.update_interval == 30
    assert config.realtime.max_clusters == 50

    assert config.redis.host == "localhost"
    assert config.redis.port == 6379
    assert config.redis.db == 0

    assert config.api.host == "0.0.0.0"
    assert config.api.port == 8000
    assert config.api.debug is True


def test_get_config_is_load_config_alias() -> None:
    """``get_config`` is the same callable as ``load_config``."""
    assert get_config is load_config


def test_yaml_override_changes_value(write_yaml, monkeypatch: pytest.MonkeyPatch) -> None:
    """A YAML file deep-merges over defaults, overriding only the given fields."""
    # Clear any ambient env overrides so we isolate the YAML behavior.
    for var in ("REDIS_HOST", "REDIS_PORT", "API_HOST", "API_PORT", "API_DEBUG"):
        monkeypatch.delenv(var, raising=False)

    path = write_yaml(
        {
            "kmeans": {"n_clusters": 16},
            "api": {"port": 9001},
        }
    )
    cfg = load_config(path)

    # Overridden values.
    assert cfg.kmeans.n_clusters == 16
    assert cfg.api.port == 9001
    # Untouched sibling fields keep their defaults (deep-merge, not replace).
    assert cfg.kmeans.max_iter == 300
    assert cfg.api.host == "0.0.0.0"
    assert cfg.dbscan.eps == 0.3


def test_env_overrides_win_over_yaml(write_yaml, monkeypatch: pytest.MonkeyPatch) -> None:
    """Operational env vars take precedence over YAML values."""
    path = write_yaml(
        {
            "redis": {"host": "yaml-redis", "port": 1111},
            "api": {"port": 9001},
        }
    )
    monkeypatch.setenv("REDIS_HOST", "env-redis")
    monkeypatch.setenv("API_PORT", "9999")

    cfg = load_config(path)

    # env wins over YAML where an env override exists ...
    assert cfg.redis.host == "env-redis"
    assert cfg.api.port == 9999
    # ... and YAML still applies where no env override is set.
    assert cfg.redis.port == 1111


def test_missing_yaml_falls_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-existent YAML path yields defaults without crashing."""
    for var in ("REDIS_HOST", "REDIS_PORT", "API_HOST", "API_PORT", "API_DEBUG"):
        monkeypatch.delenv(var, raising=False)

    cfg = load_config("/nonexistent/path/does-not-exist.yaml")

    assert cfg.kmeans.n_clusters == 8
    assert cfg.api.port == 8000
    assert cfg.dbscan.eps == 0.3
