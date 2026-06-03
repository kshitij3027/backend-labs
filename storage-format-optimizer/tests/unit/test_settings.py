"""Unit tests for ``src.settings.Settings`` and ``get_settings``.

Verifies a high-value subset of defaults, field types, environment-variable
overrides (case-insensitive), and the ``get_settings`` lru_cache singleton.
"""
from __future__ import annotations

from src.settings import Settings, get_settings


def test_default_values(settings: Settings) -> None:
    """A representative, high-value subset of defaults matches the spec."""
    assert settings.data_dir == "./data"
    assert settings.partition_bucket_seconds == 3600
    assert settings.analytical_max_columns == 3
    assert settings.full_record_min_columns == 10
    assert settings.select_min_rows == 256
    assert settings.select_min_confidence == 0.6
    assert settings.migration_interval_seconds == 5.0
    assert settings.columnar_default_codec == "SNAPPY"
    assert settings.compression_learn_enabled is True
    assert settings.index_min_filter_hits == 5
    assert settings.metrics_history_points == 60
    assert settings.ws_push_interval_seconds == 2.0
    assert settings.api_port == 8000
    assert settings.log_level == "INFO"


def test_field_types(settings: Settings) -> None:
    """Selected fields have the expected concrete Python types."""
    assert isinstance(settings.partition_bucket_seconds, int)
    assert isinstance(settings.select_write_ratio_row, float)
    assert isinstance(settings.compression_learn_enabled, bool)
    # bool is a subclass of int in Python; assert an int field is NOT a bool.
    assert not isinstance(settings.partition_bucket_seconds, bool)


def test_env_override_uppercase(monkeypatch) -> None:
    """Uppercase env vars override defaults (case-insensitive config)."""
    monkeypatch.setenv("API_PORT", "9999")
    monkeypatch.setenv("PARTITION_BUCKET_SECONDS", "120")
    fresh = Settings()
    assert fresh.api_port == 9999
    assert fresh.partition_bucket_seconds == 120


def test_env_override_lowercase(monkeypatch) -> None:
    """A lowercase env var name also overrides (case_sensitive=False)."""
    monkeypatch.setenv("data_dir", "/tmp/x")
    fresh = Settings()
    assert fresh.data_dir == "/tmp/x"


def test_get_settings_is_cached_singleton() -> None:
    """``get_settings`` returns the same cached instance on repeat calls."""
    first = get_settings()
    second = get_settings()
    assert first is second
    assert isinstance(first, Settings)
