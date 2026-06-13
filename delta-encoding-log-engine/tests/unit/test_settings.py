"""Unit tests for :mod:`app.settings`.

Covers the documented defaults, environment-variable overrides (with type
coercion and case-insensitive key mapping), the ``lru_cache`` singleton
behaviour of :func:`get_settings`, and the ``Field`` range / ``Literal``
validation guards.

All tests are isolated and deterministic: the ``settings_env`` fixture snapshots
and restores every env var the model reads and clears the ``get_settings`` cache
both before and after each test, so neither a leaked host env var nor a cached
``Settings`` instance can bleed across tests.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.settings import Settings, get_settings

# Every env var the model reads. Listed explicitly so the fixture can scrub
# anything that might leak in from the host / Docker environment.
SETTINGS_ENV_VARS = (
    "API_HOST",
    "API_PORT",
    "KEYFRAME_INTERVAL",
    "DELTA_BASELINE",
    "GZIP_DELTAS",
    "GENERATOR_FIELD_CHURN",
    "GENERATOR_SCHEMA_WIDTH",
    "ANALYZER_WINDOW",
    "RECONSTRUCT_CACHE_SIZE",
    "DASHBOARD_REFRESH_MS",
    "LOG_LEVEL",
)


@pytest.fixture
def settings_env(monkeypatch):
    """Isolate each test from host env vars and the ``get_settings`` cache.

    Deletes every settings env var (so the documented defaults apply unless a
    test sets one explicitly), and clears the LRU cache before and after the
    test so cached instances never leak between tests.
    """
    for name in SETTINGS_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield monkeypatch
    get_settings.cache_clear()


def test_defaults_match_documented_values(settings_env):
    """With no overrides, ``get_settings()`` returns the ``.env.example`` defaults."""
    settings = get_settings()

    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 8080
    assert settings.keyframe_interval == 100
    assert settings.delta_baseline == "previous"
    assert settings.gzip_deltas is False
    assert settings.generator_field_churn == 0.2
    assert settings.generator_schema_width == 8
    assert settings.analyzer_window == 200
    assert settings.reconstruct_cache_size == 1024
    assert settings.dashboard_refresh_ms == 2000
    assert settings.log_level == "INFO"


def test_default_types_are_coerced(settings_env):
    """Defaults carry their declared types (not strings)."""
    settings = get_settings()

    assert isinstance(settings.api_port, int)
    assert isinstance(settings.gzip_deltas, bool)
    assert isinstance(settings.generator_field_churn, float)


def test_env_override_with_coercion_and_case_insensitive_keys(settings_env):
    """UPPER_SNAKE env vars override fields, with bool/float/int coercion."""
    settings_env.setenv("API_PORT", "9090")
    settings_env.setenv("KEYFRAME_INTERVAL", "50")
    settings_env.setenv("DELTA_BASELINE", "keyframe")
    settings_env.setenv("GZIP_DELTAS", "true")
    settings_env.setenv("GENERATOR_FIELD_CHURN", "0.5")
    get_settings.cache_clear()

    settings = get_settings()

    # int coercion
    assert settings.api_port == 9090
    assert isinstance(settings.api_port, int)
    assert settings.keyframe_interval == 50
    # Literal string override
    assert settings.delta_baseline == "keyframe"
    # bool coercion: the string "true" -> True
    assert settings.gzip_deltas is True
    # float coercion
    assert settings.generator_field_churn == 0.5
    assert isinstance(settings.generator_field_churn, float)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("false", False),
        ("False", False),
        ("0", False),
    ],
)
def test_gzip_deltas_bool_coercion_variants(settings_env, raw, expected):
    """``GZIP_DELTAS`` accepts the usual truthy/falsey string spellings."""
    settings_env.setenv("GZIP_DELTAS", raw)
    get_settings.cache_clear()

    assert get_settings().gzip_deltas is expected


def test_case_insensitive_lowercase_env_key(settings_env):
    """``case_sensitive=False`` means a lowercase env key also maps."""
    settings_env.setenv("api_port", "7000")
    get_settings.cache_clear()

    assert get_settings().api_port == 7000


def test_lru_cache_returns_same_instance(settings_env):
    """Two calls without a cache clear return the identical cached object."""
    first = get_settings()
    second = get_settings()

    assert first is second


def test_cache_clear_picks_up_new_env_value(settings_env):
    """After ``cache_clear()`` a freshly set env var is reflected."""
    first = get_settings()
    assert first.api_port == 8080

    settings_env.setenv("API_PORT", "9091")
    # Without clearing, the cached instance is unchanged.
    assert get_settings() is first
    assert get_settings().api_port == 8080

    get_settings.cache_clear()
    second = get_settings()

    assert second is not first
    assert second.api_port == 9091


def test_api_port_above_max_raises():
    """``api_port`` must be <= 65535 (``Field(le=65535)``)."""
    with pytest.raises(ValidationError):
        Settings(api_port=70000)


def test_api_port_below_min_raises():
    """``api_port`` must be >= 1 (``Field(ge=1)``)."""
    with pytest.raises(ValidationError):
        Settings(api_port=0)


def test_keyframe_interval_zero_raises():
    """``keyframe_interval`` must be >= 1 (``Field(ge=1)``)."""
    with pytest.raises(ValidationError):
        Settings(keyframe_interval=0)


def test_generator_field_churn_above_one_raises():
    """``generator_field_churn`` must be <= 1.0 (``Field(le=1.0)``)."""
    with pytest.raises(ValidationError):
        Settings(generator_field_churn=1.5)


def test_generator_field_churn_below_zero_raises():
    """``generator_field_churn`` must be >= 0.0 (``Field(ge=0.0)``)."""
    with pytest.raises(ValidationError):
        Settings(generator_field_churn=-0.1)


def test_delta_baseline_invalid_literal_raises():
    """``delta_baseline`` outside the ``Literal`` set is rejected."""
    with pytest.raises(ValidationError):
        Settings(delta_baseline="sideways")


def test_validation_guard_via_env(settings_env):
    """Out-of-range values supplied through the environment also raise.

    Exercises the env-loading path (not just direct kwargs) to confirm the
    guard fires regardless of how the value arrives.
    """
    settings_env.setenv("API_PORT", "70000")
    with pytest.raises(ValidationError):
        Settings()
