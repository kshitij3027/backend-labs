"""Unit tests for src.config.Settings — defaults and override precedence."""

import pytest

from src.config import Settings, get_settings

#: Env vars the defaults test strips so it is hermetic — the compose `test` service
#: sets LIVE_STREAM_ENABLED, and a developer shell might export others.
_TUNABLE_ENV_VARS = (
    "TEMPORAL_WINDOW",
    "BASE_CAUSAL_STRENGTH",
    "LIVE_STREAM_ENABLED",
    "MAX_INCIDENT_HISTORY",
    "LOG_LEVEL",
)


@pytest.fixture()
def fresh_settings_cache():
    """Clear the get_settings LRU cache around a test that monkeypatches the env."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_defaults(monkeypatch):
    for var in _TUNABLE_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    settings = Settings(_env_file=None)  # hermetic: ignore any ambient .env file too

    # Causal-edge scoring (Req §7).
    assert settings.temporal_window == 300
    assert settings.base_causal_strength == 0.5
    assert settings.service_dependency_bonus == 0.3
    assert settings.error_propagation_bonus == 0.2
    assert settings.temporal_gap_threshold == 60
    assert settings.temporal_gap_penalty == 0.1
    assert settings.causal_strength_min == 0.1
    assert settings.causal_strength_max == 1.0

    # Confidence scoring (Req §7).
    assert settings.score_critical == 0.6
    assert settings.score_error == 0.4
    assert settings.score_warning == 0.2
    assert settings.temporal_score_weight == 0.3
    assert settings.centrality_score_weight == 0.2

    # Server / history / streaming.
    assert settings.server_port == 8000
    assert settings.cors_origins == "*"
    assert settings.max_incident_history == 1000
    # Live stream defaults OFF so tests/CI never run the background loop (enabled C8).
    assert settings.live_stream_enabled is False
    assert (
        settings.service_dependency_map_path == "src/config/service_dependency_map.json"
    )


def test_constructor_override_wins_over_default():
    # A constructor kwarg beats the field default (env file disabled for hermeticity).
    assert Settings(_env_file=None, temporal_window=99).temporal_window == 99


def test_env_override_int_wins_over_default(monkeypatch, fresh_settings_cache):
    monkeypatch.setenv("TEMPORAL_WINDOW", "42")
    assert get_settings().temporal_window == 42


def test_env_override_bool_wins_over_default(monkeypatch, fresh_settings_cache):
    monkeypatch.setenv("LIVE_STREAM_ENABLED", "true")
    assert get_settings().live_stream_enabled is True
