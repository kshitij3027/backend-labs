"""Unit tests for src.config.Settings — defaults and environment-override precedence."""

import pytest

from src.config import Settings, get_settings

#: Env vars the defaults test must strip so it is hermetic — the compose `test`
#: service sets some of these (e.g. PIPELINE_ENABLED=false, REDIS_URL).
_TUNABLE_ENV_VARS = (
    "REDIS_URL",
    "WINDOW_SECONDS",
    "EVENTS_PER_SECOND",
    "DETECTION_INTERVAL_SECONDS",
    "PIPELINE_ENABLED",
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
    assert settings.window_seconds == 30
    # 135, not a round 120: realized ingest is ~0.9x target, and 135 keeps the
    # measured rate above the 100 eps E2E/load gate (see src/config.py).
    assert settings.events_per_second == 135
    assert settings.pipeline_enabled is True
    assert settings.detection_interval_seconds == 2.0
    assert settings.redis_url == "redis://localhost:6379/0"


def test_env_override_int_wins_over_default(monkeypatch, fresh_settings_cache):
    monkeypatch.setenv("WINDOW_SECONDS", "5")
    assert get_settings().window_seconds == 5


def test_env_override_float_wins_over_default(monkeypatch, fresh_settings_cache):
    monkeypatch.setenv("DETECTION_INTERVAL_SECONDS", "0.5")
    assert get_settings().detection_interval_seconds == 0.5
