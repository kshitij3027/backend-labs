"""Unit tests for src.settings — defaults, env overrides, and cache behaviour."""
from __future__ import annotations

import pytest

from src.settings import Settings, get_settings

# Every env var the Settings class currently reads.
ENV_VARS = ("API_HOST", "API_PORT", "DATA_DIR", "LOG_LEVEL")


@pytest.fixture(autouse=True)
def _cold_settings_cache() -> object:
    """Start and finish every test with an empty get_settings LRU cache."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no env overrides, fields carry the documented defaults."""
    for var in ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    s = Settings()
    assert s.api_host == "0.0.0.0"
    assert s.api_port == 8001
    assert s.data_dir == "./data"
    assert s.log_level == "INFO"


def test_env_override_after_cache_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    """An env var override takes effect once the settings cache is cleared."""
    monkeypatch.setenv("API_PORT", "9999")
    get_settings.cache_clear()
    assert get_settings().api_port == 9999


def test_get_settings_returns_cached_singleton() -> None:
    """Repeated get_settings() calls hand back the same cached instance."""
    first = get_settings()
    second = get_settings()
    assert first is second
    # Direct construction bypasses the cache and yields a distinct object.
    assert Settings() is not first
