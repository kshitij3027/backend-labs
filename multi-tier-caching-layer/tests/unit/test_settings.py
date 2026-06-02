"""Unit tests for the Settings configuration object."""
from __future__ import annotations

import pytest

from src.settings import Settings, get_settings


def test_defaults_match_spec() -> None:
    """Spot-check that key defaults match the §7 configuration table."""
    s = Settings()
    assert s.l1_max_size == 1000
    assert s.l1_memory_mb == 100
    assert s.l1_ttl == 300
    assert s.cache_mem_cap_mb == 200
    assert s.l2_compress is True
    assert s.redis_port == 6379


def test_effective_redis_url_derived_when_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When redis_url is empty, it derives from host/port with db 0.

    The compose ``test`` service injects ``REDIS_URL`` into the environment,
    which pydantic-settings would otherwise read into ``redis_url`` (the field
    name matches the env var). Clear it here so the "empty default" path is
    exercised hermetically, independent of the ambient environment.
    """
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("redis_url", raising=False)
    s = Settings()
    assert s.redis_url == ""
    assert s.effective_redis_url == "redis://redis:6379/0"


def test_effective_redis_url_uses_explicit_value() -> None:
    """An explicit redis_url is returned verbatim by the property."""
    explicit = "redis://cache-host:6380/3"
    s = Settings(redis_url=explicit)
    assert s.effective_redis_url == explicit


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """An env var overrides the default on a freshly constructed Settings."""
    monkeypatch.setenv("L1_MAX_SIZE", "42")
    # Construct a NEW Settings() — do not rely on the cached get_settings().
    s = Settings()
    assert s.l1_max_size == 42


def test_get_settings_is_cached() -> None:
    """get_settings() returns the same cached instance each call."""
    assert get_settings() is get_settings()
