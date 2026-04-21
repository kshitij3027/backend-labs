"""Tests for :mod:`src.config`.

Verifies defaults load correctly, env-var overrides flow through
``pydantic-settings``, and the :func:`get_settings` cache behaves as
advertised (single instance per process, reset via
:func:`reset_settings_cache`).
"""

from __future__ import annotations

import pytest

from src.config import Settings, get_settings, reset_settings_cache


def test_defaults_load() -> None:
    """With no env overrides the documented defaults must apply."""
    s = Settings()
    assert s.http_port == 8000
    assert s.http_host == "0.0.0.0"
    assert s.log_level == "INFO"
    assert s.severity_weights["ERROR"] == 1.0
    assert s.severity_weights["INFO"] == 0.4
    assert s.ranking_weights["tfidf"] == 0.45
    assert s.incident_ranking_weights["severity"] == 0.25
    assert s.default_limit == 10
    assert s.query_cache_size == 1000
    assert s.candidate_top_k == 200
    assert s.idf_rebuild_every_n_docs == 500
    assert s.idf_rebuild_every_s == 2.0
    assert s.synonyms_path is None
    assert s.intent_patterns_path is None


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """An env var must override the default on the *next* cached read."""
    monkeypatch.setenv("HTTP_PORT", "9000")
    reset_settings_cache()
    assert get_settings().http_port == 9000


def test_env_override_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    """Literal-typed fields still accept their allowed values via env."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    reset_settings_cache()
    assert get_settings().log_level == "DEBUG"


def test_get_settings_caches() -> None:
    """Two consecutive calls must return the exact same instance."""
    a = get_settings()
    b = get_settings()
    assert a is b


def test_reset_settings_cache_works(monkeypatch: pytest.MonkeyPatch) -> None:
    """After a cache reset a new env value must show up in the next read."""
    first = get_settings()
    monkeypatch.setenv("HTTP_PORT", "12345")
    reset_settings_cache()
    second = get_settings()
    assert first is not second
    assert second.http_port == 12345
