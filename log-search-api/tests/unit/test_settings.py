from __future__ import annotations

import pytest
from pydantic import ValidationError


def _purge_env(monkeypatch: pytest.MonkeyPatch) -> None:
    keys = [
        "SECRET_KEY",
        "API_HOST",
        "API_PORT",
        "PROJECT_NAME",
        "API_V1_PREFIX",
        "JWT_ALGORITHM",
        "ACCESS_TOKEN_TTL_MINUTES",
        "RATE_LIMIT_REQUESTS",
        "RATE_LIMIT_WINDOW_SECONDS",
        "REDIS_URL",
        "CACHE_REDIS_DB",
        "RATE_LIMIT_REDIS_DB",
        "ELASTICSEARCH_URL",
        "ELASTICSEARCH_INDEX",
        "SEARCH_CACHE_TTL_SECONDS",
        "DEFAULT_SEARCH_LIMIT",
        "MAX_SEARCH_LIMIT",
        "DEFAULT_SEARCH_OFFSET",
        "DEFAULT_SORT_BY",
        "DEFAULT_SORT_ORDER",
        "DEFAULT_INCLUDE_CONTENT",
        "CORS_ALLOWED_ORIGINS",
        "SEED_USERNAME",
        "SEED_PASSWORD_HASH",
        "LOG_LEVEL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def test_settings_requires_secret_key(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _purge_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    from src.config import Settings

    with pytest.raises(ValidationError):
        Settings()


def test_settings_defaults_match_spec(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _purge_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SECRET_KEY", "x" * 32)
    from src.config import Settings

    settings = Settings()
    assert settings.PROJECT_NAME == "Log Search API"
    assert settings.API_V1_PREFIX == "/api/v1"
    assert settings.RATE_LIMIT_REQUESTS == 100
    assert settings.RATE_LIMIT_WINDOW_SECONDS == 60
    assert settings.ACCESS_TOKEN_TTL_MINUTES == 15
    assert settings.JWT_ALGORITHM == "HS256"
    assert settings.SEARCH_CACHE_TTL_SECONDS == 300
    assert settings.DEFAULT_SEARCH_LIMIT == 100
    assert settings.MAX_SEARCH_LIMIT == 1000
    assert settings.DEFAULT_SEARCH_OFFSET == 0
    assert settings.DEFAULT_SORT_BY == "relevance"
    assert settings.DEFAULT_SORT_ORDER == "desc"
    assert settings.DEFAULT_INCLUDE_CONTENT is True
    assert settings.ELASTICSEARCH_INDEX == "logs"
    assert settings.CACHE_REDIS_DB == 0
    assert settings.RATE_LIMIT_REDIS_DB == 1


def test_cors_origins_list_splits_csv(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _purge_env(monkeypatch)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SECRET_KEY", "x" * 32)
    monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "http://a.com, http://b.com ,http://c.com")
    from src.config import Settings

    origins = Settings().cors_origins_list()
    assert origins == ["http://a.com", "http://b.com", "http://c.com"]
