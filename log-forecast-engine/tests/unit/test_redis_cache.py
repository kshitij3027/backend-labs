"""Unit tests for the Redis prediction cache (C8) — graceful degradation.

These tests do NOT require a live Redis. They drive the public helpers in
:mod:`src.clients.redis` with the client pointed at an unreachable host (so any
real network op fails fast) and/or with the client object monkeypatched, and
assert the documented contract: every operation degrades to a no-op (writes) or
``None``/``False`` (reads) and NEVER raises when Redis is unavailable.
"""

from __future__ import annotations

import pytest

from src.clients import redis as redis_client


@pytest.fixture
def unreachable_redis(monkeypatch: pytest.MonkeyPatch):
    """Force the cache to build a client pointed at an unreachable host.

    Uses a TEST-NET (RFC 5737) address with a closed port so connect attempts
    fail fast (socket_connect_timeout is 2s in the client). We patch the
    *resolved* settings object the cache reads (``get_settings().redis_url``)
    rather than the env var, because the config loader currently lets YAML win
    over the environment (see the implementation-bug note in the agent report),
    which would otherwise keep the real URL in place.
    """
    from src.config import get_settings as real_get_settings

    settings = real_get_settings()
    monkeypatch.setattr(settings, "redis_url", "redis://192.0.2.1:6390/0")
    redis_client.reset_client()
    try:
        yield
    finally:
        redis_client.reset_client()


def test_ping_false_when_unreachable(unreachable_redis) -> None:
    # Must return False (not raise) when the server cannot be reached.
    assert redis_client.ping() is False


def test_ping_false_when_client_ping_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Boom:
        def ping(self) -> bool:
            raise RuntimeError("connection refused")

    monkeypatch.setattr(redis_client, "get_redis", lambda: _Boom())
    assert redis_client.ping() is False


def test_ping_false_when_client_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(redis_client, "get_redis", lambda: None)
    assert redis_client.ping() is False


def test_cache_prediction_noop_when_unreachable(unreachable_redis) -> None:
    payload = {"metric_name": "response_time", "ensemble_prediction": [1.0, 2.0]}
    # Must not raise even though the write cannot reach Redis.
    redis_client.cache_prediction("response_time", 60, payload)


def test_get_cached_prediction_none_when_unreachable(unreachable_redis) -> None:
    assert redis_client.get_cached_prediction("response_time", 60) is None
    # Latest-key path (no horizon) also degrades to None.
    assert redis_client.get_cached_prediction("response_time") is None


def test_get_cached_prediction_none_when_client_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(redis_client, "get_redis", lambda: None)
    assert redis_client.get_cached_prediction("x", 60) is None


def test_cache_write_swallows_client_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Boom:
        def set(self, *a, **k):
            raise RuntimeError("down")

    monkeypatch.setattr(redis_client, "get_redis", lambda: _Boom())
    # set() raising must be swallowed -> no exception escapes.
    redis_client.cache_prediction("m", 60, {"a": 1})


def test_get_swallows_malformed_json(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Bad:
        def get(self, *a, **k):
            return "{not json"

    monkeypatch.setattr(redis_client, "get_redis", lambda: _Bad())
    assert redis_client.get_cached_prediction("m", 60) is None


def test_get_returns_none_for_non_dict_json(monkeypatch: pytest.MonkeyPatch) -> None:
    class _List:
        def get(self, *a, **k):
            return "[1, 2, 3]"

    monkeypatch.setattr(redis_client, "get_redis", lambda: _List())
    assert redis_client.get_cached_prediction("m", 60) is None
