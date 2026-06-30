"""Integration tests for graceful degradation when Redis is DOWN (C4).

The embedding cache is a best-effort accelerator: Postgres+pgvector is the durable
store and embeddings recompute cheaply from the baked model, so a Redis outage must
**never** break the flow. These tests point the client at an unreachable endpoint
and assert every cache op degrades quietly (``ping`` → ``False``, reads → ``None``,
writes → no-op) while ``embed_text_cached`` still returns a valid vector.

How the outage is simulated
---------------------------
``src.clients.redis.get_redis`` builds its client from ``get_settings().redis_url``.
We monkeypatch the ``get_settings`` symbol *in the redis module* to return a settings
object whose ``redis_url`` is a closed port on ``127.0.0.1`` (``socket_connect_timeout``
is 2s so tests stay fast), then ``reset_client()`` so the next ``get_redis()`` rebuilds
against the dead URL. Teardown restores real settings and resets the client so later
tests get a live cache again.
"""

from __future__ import annotations

from typing import Iterator

import numpy as np
import pytest

from src import embeddings
from src.clients import redis as redis_client
from src.config import get_settings

# A port that nothing listens on → connections fail fast (refused / timeout).
_DEAD_REDIS_URL = "redis://127.0.0.1:6390/0"


@pytest.fixture
def dead_redis(monkeypatch) -> Iterator[None]:  # noqa: ANN001
    """Repoint the redis client at an unreachable endpoint for the test's duration.

    Builds a real ``Settings`` (so every other field is valid) but overrides
    ``redis_url`` to a dead port, patches ``redis.get_settings`` to return it, and
    resets the cached client. On teardown the client is reset again so subsequent
    tests rebuild against the real ``REDIS_URL`` from the compose ``test`` service.
    """
    dead_settings = get_settings().model_copy(update={"redis_url": _DEAD_REDIS_URL})
    monkeypatch.setattr(redis_client, "get_settings", lambda: dead_settings)
    redis_client.reset_client()
    try:
        yield
    finally:
        # monkeypatch auto-undoes the get_settings patch; drop the poisoned client
        # so the next get_redis() rebuilds from the real (restored) settings.
        redis_client.reset_client()


def test_ping_false_when_redis_down(dead_redis) -> None:  # noqa: ANN001
    """``ping()`` reports ``False`` (never raises) when Redis is unreachable."""
    assert redis_client.ping() is False


def test_cache_get_returns_none_when_redis_down(dead_redis) -> None:  # noqa: ANN001
    """A read against a dead Redis degrades to a miss (``None``), no exception."""
    result = redis_client.cache_get_embedding("anything at all")
    assert result is None


def test_cache_set_is_noop_when_redis_down(dead_redis) -> None:  # noqa: ANN001
    """A write against a dead Redis is a silent no-op (must not raise)."""
    vec = np.zeros(384, dtype=np.float32)
    # Should simply return without raising.
    redis_client.cache_set_embedding("anything at all", vec)


def test_embed_text_cached_still_returns_vector_when_redis_down(dead_redis) -> None:  # noqa: ANN001
    """With Redis down, ``embed_text_cached`` recomputes and still returns ``(384,)``.

    The read degrades to a miss, the recompute path runs the model, and the
    best-effort write no-ops — so the caller transparently gets a valid unit vector
    despite the outage. Correctness is never coupled to the cache being up.
    """
    vec = embeddings.embed_text_cached("cache is down but I still need an embedding")
    assert isinstance(vec, np.ndarray)
    assert vec.shape == (384,)
    assert vec.dtype == np.float32
    assert np.isclose(np.linalg.norm(vec), 1.0, atol=1e-4)


def test_client_recovers_after_degradation(dead_redis) -> None:  # noqa: ANN001
    """After the fixture tears down, the client rebuilds against the live Redis.

    Inside the test Redis is down (ping False); once ``dead_redis`` restores real
    settings and resets the client on teardown, later tests see a healthy cache.
    This case asserts the down-state; the recovery itself is covered by the cache
    tests which ``reset_client()`` + ``ping()`` at setup.
    """
    assert redis_client.ping() is False
