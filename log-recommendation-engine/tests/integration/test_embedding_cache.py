"""Integration tests for the C4 embedding cache against a REAL Redis.

``REDIS_URL`` is supplied by the compose ``test`` service and points at the
``redis`` service, so these exercise the actual read-through path end to end:
compute-on-miss, store as raw ``float32`` bytes, and serve-on-hit.

Coverage:
  * cold miss → ``cache_get_embedding`` returns ``None``;
  * ``embed_text_cached`` computes + stores, and a subsequent
    ``cache_get_embedding`` returns a ``(384,)`` array **byte-identical** to it;
  * the Redis key ``embedding_key(text)`` exists and the stored value is exactly
    ``384 * 4 = 1536`` bytes (float32);
  * **read-through**: a second ``embed_text_cached`` is served from cache without
    recomputing — proven by monkeypatching ``embeddings.embed_text`` to blow up if
    called again.

Each test uses a unique text so entries never collide across tests/reruns, and
cleans up its key on teardown. ``reset_client()`` is called up-front so a client
possibly poisoned by the degradation test (pointing at a dead endpoint) is rebuilt
against the real ``REDIS_URL``.
"""

from __future__ import annotations

import uuid
from typing import Iterator

import numpy as np
import pytest

from src import embeddings
from src.clients import redis as redis_client


@pytest.fixture
def fresh_redis() -> Iterator[None]:
    """Ensure a live client built from the real ``REDIS_URL`` and require Redis up.

    Drops any cached client first (it may have been repointed at a dead host by the
    degradation test), then rebuilds and pings. Skips the test if Redis is genuinely
    unreachable so a missing service reads as a skip, not a spurious failure.
    """
    redis_client.reset_client()
    if not redis_client.ping():
        pytest.skip("Redis is not reachable at REDIS_URL; skipping cache integration test")
    yield
    redis_client.reset_client()


@pytest.fixture
def unique_text() -> str:
    """A unique incident-document string so its cache key is never pre-populated."""
    return f"integration cache probe {uuid.uuid4().hex}"


def _delete_key(text: str) -> None:
    """Best-effort cleanup of the cache entry for ``text`` (never raises)."""
    client = redis_client.get_redis()
    if client is not None:
        try:
            client.delete(redis_client.embedding_key(text))
        except Exception:  # noqa: BLE001 - cleanup must not fail the test
            pass


# --------------------------------------------------------------------------- #
# Miss → compute+store → hit round-trip
# --------------------------------------------------------------------------- #
def test_cache_miss_then_readthrough_populates(fresh_redis, unique_text) -> None:  # noqa: ANN001
    """Cold miss, then ``embed_text_cached`` stores a vector retrievable on hit."""
    _delete_key(unique_text)
    try:
        # 1. Cold miss.
        assert redis_client.cache_get_embedding(unique_text) is None

        # 2. Read-through computes and stores.
        v1 = embeddings.embed_text_cached(unique_text)
        assert isinstance(v1, np.ndarray)
        assert v1.shape == (384,)
        assert v1.dtype == np.float32

        # 3. Now it is a hit and byte-identical to what was computed.
        cached = redis_client.cache_get_embedding(unique_text)
        assert cached is not None
        assert cached.shape == (384,)
        assert np.array_equal(cached, v1)
    finally:
        _delete_key(unique_text)


def test_cached_value_key_exists_and_byte_length(fresh_redis, unique_text) -> None:  # noqa: ANN001
    """The Redis key exists and the stored value is exactly 1536 bytes (384*float32)."""
    _delete_key(unique_text)
    try:
        embeddings.embed_text_cached(unique_text)

        client = redis_client.get_redis()
        assert client is not None

        key = redis_client.embedding_key(unique_text)
        assert client.exists(key) == 1

        raw = client.get(key)
        assert raw is not None
        # 384 float32 values * 4 bytes each = 1536 raw bytes (no pickling/JSON).
        assert len(raw) == 1536
    finally:
        _delete_key(unique_text)


# --------------------------------------------------------------------------- #
# Read-through: second call served from cache, no recompute
# --------------------------------------------------------------------------- #
def test_second_call_served_from_cache_without_recompute(
    fresh_redis, unique_text, monkeypatch
) -> None:  # noqa: ANN001
    """A warm second ``embed_text_cached`` must not recompute the embedding.

    After the first call populates the cache, ``embeddings.embed_text`` is patched
    to raise. A second ``embed_text_cached`` that still returns the same vector
    proves it was served straight from Redis (the recompute path was never taken).
    """
    _delete_key(unique_text)
    try:
        v1 = embeddings.embed_text_cached(unique_text)

        def _boom(_text: str):  # noqa: ANN202
            raise AssertionError(
                "embed_text was called on a cache hit — read-through did not serve "
                "from Redis"
            )

        monkeypatch.setattr(embeddings, "embed_text", _boom)

        v2 = embeddings.embed_text_cached(unique_text)
        assert np.array_equal(v2, v1)
    finally:
        _delete_key(unique_text)
