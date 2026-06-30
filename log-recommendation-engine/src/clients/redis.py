"""Redis client + embedding cache (binary-safe, graceful degradation).

Redis is used here as a fast read-through cache for dense text embeddings. The
embeddings are cheap to recompute from the baked-in sentence-transformers model
(see :mod:`src.embeddings`) and Postgres+pgvector remains the durable store of
corpus vectors, so this module is written to **never crash the request flow when
Redis is down**: every cache operation is wrapped in ``try/except`` and degrades
to a no-op (writes) or ``None`` (reads), logging a warning instead of raising.
(Fuller degradation/circuit-breaker handling lands in C21; this is the baseline.)

Binary-safe values
------------------
Unlike the JSON caches elsewhere in the fleet, embeddings are raw ``float32``
numpy buffers, so the client is built with ``decode_responses=False`` and values
are stored/read as bytes. A vector is serialised with ``vec.tobytes()`` and
rehydrated with ``np.frombuffer(raw, dtype=float32)`` — a zero-copy, exact
round-trip (no precision loss, no pickling).

Key scheme
----------
``emb:{sha256(text)}`` — the cache key is ``"emb:"`` followed by the hex SHA-256
digest of the UTF-8 encoded text. Hashing keeps keys bounded and printable
regardless of the (possibly long, multi-line) incident document text, and makes
identical documents share a cache entry. The *same* document text feeds both the
corpus and query embedding paths (see :func:`src.embeddings.build_incident_text`),
so cache hits are meaningful across ingest and retrieval.

A TTL is always set on writes; it defaults to ``settings.embedding_cache_ttl_sec``
(24h) so stale entries expire on their own.
"""

from __future__ import annotations

import hashlib
from typing import Any

import numpy as np

try:  # redis is pinned in requirements; guard so import never hard-fails.
    import redis as _redis
except Exception:  # pragma: no cover - redis is a hard dependency
    _redis = None  # type: ignore[assignment]

from src import observability
from src.config import get_settings

logger = observability.get_logger(__name__)

# Module-level lazy singleton client (built once per process from settings).
_client: "Any | None" = None

# Namespace prefix for embedding-cache keys.
_EMB_KEY_PREFIX = "emb:"

# numpy dtype the cache serialises to/from. Kept in one place so the write
# (``tobytes``) and read (``frombuffer``) sides can never drift apart.
_EMB_DTYPE = np.float32


# --------------------------------------------------------------------------- #
# Client construction
# --------------------------------------------------------------------------- #
def get_redis() -> "Any | None":
    """Return the process-wide Redis client, building it once from settings.

    The client is **binary-safe** (``decode_responses=False``) because the
    embedding cache stores raw ``float32`` bytes, not text. Construction never
    raises: if the ``redis`` package is unavailable or the client cannot be
    built, a warning is logged and ``None`` is returned, and every cache helper
    treats ``None`` as "cache unavailable" (graceful degradation). ``redis-py``
    connects lazily, so a successful build does **not** guarantee the server is
    reachable — that is what :func:`ping` is for.
    """
    global _client
    if _client is not None:
        return _client
    if _redis is None:  # pragma: no cover - redis is pinned
        logger.warning("redis package unavailable; embedding cache disabled")
        return None
    try:
        settings = get_settings()
        _client = _redis.Redis.from_url(
            settings.redis_url,
            decode_responses=False,  # binary-safe: values are float32 bytes
            socket_connect_timeout=2,
            socket_timeout=2,
        )
    except Exception as exc:  # noqa: BLE001 - never crash on client build
        logger.warning("could not build Redis client: %s", exc)
        _client = None
    return _client


def reset_client() -> None:
    """Drop the cached client (used by tests after monkeypatching settings)."""
    global _client
    _client = None


def _default_ttl_seconds() -> int:
    """Default embedding-cache TTL from settings (seconds); safe fallback 24h."""
    try:
        return int(get_settings().embedding_cache_ttl_sec)
    except Exception:  # noqa: BLE001
        return 86400


def embedding_key(text: str) -> str:
    """Return the cache key for ``text``: ``"emb:" + sha256(text).hexdigest()``."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"{_EMB_KEY_PREFIX}{digest}"


# --------------------------------------------------------------------------- #
# Cache operations (all degrade gracefully — never raise)
# --------------------------------------------------------------------------- #
def cache_get_embedding(text: str) -> "np.ndarray | None":
    """Return the cached embedding for ``text``, or ``None`` on miss/unavailable.

    The stored value is a raw ``float32`` byte buffer; it is rehydrated with
    ``np.frombuffer(raw, dtype=float32)`` (a copy is taken so the returned array
    owns writable memory rather than aliasing the read-only buffer). Any error
    (Redis down, malformed/empty buffer) is swallowed and ``None`` is returned so
    the caller falls back to recomputing the embedding.
    """
    client = get_redis()
    if client is None:
        return None
    try:
        raw = client.get(embedding_key(text))
    except Exception as exc:  # noqa: BLE001 - read failures degrade to a miss
        logger.warning("Redis embedding-cache read failed: %s", exc)
        return None
    if not raw:
        return None
    try:
        # copy=True (via np.array) so the result is writable and independent of
        # the transient read-only bytes buffer.
        vec = np.array(np.frombuffer(raw, dtype=_EMB_DTYPE))
    except Exception as exc:  # noqa: BLE001 - malformed buffer -> treat as a miss
        logger.warning("malformed cached embedding: %s", exc)
        return None
    if vec.size == 0:
        return None
    return vec


def cache_set_embedding(
    text: str, vec: "np.ndarray", ttl: int | None = None
) -> None:
    """Serialise ``vec`` to ``float32`` bytes and cache it under ``text``'s key.

    ``ttl`` defaults to ``settings.embedding_cache_ttl_sec``. No-ops (logging a
    warning) if Redis is unavailable or the write fails — a cache miss just means
    the embedding is recomputed next time, so a write failure is harmless.
    """
    client = get_redis()
    if client is None:
        return
    ttl_seconds = ttl if ttl is not None else _default_ttl_seconds()
    try:
        blob = np.ascontiguousarray(vec, dtype=_EMB_DTYPE).tobytes()
    except Exception as exc:  # noqa: BLE001 - bad array -> skip caching
        logger.warning("could not serialise embedding for cache: %s", exc)
        return
    try:
        client.set(embedding_key(text), blob, ex=ttl_seconds)
    except Exception as exc:  # noqa: BLE001 - Redis down must not break the flow
        logger.warning("Redis embedding-cache write failed: %s", exc)


def ping() -> bool:
    """Health check: ``True`` if Redis answers a PING, ``False`` otherwise.

    Never raises — safe to call from a readiness probe when Redis is down.
    """
    client = get_redis()
    if client is None:
        return False
    try:
        return bool(client.ping())
    except Exception as exc:  # noqa: BLE001
        logger.warning("Redis ping failed: %s", exc)
        return False


__all__ = [
    "get_redis",
    "reset_client",
    "embedding_key",
    "cache_get_embedding",
    "cache_set_embedding",
    "ping",
]
