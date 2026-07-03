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
import json
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

# Namespace prefix for the /recommend response cache (C9). A recommendation is a
# JSON document keyed by the stable hash of the normalised query, so an identical
# repeated query can be served without re-embedding / re-retrieving / re-ranking.
_REC_KEY_PREFIX = "rec:"

# Namespace prefix for the per-pattern feedback epoch (C11). A monotonically
# increasing integer counter, one per query pattern, that is folded into the
# recommendation cache key. Each vote INCRs it, which changes the cache key for that
# pattern and forces the next identical /recommend to MISS and re-rank with the fresh
# feedback (old entries just expire by TTL).
_FB_EPOCH_KEY_PREFIX = "fbver:"

# Global runtime-config version counter (C12). A single monotonically increasing
# integer, bumped on every PUT /config, that is folded into the recommendation cache
# key alongside the feedback epoch. A config change therefore changes every cache key
# and forces the next /recommend to MISS and recompute under the new weights/epsilon/
# thresholds — a live retune with no restart. Fault tolerant: reads default to 0.
_CONFIG_VERSION_KEY = "cfgver"

# Redis hash holding the live runtime-config overrides (C12). A single shared hash so
# every replica reads the same tuned values; the recommendation service overlays these
# over the static settings via :mod:`src.runtime_config`. Fault tolerant on read/write.
_RUNTIME_CONFIG_KEY = "runtime_config"

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


def _default_recommendation_ttl_seconds() -> int:
    """Default /recommend response-cache TTL from settings (seconds); fallback 1h."""
    try:
        return int(get_settings().recommendation_cache_ttl_sec)
    except Exception:  # noqa: BLE001
        return 3600


def embedding_key(text: str) -> str:
    """Return the cache key for ``text``: ``"emb:" + sha256(text).hexdigest()``."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"{_EMB_KEY_PREFIX}{digest}"


def recommendation_key(query_hash: str) -> str:
    """Return the /recommend cache key for a query hash: ``"rec:" + query_hash``.

    The recommendation service already computes a stable SHA-256 hash of the
    normalised query, so this is a simple namespaced prefix rather than a second
    hash.
    """
    return f"{_REC_KEY_PREFIX}{query_hash}"


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
        observability.record_cache("embedding", False)
        return None
    try:
        raw = client.get(embedding_key(text))
    except Exception as exc:  # noqa: BLE001 - read failures degrade to a miss
        logger.warning("Redis embedding-cache read failed: %s", exc)
        observability.record_cache("embedding", False)
        return None
    if not raw:
        observability.record_cache("embedding", False)
        return None
    try:
        # copy=True (via np.array) so the result is writable and independent of
        # the transient read-only bytes buffer.
        vec = np.array(np.frombuffer(raw, dtype=_EMB_DTYPE))
    except Exception as exc:  # noqa: BLE001 - malformed buffer -> treat as a miss
        logger.warning("malformed cached embedding: %s", exc)
        observability.record_cache("embedding", False)
        return None
    if vec.size == 0:
        observability.record_cache("embedding", False)
        return None
    observability.record_cache("embedding", True)
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


# --------------------------------------------------------------------------- #
# Recommendation response cache (C9) — JSON documents, graceful degradation
# --------------------------------------------------------------------------- #
def cache_get_recommendation(query_hash: str) -> "dict[str, Any] | None":
    """Return the cached /recommend response for ``query_hash``, or ``None``.

    The stored value is a UTF-8 JSON document (bytes, since the shared client is
    binary-safe). Any failure — Redis down, a miss, or a malformed payload — is
    swallowed and ``None`` is returned so the caller recomputes the recommendation.
    """
    client = get_redis()
    if client is None:
        return None
    try:
        raw = client.get(recommendation_key(query_hash))
    except Exception as exc:  # noqa: BLE001 - read failures degrade to a miss
        logger.warning("Redis recommendation-cache read failed: %s", exc)
        return None
    if not raw:
        return None
    try:
        # The client is binary-safe, so ``raw`` is bytes; json.loads accepts them.
        data = json.loads(raw)
    except Exception as exc:  # noqa: BLE001 - malformed payload -> treat as a miss
        logger.warning("malformed cached recommendation: %s", exc)
        return None
    if not isinstance(data, dict):
        return None
    return data


def cache_set_recommendation(
    query_hash: str, payload: "dict[str, Any]", ttl: int | None = None
) -> None:
    """Serialise ``payload`` to JSON and cache it under ``query_hash``'s key.

    ``ttl`` defaults to ``settings.recommendation_cache_ttl_sec`` (1h). No-ops
    (logging a warning) if Redis is unavailable or the write/serialisation fails —
    a cache miss just recomputes the recommendation, so a write failure is harmless.
    """
    client = get_redis()
    if client is None:
        return
    ttl_seconds = ttl if ttl is not None else _default_recommendation_ttl_seconds()
    try:
        blob = json.dumps(payload).encode("utf-8")
    except Exception as exc:  # noqa: BLE001 - non-serialisable payload -> skip caching
        logger.warning("could not serialise recommendation for cache: %s", exc)
        return
    try:
        client.set(recommendation_key(query_hash), blob, ex=ttl_seconds)
    except Exception as exc:  # noqa: BLE001 - Redis down must not break the flow
        logger.warning("Redis recommendation-cache write failed: %s", exc)


# --------------------------------------------------------------------------- #
# Feedback epoch (C11) — per-pattern cache-invalidation counter
# --------------------------------------------------------------------------- #
def feedback_epoch_key(pattern: str) -> str:
    """Return the Redis key holding the feedback epoch for ``pattern``.

    ``"fbver:" + sha256(pattern)`` — the query pattern (``service|severity|tags``)
    is hashed so the key stays bounded and printable regardless of how many / how
    long the tags are, mirroring the embedding/recommendation key scheme. Identical
    patterns therefore share one counter, which is exactly what lets a vote on a
    pattern invalidate every cached ``/recommend`` for that same pattern.
    """
    digest = hashlib.sha256(pattern.encode("utf-8")).hexdigest()
    return f"{_FB_EPOCH_KEY_PREFIX}{digest}"


def get_feedback_epoch(pattern: str) -> int:
    """Return the current feedback epoch for ``pattern`` (``0`` if unset / unavailable).

    Read-through for the recommendation cache key: the epoch is folded into the key so
    a change (a vote bumped it) produces a fresh key and thus a cache MISS. Fault
    tolerant — Redis down, a missing key, or a non-integer value all degrade to ``0``
    (treat as "no votes yet, serve/populate the base cache"), never raising.
    """
    client = get_redis()
    if client is None:
        return 0
    try:
        raw = client.get(feedback_epoch_key(pattern))
    except Exception as exc:  # noqa: BLE001 - read failures degrade to epoch 0
        logger.warning("Redis feedback-epoch read failed: %s", exc)
        return 0
    if raw is None:
        return 0
    try:
        # Binary-safe client -> value is bytes; int() accepts a numeric byte string.
        return int(raw)
    except (TypeError, ValueError) as exc:
        logger.warning("malformed feedback epoch (%r): %s", raw, exc)
        return 0


def bump_feedback_epoch(pattern: str) -> None:
    """Increment the feedback epoch for ``pattern`` (INCR), invalidating its cache.

    Called after a vote is recorded for ``pattern``: the INCR changes the value
    :func:`get_feedback_epoch` returns, so the next identical ``/recommend`` builds a
    different cache key and MISSes (recomputing with the new feedback). ``INCR`` on a
    missing key creates it at ``1``, so no initialisation is needed. No-ops (logging a
    warning) if Redis is unavailable or the write fails — a failed bump only means the
    stale cache lingers until its TTL, which must never break recording the vote.
    """
    client = get_redis()
    if client is None:
        return
    try:
        client.incr(feedback_epoch_key(pattern))
    except Exception as exc:  # noqa: BLE001 - Redis down must not break feedback
        logger.warning("Redis feedback-epoch bump failed: %s", exc)


# --------------------------------------------------------------------------- #
# Runtime config (C12) — global version counter + shared overrides hash
# --------------------------------------------------------------------------- #
def get_config_version() -> int:
    """Return the current global runtime-config version (``0`` if unset/unavailable).

    Read-through for the recommendation cache key: the version is folded into the key
    so a ``PUT /config`` (which bumps it) yields fresh keys and thus cache MISSes,
    making a retune take effect on the next ``/recommend`` without a restart. Fault
    tolerant — Redis down, a missing key, or a non-integer value all degrade to ``0``,
    never raising.
    """
    client = get_redis()
    if client is None:
        return 0
    try:
        raw = client.get(_CONFIG_VERSION_KEY)
    except Exception as exc:  # noqa: BLE001 - read failures degrade to version 0
        logger.warning("Redis config-version read failed: %s", exc)
        return 0
    if raw is None:
        return 0
    try:
        # Binary-safe client -> value is bytes; int() accepts a numeric byte string.
        return int(raw)
    except (TypeError, ValueError) as exc:
        logger.warning("malformed config version (%r): %s", raw, exc)
        return 0


def bump_config_version() -> int:
    """Increment the global runtime-config version (INCR) and return the new value.

    Called after a ``PUT /config`` writes new overrides: the INCR changes the value
    :func:`get_config_version` returns, so the next ``/recommend`` builds a different
    cache key and MISSes (recomputing under the new config). ``INCR`` on a missing key
    creates it at ``1``. Fault tolerant — if Redis is unavailable or the write fails a
    warning is logged and ``0`` is returned; the override write itself already handles
    that case, so a failed bump only means the stale cache lingers until its TTL.
    """
    client = get_redis()
    if client is None:
        return 0
    try:
        return int(client.incr(_CONFIG_VERSION_KEY))
    except Exception as exc:  # noqa: BLE001 - Redis down must not break /config
        logger.warning("Redis config-version bump failed: %s", exc)
        return 0


def get_runtime_config() -> "dict[str, str]":
    """Return the shared runtime-config overrides hash as ``{field: raw_str}``.

    The values are the raw string forms written by :func:`set_runtime_config`
    (numbers stringified); the caller (:mod:`src.runtime_config`) coerces them back to
    the correct types and validates ranges. Fault tolerant — Redis down, a missing
    hash, or a read error all degrade to an empty dict, so callers transparently fall
    back to the static settings defaults.
    """
    client = get_redis()
    if client is None:
        return {}
    try:
        raw = client.hgetall(_RUNTIME_CONFIG_KEY)
    except Exception as exc:  # noqa: BLE001 - read failures degrade to no overrides
        logger.warning("Redis runtime-config read failed: %s", exc)
        return {}
    if not raw:
        return {}
    result: dict[str, str] = {}
    for key, value in raw.items():
        # Binary-safe client -> keys/values are bytes; decode to str for the caller.
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        v = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        result[k] = v
    return result


def set_runtime_config(updates: "dict[str, Any]") -> None:
    """Merge ``updates`` into the shared runtime-config overrides hash (HSET).

    Values are stored as their ``str`` form (the hash is a flat string map);
    :func:`get_runtime_config` reads them back and :mod:`src.runtime_config` coerces
    and validates them. This is a *merge* (HSET of the given fields), so unrelated
    previously-set overrides are preserved. No-ops (logging a warning) if Redis is
    unavailable or the write fails — the caller decides whether to still bump the
    version; a lost write just means the override never takes effect.
    """
    client = get_redis()
    if client is None:
        return
    if not updates:
        return
    mapping = {str(k): str(v) for k, v in updates.items()}
    try:
        client.hset(_RUNTIME_CONFIG_KEY, mapping=mapping)
    except Exception as exc:  # noqa: BLE001 - Redis down must not break /config
        logger.warning("Redis runtime-config write failed: %s", exc)


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
    "recommendation_key",
    "cache_get_recommendation",
    "cache_set_recommendation",
    "feedback_epoch_key",
    "get_feedback_epoch",
    "bump_feedback_epoch",
    "get_config_version",
    "bump_config_version",
    "get_runtime_config",
    "set_runtime_config",
    "ping",
]
