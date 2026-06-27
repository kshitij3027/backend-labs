"""Redis client + prediction cache (read-through, graceful degradation).

Redis is used **only** as a fast read cache for generated forecasts. PostgreSQL
is the durable source of truth (see :mod:`src.db`), so this module is written to
*never crash the prediction flow* when Redis is down: every cache operation is
wrapped in ``try/except`` and degrades to a no-op (writes) or ``None`` (reads),
logging a warning instead of raising.

Key scheme
----------
* ``forecast:{metric_name}:{horizon_minutes}`` — the forecast for a specific
  horizon.
* ``forecast:{metric_name}:latest`` — a pointer copy of the most recently cached
  forecast for the metric (any horizon), so a read without a horizon is cheap.

Values are plain JSON strings (no pickling), so the cache is language-agnostic
and inspectable. A TTL is always set; it defaults to ``2 *
prediction_interval_min`` minutes so a stale forecast naturally expires shortly
after the next scheduled run would have produced a fresh one.
"""

from __future__ import annotations

import json
import logging
from typing import Any

try:  # redis is pinned in requirements; guard so import never hard-fails.
    import redis as _redis
except Exception:  # pragma: no cover - redis is a hard dependency
    _redis = None  # type: ignore[assignment]

from src.config import get_settings

logger = logging.getLogger(__name__)

# Module-level lazy singleton client (built once per process from settings).
_client: "Any | None" = None

# Sentinel default for the "latest" suffix used when no horizon is supplied.
_LATEST_SUFFIX = "latest"


# --------------------------------------------------------------------------- #
# Client construction
# --------------------------------------------------------------------------- #
def get_redis() -> "Any | None":
    """Return the process-wide Redis client, building it once from settings.

    The client is created with ``decode_responses=True`` so values come back as
    ``str`` (we store/read JSON). Construction itself never raises: if the
    ``redis`` package is unavailable or the client cannot be built, a warning is
    logged and ``None`` is returned, and every cache helper treats ``None`` as
    "cache unavailable" (graceful degradation). Note that ``redis-py`` connects
    lazily, so a successful build does **not** guarantee the server is reachable
    — that is what :func:`ping` is for.
    """
    global _client
    if _client is not None:
        return _client
    if _redis is None:  # pragma: no cover - redis is pinned
        logger.warning("redis package unavailable; prediction cache disabled")
        return None
    try:
        settings = get_settings()
        _client = _redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
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
    """Default cache TTL: ``2 * prediction_interval_min`` minutes, in seconds."""
    try:
        interval_min = int(get_settings().prediction_interval_min)
    except Exception:  # noqa: BLE001
        interval_min = 5
    return max(60, 2 * interval_min * 60)


def _key(metric_name: str, horizon_minutes: "int | str") -> str:
    """Build a cache key for ``metric_name`` at ``horizon_minutes``."""
    return f"forecast:{metric_name}:{horizon_minutes}"


# --------------------------------------------------------------------------- #
# Cache operations (all degrade gracefully — never raise)
# --------------------------------------------------------------------------- #
def cache_prediction(
    metric_name: str,
    horizon_minutes: int,
    payload: dict,
    ttl_seconds: int | None = None,
) -> None:
    """JSON-serialise ``payload`` and cache it under the horizon + latest keys.

    Writes two keys with the same value and TTL:
    ``forecast:{metric}:{horizon_minutes}`` and ``forecast:{metric}:latest``.
    No-ops (logging a warning) if Redis is unavailable or the write fails — the
    forecast has already been persisted to Postgres, so a cache miss is harmless.
    """
    client = get_redis()
    if client is None:
        return
    ttl = ttl_seconds if ttl_seconds is not None else _default_ttl_seconds()
    try:
        blob = json.dumps(payload, default=str)
    except (TypeError, ValueError) as exc:
        logger.warning("could not JSON-serialise forecast for cache: %s", exc)
        return
    try:
        client.set(_key(metric_name, horizon_minutes), blob, ex=ttl)
        client.set(_key(metric_name, _LATEST_SUFFIX), blob, ex=ttl)
    except Exception as exc:  # noqa: BLE001 - Redis down must not break the flow
        logger.warning("Redis cache write failed for %r: %s", metric_name, exc)


def get_cached_prediction(
    metric_name: str,
    horizon_minutes: int | None = None,
) -> dict | None:
    """Return the cached forecast dict, or ``None`` on miss / Redis unavailable.

    With ``horizon_minutes`` omitted the ``latest`` pointer is read. Any error
    (Redis down, malformed JSON) is swallowed and ``None`` is returned so the
    caller can fall back to Postgres.
    """
    client = get_redis()
    if client is None:
        return None
    suffix: "int | str" = (
        _LATEST_SUFFIX if horizon_minutes is None else horizon_minutes
    )
    try:
        raw = client.get(_key(metric_name, suffix))
    except Exception as exc:  # noqa: BLE001 - read failures degrade to a miss
        logger.warning("Redis cache read failed for %r: %s", metric_name, exc)
        return None
    if raw is None:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        logger.warning("malformed cached forecast for %r: %s", metric_name, exc)
        return None
    return data if isinstance(data, dict) else None


def ping() -> bool:
    """Health check: ``True`` if Redis answers a PING, ``False`` otherwise.

    Never raises — used by the future ``/health`` endpoint and safe to call when
    Redis is down.
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
    "cache_prediction",
    "get_cached_prediction",
    "ping",
]
