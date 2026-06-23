"""Thin, defensive factory for a synchronous Redis client.

The Real-Time Log Pattern Clustering engine treats Redis as an *optional* backend: when
it is reachable the :class:`~src.state.StateStore` persists cluster stats / patterns /
anomalies there; when it is not, the store falls back to an in-process dict so unit
tests (and a Redis-less deployment) keep working.

To make that fallback seamless, :func:`make_redis_client` never raises — a connection
or auth failure simply yields ``None``. The real ``redis`` package is imported lazily
*inside* the functions so that merely importing this module can never hard-fail even if
the dependency were somehow absent.

A sync client is intentional: the streaming engine runs its persistence calls inside a
threadpool, so an async client would add no benefit and more moving parts.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

from src.config import AppConfig, load_config

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    import redis as _redis

logger = logging.getLogger(__name__)

#: Default socket / connect timeout (seconds). Kept short so an unreachable Redis fails
#: fast and the caller can fall back to in-memory state without a long stall.
DEFAULT_SOCKET_TIMEOUT = 2.0


def make_redis_client(
    config: Optional[AppConfig] = None,
    *,
    socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
) -> "Optional[_redis.Redis]":
    """Build and verify a synchronous Redis client, or return ``None`` if unreachable.

    The connection is validated eagerly with a ``PING`` so the caller never receives a
    client that cannot actually talk to Redis. Any failure (import error, refused
    connection, timeout, auth) is swallowed and logged at debug level — this function
    must never raise, because a Redis outage must not crash startup.

    Args:
        config: Application config supplying ``redis.{host,port,db}``. Loaded via
            :func:`~src.config.load_config` (honouring ``REDIS_HOST`` / ``REDIS_PORT``
            / ``REDIS_DB`` env overrides) when ``None``.
        socket_timeout: Socket read and connect timeout in seconds.

    Returns:
        A live, decoded-responses :class:`redis.Redis` instance whose ``PING``
        succeeded, or ``None`` when Redis is unavailable for any reason.
    """
    try:
        import redis  # local import: keeps module import safe even sans dependency
    except Exception:  # pragma: no cover - redis is a pinned dependency
        logger.debug("redis package unavailable; falling back to in-memory state")
        return None

    cfg = config or load_config()
    try:
        client = redis.Redis(
            host=cfg.redis.host,
            port=cfg.redis.port,
            db=cfg.redis.db,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_timeout,
            decode_responses=True,
        )
        client.ping()
        return client
    except Exception as exc:  # connection refused / timeout / auth / DNS ...
        logger.debug(
            "Redis not reachable at %s:%s/%s (%s); using in-memory fallback",
            cfg.redis.host,
            cfg.redis.port,
            cfg.redis.db,
            exc,
        )
        return None


def redis_available(client: Any) -> bool:
    """Return ``True`` iff ``client`` is non-``None`` and responds to ``PING``.

    Safe to call with ``None`` or a stale/disconnected client — any error is treated as
    "not available" rather than propagated.

    Args:
        client: A :class:`redis.Redis` instance (or ``None``).

    Returns:
        Whether the client is currently usable.
    """
    if client is None:
        return False
    try:
        return bool(client.ping())
    except Exception:
        return False
