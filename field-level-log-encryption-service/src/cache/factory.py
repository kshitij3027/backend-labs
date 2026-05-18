"""Cache backend factory.

Production deployments want Redis (shared state across processes,
persistence across restarts). Development and CI want zero-dep in-process
caching that works even when Redis hasn't been started. :func:`build_cache`
papers over the difference: it tries Redis first; on failure (refused
connection, DNS miss, timeout) it logs a warning and returns an
:class:`~src.cache.in_memory.InMemoryCache` instead.

The fallback is opt-in via ``fallback_to_memory=True`` (the default).
Callers who explicitly want "Redis-or-die" can pass ``False`` — the
integration test suite uses that mode to assert the failure path.

Logging note: the fallback warning goes to the standard ``src.cache``
logger so it shows up in ``docker compose logs app`` alongside the rest
of the startup output. We deliberately use ``logger.warning`` not
``logger.error`` — falling back is operationally degraded (no
cross-process counter sharing) but not a service outage.
"""
from __future__ import annotations

import logging

from .in_memory import InMemoryCache
from .provider import CacheProvider, CacheUnavailable
from .redis_cache import RedisCache

logger = logging.getLogger(__name__)


def build_cache(
    host: str,
    port: int,
    *,
    fallback_to_memory: bool = True,
) -> CacheProvider:
    """Build a :class:`CacheProvider`, preferring Redis.

    Parameters
    ----------
    host : str
        Redis host (typically the compose service name ``redis``).
    port : int
        Redis port (typically 6379).
    fallback_to_memory : bool
        When ``True`` (default), a :class:`CacheUnavailable` from the
        Redis constructor is caught, logged at WARNING level, and an
        :class:`~src.cache.in_memory.InMemoryCache` is returned in its
        place. When ``False``, the exception propagates — useful in
        tests where we want to assert the failure mode directly.

    Returns
    -------
    CacheProvider
        Either a connected :class:`~src.cache.redis_cache.RedisCache`
        or (on Redis failure with fallback enabled) an
        :class:`~src.cache.in_memory.InMemoryCache`.

    Raises
    ------
    CacheUnavailable
        Only when ``fallback_to_memory=False`` and Redis is unreachable.
    """
    try:
        return RedisCache(host=host, port=port)
    except CacheUnavailable as exc:
        if not fallback_to_memory:
            # Caller wants Redis-or-die — re-raise verbatim so the
            # downstream handler sees the original failure detail.
            raise
        # Fallback path: log a one-line warning so operators notice
        # they're running in degraded mode, then return the pure-Python
        # backend. The warning is intentionally short — a long
        # traceback here would drown out the rest of the startup log.
        logger.warning(
            "Redis unreachable, falling back to InMemoryCache: %s", exc
        )
        return InMemoryCache()
