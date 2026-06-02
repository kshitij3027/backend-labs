"""FastAPI dependency providers backed by ``app.state``.

The whole cache object graph (L1, L2, Postgres pool, metrics, pattern engine,
cache manager, warmer) is constructed once during the lifespan startup (see
:mod:`src.main`) and stashed on ``app.state``. These thin providers hand each
piece to route handlers via ``Annotated[..., Depends(...)]`` so the routes never
reach into ``app.state`` directly.
"""

from __future__ import annotations

from fastapi import Request

from src.cache_manager import CacheManager
from src.l1_cache import L1Cache
from src.l2_redis import L2Redis
from src.metrics import Metrics
from src.patterns import PatternEngine
from src.warmer import Warmer


def get_cache_manager(request: Request) -> CacheManager:
    """Return the process-wide :class:`CacheManager` from ``app.state``."""
    return request.app.state.cache_manager


def get_metrics(request: Request) -> Metrics:
    """Return the :class:`Metrics` aggregator from ``app.state``."""
    return request.app.state.metrics


def get_patterns(request: Request) -> PatternEngine:
    """Return the :class:`PatternEngine` from ``app.state``."""
    return request.app.state.patterns


def get_warmer(request: Request) -> Warmer:
    """Return the background :class:`Warmer` from ``app.state``."""
    return request.app.state.warmer


def get_l1(request: Request) -> L1Cache:
    """Return the in-process :class:`L1Cache` tier from ``app.state``."""
    return request.app.state.l1


def get_l2(request: Request) -> L2Redis:
    """Return the :class:`L2Redis` tier from ``app.state``."""
    return request.app.state.l2


def get_pg_pool(request: Request):
    """Return the asyncpg pool (L3 + slow backend) from ``app.state``."""
    return request.app.state.pg_pool
