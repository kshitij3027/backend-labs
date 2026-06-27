"""External-service client wrappers for the Predictive Log Analytics Engine.

Currently this package holds the Redis client + prediction-cache helpers
(:mod:`src.clients.redis`). Redis is used *only* as a fast read-through cache for
generated forecasts — PostgreSQL remains the durable source of truth, so every
cache operation degrades gracefully when Redis is unreachable.
"""

from __future__ import annotations

from src.clients.redis import (
    cache_prediction,
    get_cached_prediction,
    get_redis,
    ping,
)

__all__ = [
    "get_redis",
    "cache_prediction",
    "get_cached_prediction",
    "ping",
]
