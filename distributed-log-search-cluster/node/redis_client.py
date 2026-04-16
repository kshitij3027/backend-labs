"""Async Redis client with a shared connection pool."""

from __future__ import annotations

from redis.asyncio import ConnectionPool, Redis

_pool: ConnectionPool | None = None


def get_redis(host: str, port: int, db: int = 0) -> Redis:
    """Return a Redis client backed by a shared async connection pool."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            max_connections=50,
        )
    return Redis(connection_pool=_pool)


async def close_redis() -> None:
    """Dispose of the shared connection pool, if any."""
    global _pool
    if _pool is not None:
        await _pool.disconnect()
        _pool = None
