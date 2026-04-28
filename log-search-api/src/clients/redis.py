from __future__ import annotations

import asyncio
import logging

import redis.asyncio as redis
from fastapi import Request

logger = logging.getLogger(__name__)


def make_redis_pool(url: str, db: int) -> redis.ConnectionPool:
    return redis.ConnectionPool.from_url(
        url,
        db=db,
        max_connections=50,
        decode_responses=False,
    )


def make_redis_client(pool: redis.ConnectionPool) -> redis.Redis:
    return redis.Redis(connection_pool=pool)


async def ping_redis(client: redis.Redis) -> tuple[bool, str | None]:
    try:
        await asyncio.wait_for(client.ping(), timeout=2.0)
        return True, None
    except Exception as exc:
        return False, str(exc)


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis_cache
