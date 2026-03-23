import redis.asyncio as aioredis
import structlog

from src.config import settings

logger = structlog.get_logger()

redis_client: aioredis.Redis | None = None


async def init_redis() -> None:
    global redis_client
    logger.info("connecting_to_redis", url=settings.REDIS_URL)
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    await redis_client.ping()
    logger.info("redis_connected")


async def close_redis() -> None:
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None
        logger.info("redis_connection_closed")


def get_redis() -> aioredis.Redis:
    if redis_client is None:
        raise RuntimeError("Redis client not initialized. Call init_redis() first.")
    return redis_client
