"""Redis storage layer with injectable client for testing."""

from __future__ import annotations

import redis.asyncio as aioredis


class RedisStorage:
    """Async Redis wrapper with injectable client support.

    When ``client`` is provided (e.g. a fakeredis instance for tests),
    the :meth:`connect` call becomes a no-op — the injected client is
    used directly.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        client: aioredis.Redis | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._client = client

    @property
    def client(self) -> aioredis.Redis | None:
        return self._client

    async def connect(self) -> None:
        """Open a Redis connection if no client was injected."""
        if self._client is None:
            self._client = aioredis.Redis(
                host=self._host,
                port=self._port,
                decode_responses=True,
            )

    async def close(self) -> None:
        """Gracefully close the Redis connection."""
        if self._client:
            await self._client.aclose()

    async def ping(self) -> bool:
        """Return True if Redis is reachable, False otherwise."""
        try:
            return await self._client.ping()
        except Exception:
            return False
