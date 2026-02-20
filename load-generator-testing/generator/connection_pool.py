import asyncio
import ssl
from typing import Optional


class ConnectionPool:
    def __init__(
        self,
        host: str,
        port: int,
        size: int,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.host = host
        self.port = port
        self.size = size
        self.ssl_context = ssl_context
        self._pool: asyncio.Queue = asyncio.Queue(maxsize=size)
        self._created = 0

    async def _create_connection(self):
        kwargs = {}
        if self.ssl_context:
            kwargs["ssl"] = self.ssl_context
            kwargs["server_hostname"] = self.host
        reader, writer = await asyncio.open_connection(
            self.host, self.port, **kwargs
        )
        return reader, writer

    async def acquire(self):
        # Try to get from pool first (non-blocking)
        try:
            reader, writer = self._pool.get_nowait()
            # Verify connection is alive
            if writer.is_closing():
                return await self._create_connection()
            return reader, writer
        except asyncio.QueueEmpty:
            pass

        # Create new if under limit
        if self._created < self.size:
            self._created += 1
            return await self._create_connection()

        # Wait for one to become available
        reader, writer = await self._pool.get()
        if writer.is_closing():
            return await self._create_connection()
        return reader, writer

    async def release(self, reader, writer):
        if writer.is_closing():
            return
        try:
            self._pool.put_nowait((reader, writer))
        except asyncio.QueueFull:
            writer.close()
            await writer.wait_closed()

    async def close_all(self):
        while not self._pool.empty():
            try:
                reader, writer = self._pool.get_nowait()
                writer.close()
                await writer.wait_closed()
            except (asyncio.QueueEmpty, Exception):
                break
