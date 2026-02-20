"""Asyncio UDP server for log ingestion (fire-and-forget)."""

import asyncio
import json

from server.config import ServerConfig
from server.persistence import LogPersistence

LOG_LEVELS = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}


class UDPServerProtocol(asyncio.DatagramProtocol):
    """UDP protocol handler for NDJSON log messages.

    Fire-and-forget: no response is sent back to the client.
    """

    def __init__(self, config: ServerConfig, persistence: LogPersistence):
        self.config = config
        self.persistence = persistence
        self.batch: list[dict] = []
        self.batch_size = config.BATCH_SIZE
        self._min_level = LOG_LEVELS.get(config.MIN_LOG_LEVEL.upper(), 0)

    def connection_made(self, transport):
        self.transport = transport
        print(f"[UDP] Server ready on port {self.config.UDP_PORT}")

    def datagram_received(self, data: bytes, addr):
        try:
            line = data.decode("utf-8", errors="replace").strip()
            if not line:
                return

            msg = json.loads(line)
            if "level" not in msg or "message" not in msg:
                return

            level = LOG_LEVELS.get(msg["level"].upper(), -1)
            if level < self._min_level:
                return

            self.batch.append(msg)

            if len(self.batch) >= self.batch_size:
                asyncio.ensure_future(self._flush_batch())

        except (json.JSONDecodeError, Exception):
            pass

    async def _flush_batch(self):
        if self.batch and self.config.ENABLE_PERSISTENCE:
            batch = self.batch
            self.batch = []
            await self.persistence.write_batch(batch)

    def error_received(self, exc):
        print(f"[UDP] Error: {exc}")

    def connection_lost(self, exc):
        print("[UDP] Connection closed")


class UDPServer:
    """Manages the UDP server lifecycle."""

    def __init__(self, config: ServerConfig, persistence: LogPersistence):
        self.config = config
        self.persistence = persistence
        self.transport = None
        self.protocol = None

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: UDPServerProtocol(self.config, self.persistence),
            local_addr=(self.config.SERVER_HOST, self.config.UDP_PORT),
        )
        print(f"[UDP] Listening on {self.config.SERVER_HOST}:{self.config.UDP_PORT}")

    async def stop(self) -> None:
        if self.protocol:
            await self.protocol._flush_batch()
        if self.transport:
            self.transport.close()
        print("[UDP] Server stopped")
