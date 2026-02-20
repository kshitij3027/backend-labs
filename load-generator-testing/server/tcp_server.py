"""Asyncio TCP server for log ingestion."""

import asyncio
import ssl

from server.circuit_breaker import CircuitBreaker
from server.config import ServerConfig
from server.handler import ConnectionHandler
from server.persistence import LogPersistence


class TCPServer:
    """Async TCP server that accepts NDJSON log messages."""

    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self.persistence = LogPersistence(config.LOG_DIR)
        self.server: asyncio.Server | None = None
        self.shutdown_event = asyncio.Event()
        self.active_connections = 0
        self.ssl_context: ssl.SSLContext | None = None
        self.circuit_breaker: CircuitBreaker | None = None

    async def start(self) -> None:
        """Start the TCP server and serve until shutdown."""
        # TLS setup
        if self.config.ENABLE_TLS:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_ctx.load_cert_chain(
                certfile=f"{self.config.CERT_DIR}/server.crt",
                keyfile=f"{self.config.CERT_DIR}/server.key",
            )
            self.ssl_context = ssl_ctx
            print("[SERVER] TLS enabled")

        # Circuit breaker setup
        if self.config.CIRCUIT_BREAKER_ENABLED:
            self.circuit_breaker = CircuitBreaker()
            print("[SERVER] Circuit breaker enabled")

        self.server = await asyncio.start_server(
            self._client_connected,
            self.config.SERVER_HOST,
            self.config.SERVER_PORT,
            ssl=self.ssl_context,
        )

        addrs = ", ".join(
            str(sock.getsockname()) for sock in self.server.sockets
        )
        print(f"Server listening on {addrs}")

        async with self.server:
            await self.shutdown_event.wait()

    async def _client_connected(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Callback for each new client connection."""
        self.active_connections += 1
        try:
            handler = ConnectionHandler(
                self.config, self.persistence, self.circuit_breaker
            )
            await handler.handle_client(reader, writer)
        except Exception as e:
            print(f"[SERVER] Error in client handler: {e}")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        finally:
            self.active_connections -= 1

    async def stop(self) -> None:
        """Gracefully stop the server."""
        print("Shutting down server...")
        self.shutdown_event.set()

        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

        await self.persistence.close()
        print("Server stopped")
