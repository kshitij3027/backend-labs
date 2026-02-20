"""Entry point for the TCP log server."""

import asyncio
import signal
import sys

from server.config import ServerConfig
from server.tcp_server import TCPServer


async def main() -> None:
    config = ServerConfig.from_env()
    server = TCPServer(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(server.stop()))

    print(f"Starting server on {config.SERVER_HOST}:{config.SERVER_PORT}")
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
