"""Entry point for the TCP log server."""

import asyncio
import signal
import sys

from server.config import ServerConfig
from server.tcp_server import TCPServer

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print("Using uvloop for enhanced performance")
except ImportError:
    pass


async def main() -> None:
    config = ServerConfig.from_env()
    tcp_server = TCPServer(config)

    udp_server = None
    if config.ENABLE_UDP:
        from server.udp_server import UDPServer
        udp_server = UDPServer(config, tcp_server.persistence)

    async def shutdown():
        if udp_server:
            await udp_server.stop()
        await tcp_server.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

    print(f"Starting server on {config.SERVER_HOST}:{config.SERVER_PORT}")

    if udp_server:
        await udp_server.start()

    await tcp_server.start()


if __name__ == "__main__":
    asyncio.run(main())
