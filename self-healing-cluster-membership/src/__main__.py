"""Entry point for the self-healing cluster membership system."""

import asyncio
import logging
import signal
import sys

from src.config import load_config
from src.node import ClusterMember

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    config = load_config()
    node = ClusterMember(config)

    # Handle graceful shutdown
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Received shutdown signal")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    # Start the node
    await node.start()

    # Join the cluster via seed nodes
    # Small delay to let other nodes start their HTTP servers
    await asyncio.sleep(2)
    await node.join_cluster()

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Graceful shutdown
    await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
