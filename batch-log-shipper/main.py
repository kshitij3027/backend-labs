"""Server entry point for the Batch Log Shipper."""

import logging
import signal
import threading

from src.config import load_server_config
from src.server import UDPLogServer


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    config = load_server_config()
    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    server = UDPLogServer(config, shutdown_event)
    logger.info(
        "Starting Batch Log Shipper server on %s:%d", config.host, config.port
    )

    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
