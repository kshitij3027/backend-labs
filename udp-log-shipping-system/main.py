"""Entry point for the UDP Log Shipping Server."""

import logging
import signal
import sys
import threading

from src.config import load_config
from src.server import UDPLogServer


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stderr,
    )

    config = load_config()
    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        logging.getLogger(__name__).info("Received signal %d, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server = UDPLogServer(config, shutdown_event)
    try:
        server.start()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
