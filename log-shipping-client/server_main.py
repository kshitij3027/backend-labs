"""Entry point for the simple TCP log server."""

import logging
import os
import signal
import sys
import threading

from src.server import SimpleLogServer


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stderr,
    )

    host = os.environ.get("SERVER_HOST", "0.0.0.0")
    port = int(os.environ.get("SERVER_PORT", "9000"))
    shutdown_event = threading.Event()

    def signal_handler(signum, frame):
        logging.getLogger(__name__).info("Received signal %d, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server = SimpleLogServer(host, port, shutdown_event)
    try:
        server.start()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
