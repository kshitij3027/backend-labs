"""TLS Log Server — entry point."""

import logging
import signal
import sys
import threading

from src.config import load_server_config
from src.server import TLSLogServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    config = load_server_config()
    shutdown_event = threading.Event()

    server = TLSLogServer(config, shutdown_event)

    def handle_signal(signum, frame):
        print(f"\n[SERVER] Received signal {signum}, shutting down...")
        server.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
