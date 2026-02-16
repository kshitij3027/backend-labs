"""Entry point for the UDP Log Shipping Server."""

import logging
import os
import signal
import sys
import threading

from src.config import load_config
from src.dashboard import create_dashboard_app, run_dashboard
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

    dashboard_port = int(os.environ.get("DASHBOARD_PORT", 8080))
    app = create_dashboard_app(server.metrics, server.error_tracker)
    dash_thread = threading.Thread(target=run_dashboard, args=(app, dashboard_port), daemon=True)
    dash_thread.start()
    logging.getLogger(__name__).info("Dashboard running on port %d", dashboard_port)

    try:
        server.start()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
