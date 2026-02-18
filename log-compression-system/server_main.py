"""Log compression server — receives and decompresses log batches over TCP."""

import logging
import signal
import sys
import threading

from src.config import load_server_config
from src.tcp_server import TCPLogReceiver
from src.metrics import ReceiverMetrics
from src.dashboard import DashboardServer


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_server_config()
    shutdown = threading.Event()
    metrics = ReceiverMetrics()

    server = TCPLogReceiver(config.host, config.port, shutdown, metrics)

    dashboard = DashboardServer(port=8080, metrics=metrics)
    dashboard.start()

    def handle_signal(signum, frame):
        logging.info("Received signal %d, shutting down...", signum)
        server.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        dashboard.stop()
        server.stop()
        stats = metrics.snapshot()
        print("\n--- Server Statistics ---")
        for key, value in stats.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
