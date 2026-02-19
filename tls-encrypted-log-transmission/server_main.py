"""TLS Log Server — entry point."""

import logging
import signal
import sys
import threading

from src.config import load_server_config
from src.server import TLSLogServer
from src.log_rotation import RotatingLogWriter
from src.handler import set_log_writer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main():
    config = load_server_config()
    shutdown_event = threading.Event()

    log_writer = RotatingLogWriter(config.log_dir, config.max_logs_per_file)
    set_log_writer(log_writer)
    print(f"[SERVER] Log rotation enabled — max {config.max_logs_per_file} entries/file in {config.log_dir}")

    server = TLSLogServer(config, shutdown_event)

    def handle_signal(signum, frame):
        print(f"\n[SERVER] Received signal {signum}, shutting down...")
        server.stop()
        log_writer.close()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()
        log_writer.close()


if __name__ == "__main__":
    main()
