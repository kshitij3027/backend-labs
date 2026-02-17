"""Client entry point for the Batch Log Shipper."""

import logging
import signal
import threading
import sys

from src.config import load_client_config
from src.batch_client import BatchLogClient


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

    config = load_client_config()
    shutdown_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    client = BatchLogClient(config, shutdown_event)
    logger.info(
        "Starting batch log client: target=%s:%d, batch_size=%d, flush_interval=%.1fs",
        config.target_host,
        config.target_port,
        config.batch_size,
        config.flush_interval,
    )

    try:
        client.generate_sample_logs(config.logs_per_second, config.run_time)
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
