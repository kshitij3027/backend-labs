"""Entry point for the log shipping client."""

import logging
import signal
import sys
import threading

from src.config import load_config
from src.resilient_shipper import ResilientLogShipper
from src.shipper import LogShipper


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

    logger = logging.getLogger(__name__)
    logger.info("Starting log shipper — mode=%s, file=%s, server=%s:%d",
                "batch" if config.batch_mode else "continuous",
                config.log_file, config.server_host, config.server_port)

    if config.resilient:
        logger.info("Using resilient shipper (buffered producer-consumer)")
        shipper = ResilientLogShipper(config, shutdown_event)
    else:
        shipper = LogShipper(config, shutdown_event)
    shipper.run()


if __name__ == "__main__":
    main()
