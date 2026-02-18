"""Log compression client — generates, compresses, and ships logs over TCP."""

import logging
import signal
import sys
import threading

from src.config import load_client_config
from src.log_shipper import LogShipper
from src.log_generator import LogGenerator
from src.adaptive import AdaptiveCompression


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_client_config()
    shutdown = threading.Event()

    shipper = LogShipper(config, shutdown)
    adaptive = None

    def handle_signal(signum, frame):
        logging.info("Received signal %d, shutting down...", signum)
        shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        if not shipper.start():
            logging.error("Failed to connect to server")
            sys.exit(1)

        if config.adaptive_enabled:
            adaptive = AdaptiveCompression(
                compression_handler=shipper.compressor,
                min_level=config.adaptive_min_level,
                max_level=config.adaptive_max_level,
                check_interval=config.adaptive_check_interval,
            )
            adaptive.start()

        generator = LogGenerator(config.log_rate, config.run_time, shutdown)
        generator.generate(shipper.ship)
    except KeyboardInterrupt:
        pass
    finally:
        if adaptive:
            adaptive.stop()
        shipper.stop()


if __name__ == "__main__":
    main()
