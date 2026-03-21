"""Main entrypoint — wires producer, consumer, monitor, and dashboard together."""

import logging
import signal
import sys
import threading
import time

from src.config import load_config
from src.producer import ProfileProducer
from src.consumer import StateConsumer
from src.monitor import CompactionMonitor
from src.dashboard import create_app

logger = logging.getLogger(__name__)


def main() -> None:
    """Start all components and run the Flask dashboard in the main thread."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    config = load_config()
    logger.info("Configuration loaded: broker=%s", config.active_bootstrap_servers)

    # --- Consumer: rebuild state, then start live consumption ---
    consumer = StateConsumer(config)
    consumer.rebuild_state()

    consumer_thread = threading.Thread(
        target=consumer.consume_loop, daemon=True, name="consumer-thread"
    )
    consumer_thread.start()
    logger.info("Consumer thread started.")

    # --- Producer: long-running background generation ---
    producer = ProfileProducer(config)
    producer_thread = threading.Thread(
        target=producer.run, args=(86400,), daemon=True, name="producer-thread"
    )
    producer_thread.start()
    logger.info("Producer thread started (duration=86400s).")

    # --- Monitor ---
    monitor = CompactionMonitor(config, consumer)

    # --- Flask dashboard ---
    app = create_app(config, consumer, monitor)

    # --- Graceful shutdown ---
    def _shutdown(signum, frame):
        logger.info("Received signal %s — shutting down …", signum)
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info(
        "Starting dashboard on %s:%d …", config.dashboard_host, config.dashboard_port
    )
    app.run(
        host=config.dashboard_host,
        port=config.dashboard_port,
        debug=False,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
