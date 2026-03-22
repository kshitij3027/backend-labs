from gevent import monkey  # noqa: E402 — must be first
monkey.patch_all()

import logging
import signal
import sys

from src.config import load_config
from src.dashboard import create_app
from scripts.wait_for_kafka import wait_for_kafka

logger = logging.getLogger(__name__)


def main() -> None:
    """Start the Kafka streams monitoring dashboard."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    config = load_config()
    logger.info("Configuration loaded: broker=%s", config.bootstrap_servers)

    # Wait for Kafka to be available
    if not wait_for_kafka(config.bootstrap_servers):
        logger.error("Cannot reach Kafka. Exiting.")
        sys.exit(1)
    logger.info("Kafka is reachable.")

    # Create the Flask + SocketIO app
    app, socketio = create_app(config)

    # Graceful shutdown
    def _shutdown(signum, frame):
        logger.info("Received signal %s - shutting down...", signum)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Starting dashboard on %s:%d", config.dashboard_host, config.dashboard_port)
    socketio.run(app, host=config.dashboard_host, port=config.dashboard_port)


if __name__ == "__main__":
    main()
