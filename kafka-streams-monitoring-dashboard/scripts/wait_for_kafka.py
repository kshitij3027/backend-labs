"""Wait for Kafka to become reachable before starting the application."""

import os
import sys
import time
import logging

logger = logging.getLogger(__name__)


def wait_for_kafka(
    bootstrap_servers: str = "kafka:29092",
    max_retries: int = 30,
    retry_interval: float = 2.0,
) -> bool:
    """Block until Kafka is reachable or max retries exhausted.

    Returns True if Kafka is reachable, False otherwise.
    """
    from confluent_kafka.admin import AdminClient

    for attempt in range(1, max_retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin.list_topics(timeout=5)
            logger.info(
                "Kafka is reachable (attempt %d/%d). Topics: %s",
                attempt,
                max_retries,
                list(metadata.topics.keys()),
            )
            return True
        except Exception as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s", attempt, max_retries, exc
            )
            time.sleep(retry_interval)

    logger.error("Kafka not reachable after %d attempts.", max_retries)
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    success = wait_for_kafka(servers)
    sys.exit(0 if success else 1)
