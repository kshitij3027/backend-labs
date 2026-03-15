#!/usr/bin/env python3
"""Wait for Kafka brokers to be ready before starting application services."""

import os
import sys
import time

from confluent_kafka.admin import AdminClient


def wait_for_kafka(
    bootstrap_servers: str,
    max_retries: int = 30,
    retry_delay: float = 2.0,
) -> bool:
    """Block until at least one Kafka broker responds or retries are exhausted.

    Returns True if brokers are reachable, False otherwise.
    """
    for attempt in range(1, max_retries + 1):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                print(f"Kafka ready: {len(metadata.brokers)} broker(s) available")
                return True
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries}: Kafka not ready ({e})")
        time.sleep(retry_delay)

    print("ERROR: Kafka did not become ready")
    return False


if __name__ == "__main__":
    servers = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
    if not wait_for_kafka(servers):
        sys.exit(1)
