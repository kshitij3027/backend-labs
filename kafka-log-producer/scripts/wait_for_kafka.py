"""Wait for Kafka broker to be ready."""

import sys
import time

from confluent_kafka.admin import AdminClient


def wait_for_kafka(bootstrap_servers: str = "kafka:29092", timeout: int = 60) -> bool:
    """Poll the Kafka broker until it responds or timeout is reached."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                print(f"Kafka ready: {len(metadata.brokers)} broker(s)")
                return True
        except Exception as e:
            print(f"Waiting for Kafka... ({e})")
        time.sleep(2)
    print("Timeout waiting for Kafka")
    return False


if __name__ == "__main__":
    servers = sys.argv[1] if len(sys.argv) > 1 else "kafka:29092"
    if not wait_for_kafka(servers):
        sys.exit(1)
