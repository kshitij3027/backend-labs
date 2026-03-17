"""Wait for Kafka broker to be ready."""
import os
import sys
import time

from confluent_kafka.admin import AdminClient


def wait_for_kafka(
    bootstrap_servers: str = "kafka:29092",
    timeout: int = 60,
) -> bool:
    """Block until Kafka is reachable or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                print(f"Kafka ready — {len(metadata.brokers)} broker(s)")
                return True
        except Exception:
            pass
        time.sleep(2)
    print("Timed out waiting for Kafka")
    return False


if __name__ == "__main__":
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    if not wait_for_kafka(servers):
        sys.exit(1)
