"""Poll RabbitMQ until it is ready or retries are exhausted."""

import os
import sys
import time

import pika


def wait_for_rabbitmq(host=None, port=None, retries=30, delay=2):
    """Block until RabbitMQ accepts a connection or retries run out."""
    if host is None:
        host = os.environ.get("RABBITMQ_HOST", "localhost")
    if port is None:
        port = int(os.environ.get("RABBITMQ_PORT", "5672"))

    for attempt in range(1, retries + 1):
        try:
            print(f"[{attempt}/{retries}] Connecting to RabbitMQ at {host}:{port}...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, port=port)
            )
            connection.close()
            print("RabbitMQ is ready!")
            return True
        except Exception as exc:
            print(f"  Not ready yet: {exc}")
            if attempt < retries:
                time.sleep(delay)

    print("ERROR: RabbitMQ did not become ready in time.")
    return False


if __name__ == "__main__":
    success = wait_for_rabbitmq()
    sys.exit(0 if success else 1)
