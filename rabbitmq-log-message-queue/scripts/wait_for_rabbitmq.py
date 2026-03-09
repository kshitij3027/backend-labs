"""Wait for RabbitMQ to become available before proceeding."""

import os
import sys
import time

import pika
import pika.exceptions


def wait_for_rabbitmq(host=None, port=5672, max_retries=30, retry_delay=2):
    """Try to connect to RabbitMQ, retrying up to max_retries times."""
    if host is None:
        host = os.environ.get("RABBITMQ_HOST", "localhost")

    print(f"Waiting for RabbitMQ at {host}:{port}...")

    for attempt in range(1, max_retries + 1):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, port=port)
            )
            connection.close()
            print(f"RabbitMQ is ready! (connected on attempt {attempt})")
            sys.exit(0)
        except pika.exceptions.AMQPConnectionError:
            print(f"Attempt {attempt}/{max_retries}: RabbitMQ not ready, retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    print(f"Failed to connect to RabbitMQ after {max_retries} attempts.")
    sys.exit(1)


if __name__ == "__main__":
    wait_for_rabbitmq()
