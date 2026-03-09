"""Throughput benchmark for the RabbitMQ log message queue."""

import json
import os
import sys
import time
from datetime import datetime, timezone

import pika

# Add project root to path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import Config
from src.connection import RabbitMQConnection
from src.setup import RabbitMQSetup


def run_throughput_test(num_messages=1000):
    """Publish num_messages and measure throughput.

    Returns:
        True if throughput exceeds 100 msg/sec, False otherwise.
    """
    config = Config()

    # Ensure topology exists
    setup = RabbitMQSetup(config)
    setup.setup_all()
    time.sleep(1)

    # Connect
    conn = RabbitMQConnection(config)
    conn.connect()
    channel = conn.get_channel()

    exchange = config.get_exchange_config()["name"]
    levels = ["info", "error", "debug"]

    print(f"\nThroughput Test: Publishing {num_messages} messages...")
    start = time.time()

    for i in range(num_messages):
        level = levels[i % 3]
        body = json.dumps(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "source": "throughput_test",
                "message": f"Test message {i}",
            }
        )
        channel.basic_publish(
            exchange=exchange,
            routing_key=f"logs.{level}.throughput",
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )

    elapsed = time.time() - start
    rate = num_messages / elapsed

    conn.close()

    print(f"Time: {elapsed:.2f} seconds")
    print(f"Rate: {rate:.0f} messages/second")
    passed = rate > 100
    print(f"Target: >100 msg/sec -- {'PASSED' if passed else 'FAILED'}")

    return passed


if __name__ == "__main__":
    from scripts.wait_for_rabbitmq import wait_for_rabbitmq

    if not wait_for_rabbitmq():
        sys.exit(1)

    success = run_throughput_test()
    sys.exit(0 if success else 1)
