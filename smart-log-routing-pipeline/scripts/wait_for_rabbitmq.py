"""Wait for RabbitMQ management API to become available before proceeding."""

import os
import sys
import time

import requests


def wait_for_rabbitmq(host=None, management_port=15672, max_retries=30, retry_delay=2):
    """Poll the RabbitMQ management API until it is ready.

    Args:
        host: RabbitMQ hostname (defaults to RABBITMQ_HOST env var or localhost).
        management_port: Management API port (default 15672).
        max_retries: Maximum number of connection attempts.
        retry_delay: Seconds between retries.

    Returns:
        True if RabbitMQ is reachable, False otherwise.
    """
    if host is None:
        host = os.environ.get("RABBITMQ_HOST", "localhost")

    url = f"http://{host}:{management_port}/api/overview"
    print(f"Waiting for RabbitMQ management API at {url}...")

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, auth=("guest", "guest"), timeout=5)
            if response.status_code == 200:
                print(f"RabbitMQ is ready! (connected on attempt {attempt})")
                return True
            else:
                print(
                    f"Attempt {attempt}/{max_retries}: "
                    f"Got status {response.status_code}, retrying in {retry_delay}s..."
                )
        except requests.exceptions.ConnectionError:
            print(
                f"Attempt {attempt}/{max_retries}: "
                f"RabbitMQ not ready, retrying in {retry_delay}s..."
            )
        except requests.exceptions.Timeout:
            print(
                f"Attempt {attempt}/{max_retries}: "
                f"Request timed out, retrying in {retry_delay}s..."
            )
        time.sleep(retry_delay)

    print(f"Failed to connect to RabbitMQ after {max_retries} attempts.")
    return False


if __name__ == "__main__":
    success = wait_for_rabbitmq()
    sys.exit(0 if success else 1)
