"""Test log producer for generating realistic Kafka messages."""
import argparse
import json
import logging
import random
import sys
import time

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Realistic data pools
ENDPOINTS = [
    "/api/users", "/api/orders", "/api/products", "/api/auth/login",
    "/api/auth/logout", "/api/search", "/api/cart", "/api/checkout",
    "/api/health", "/api/metrics",
]
METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE"]  # weighted toward GET
STATUS_WEIGHTS = [
    (200, 70), (201, 10), (301, 5), (400, 5), (404, 5), (500, 3), (503, 2),
]
GEO_REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-east"]
SERVICES = ["auth-service", "user-service", "order-service", "payment-service", "search-service"]
COMPONENTS = ["handler", "middleware", "database", "cache", "queue"]
LOG_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]
ERROR_TYPES = [
    "NullPointerException", "TimeoutException", "ConnectionRefused",
    "OutOfMemoryError", "IndexOutOfBoundsException", "SerializationError",
]
SEVERITIES = ["ERROR", "ERROR", "ERROR", "CRITICAL"]


def _weighted_choice(weights: list[tuple]) -> int:
    """Pick a value from weighted list of (value, weight) tuples."""
    values, ws = zip(*weights)
    return random.choices(values, weights=ws, k=1)[0]


def generate_web_log() -> dict:
    return {
        "timestamp": time.time(),
        "log_type": "web_access",
        "endpoint": random.choice(ENDPOINTS),
        "method": random.choice(METHODS),
        "status_code": _weighted_choice(STATUS_WEIGHTS),
        "response_time_ms": round(max(1.0, random.gauss(50, 30)), 2),
        "source_ip": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "geo": random.choice(GEO_REGIONS),
    }


def generate_app_log() -> dict:
    return {
        "timestamp": time.time(),
        "log_type": "app_log",
        "service": random.choice(SERVICES),
        "component": random.choice(COMPONENTS),
        "level": random.choice(LOG_LEVELS),
        "message": f"Operation completed in {random.randint(1, 500)}ms",
    }


def generate_error_log() -> dict:
    return {
        "timestamp": time.time(),
        "log_type": "error_log",
        "error_type": random.choice(ERROR_TYPES),
        "stack_trace": f"at com.example.Service.process(Service.java:{random.randint(10, 500)})",
        "endpoint": random.choice(ENDPOINTS),
        "severity": random.choice(SEVERITIES),
        "message": f"Error processing request: {random.choice(ERROR_TYPES)}",
    }


TOPIC_GENERATORS = {
    "web-logs": generate_web_log,
    "app-logs": generate_app_log,
    "error-logs": generate_error_log,
}


class TestLogProducer:
    """Produces realistic log messages to Kafka topics."""

    def __init__(self, bootstrap_servers: str = "kafka:29092") -> None:
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 5,
            "batch.num.messages": 100,
        })
        self._sent = 0
        self._failed = 0

    def _delivery_callback(self, err, msg):
        if err:
            self._failed += 1
            logger.error("Delivery failed: %s", err)
        else:
            self._sent += 1

    def produce(self, count: int = 500, rate: float = 50.0) -> dict:
        """Produce `count` messages at `rate` messages/sec."""
        interval = 1.0 / rate if rate > 0 else 0
        topics = list(TOPIC_GENERATORS.keys())
        topic_weights = [60, 25, 15]  # web: 60%, app: 25%, error: 15%

        logger.info("Producing %d messages at %.0f msg/sec", count, rate)
        start = time.time()

        for i in range(count):
            topic = random.choices(topics, weights=topic_weights, k=1)[0]
            message = TOPIC_GENERATORS[topic]()
            value = json.dumps(message).encode()

            self._producer.produce(
                topic=topic,
                value=value,
                callback=self._delivery_callback,
            )

            # Flush periodically
            if (i + 1) % 100 == 0:
                self._producer.flush(timeout=5)
                logger.info("Progress: %d/%d sent", i + 1, count)

            if interval > 0:
                time.sleep(interval)

        self._producer.flush(timeout=30)
        elapsed = time.time() - start

        result = {
            "total_sent": self._sent,
            "total_failed": self._failed,
            "elapsed_seconds": round(elapsed, 2),
            "actual_rate": round(self._sent / elapsed, 2) if elapsed > 0 else 0,
        }
        logger.info("Production complete: %s", result)
        return result


def main():
    import os
    parser = argparse.ArgumentParser(description="Test Log Producer")
    parser.add_argument("--count", type=int, default=500, help="Number of messages")
    parser.add_argument("--rate", type=float, default=50.0, help="Messages per second")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    args = parser.parse_args()

    producer = TestLogProducer(args.bootstrap_servers)
    result = producer.produce(count=args.count, rate=args.rate)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
