"""Standalone data generator that produces synthetic events to Kafka topics."""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid

from confluent_kafka import Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("generate_data")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "100"))

PATHS = [
    "/api/v1/users",
    "/api/v2/orders",
    "/api/v1/products",
    "/checkout",
    "/api/v1/auth",
    "/payment",
    "/api/v2/search",
]

METHODS = ["GET", "GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"]

STATUS_CODES = [200] * 60 + [201] * 10 + [400] * 10 + [404] * 10 + [500] * 10

ERROR_TYPES = [
    "NullPointerException",
    "TimeoutError",
    "ConnectionRefused",
    "OutOfMemoryError",
    "ValidationError",
]

SEVERITIES = ["low"] * 10 + ["medium"] * 30 + ["high"] * 40 + ["critical"] * 20

SERVICES = [
    "auth-service",
    "payment-service",
    "user-service",
    "search-service",
    "notification-service",
]

USER_ACTIONS = (
    ["page_view"] * 50
    + ["login"] * 15
    + ["logout"] * 10
    + ["signup"] * 5
    + ["purchase"] * 20
)

IP_POOL = [f"10.0.{random.randint(1, 5)}.{i}" for i in range(1, 21)]

# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------

def make_log_event() -> dict:
    status = random.choice(STATUS_CODES)
    # Mostly fast responses, occasional slow ones
    response_time = random.gauss(120, 80)
    if random.random() < 0.05:
        response_time = random.uniform(800, 2000)
    response_time = max(10, response_time)

    return {
        "path": random.choice(PATHS),
        "method": random.choice(METHODS),
        "status_code": status,
        "response_time": round(response_time, 1),
        "ip_address": random.choice(IP_POOL),
        "timestamp": time.time(),
    }


def make_error_event() -> dict:
    error_type = random.choice(ERROR_TYPES)
    return {
        "error_type": error_type,
        "severity": random.choice(SEVERITIES),
        "service": random.choice(SERVICES),
        "stack_trace": (
            f"Traceback (most recent call last):\n"
            f"  File \"service.py\", line {random.randint(10, 500)}, in handle\n"
            f"    raise {error_type}(\"something went wrong\")\n"
            f"{error_type}: something went wrong"
        ),
        "timestamp": time.time(),
    }


def make_user_event() -> dict:
    return {
        "user_id": f"user-{random.randint(1, 50):03d}",
        "action": random.choice(USER_ACTIONS),
        "session_id": str(uuid.uuid4())[:8],
        "path": random.choice(PATHS),
        "timestamp": time.time(),
    }


# ---------------------------------------------------------------------------
# Kafka producer helpers
# ---------------------------------------------------------------------------

_delivery_errors = 0


def _delivery_callback(err, msg):
    global _delivery_errors
    if err is not None:
        _delivery_errors += 1
        if _delivery_errors % 100 == 1:
            logger.warning("Delivery failed: %s", err)


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 50,
        "batch.num.messages": 500,
        "queue.buffering.max.messages": 100000,
        "acks": "1",
    })


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

_running = True


def _shutdown(signum, frame):
    global _running
    logger.info("Received signal %s — stopping generator...", signum)
    _running = False


def main():
    global _delivery_errors

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info(
        "Starting data generator: broker=%s, rate=%d events/sec",
        BOOTSTRAP_SERVERS,
        EVENTS_PER_SECOND,
    )

    producer = create_producer()

    sleep_per_event = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0.01
    total_sent = 0
    last_report = time.time()

    while _running:
        try:
            # Decide topic: ~70% log, ~10% error, ~20% user
            roll = random.random()
            if roll < 0.10:
                topic = "error-events"
                payload = make_error_event()
            elif roll < 0.30:
                topic = "user-events"
                payload = make_user_event()
            else:
                topic = "log-events"
                payload = make_log_event()

            producer.produce(
                topic,
                value=json.dumps(payload).encode("utf-8"),
                callback=_delivery_callback,
            )
            total_sent += 1

            # Flush periodically to avoid buffer build-up
            if total_sent % 500 == 0:
                producer.poll(0)

            # Log summary every 10 seconds
            now = time.time()
            if now - last_report >= 10:
                producer.flush(timeout=2)
                logger.info(
                    "Sent %d events total (delivery errors: %d)",
                    total_sent,
                    _delivery_errors,
                )
                last_report = now

            time.sleep(sleep_per_event)

        except BufferError:
            producer.poll(1)
        except Exception:
            logger.exception("Unexpected error in generator loop")
            time.sleep(1)

    logger.info("Flushing remaining messages...")
    producer.flush(timeout=10)
    logger.info("Generator stopped. Total events sent: %d", total_sent)


if __name__ == "__main__":
    main()
