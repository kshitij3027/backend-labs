"""Multi-service log producer with key-based partitioning."""

import random
import time
import uuid
from datetime import datetime, timezone

import structlog
from confluent_kafka import Producer

from src.config import Settings, load_config
from src.models import LogLevel, LogMessage, ServiceName, TOPIC_MAP

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Service-specific configuration
# ---------------------------------------------------------------------------

SERVICE_CONFIG: dict[ServiceName, dict] = {
    ServiceName.WEB_API: {
        "endpoints": [
            "/api/users",
            "/api/orders",
            "/api/products",
            "/api/auth",
            "/api/search",
        ],
        "status_weights": {
            200: 70,
            201: 10,
            400: 10,
            404: 5,
            500: 5,
        },
        "level_weights": {
            LogLevel.INFO: 80,
            LogLevel.WARN: 10,
            LogLevel.ERROR: 10,
        },
    },
    ServiceName.USER_SERVICE: {
        "endpoints": [
            "/auth/login",
            "/auth/register",
            "/users/profile",
            "/users/settings",
            "/auth/reset-password",
        ],
        "status_weights": {
            200: 60,
            201: 15,
            401: 10,
            403: 5,
            500: 10,
        },
        "level_weights": {
            LogLevel.INFO: 70,
            LogLevel.WARN: 20,
            LogLevel.ERROR: 10,
        },
    },
    ServiceName.PAYMENT_SERVICE: {
        "endpoints": [
            "/payments/process",
            "/payments/refund",
            "/payments/status",
            "/payments/webhook",
            "/payments/verify",
        ],
        "status_weights": {
            200: 55,
            201: 15,
            400: 10,
            402: 8,
            500: 12,
        },
        "level_weights": {
            LogLevel.INFO: 65,
            LogLevel.WARN: 15,
            LogLevel.ERROR: 20,
        },
    },
}

# Realistic message templates keyed by status-code range
_MESSAGE_TEMPLATES: dict[str, list[str]] = {
    "2xx": [
        "Request processed successfully",
        "Resource retrieved",
        "Operation completed",
        "Data returned to client",
        "Action fulfilled",
    ],
    "4xx": [
        "Bad request: invalid parameters",
        "Resource not found",
        "Unauthorized access attempt",
        "Forbidden: insufficient permissions",
        "Payment required",
    ],
    "5xx": [
        "Internal server error",
        "Service temporarily unavailable",
        "Gateway timeout",
        "Database connection failed",
        "Upstream dependency error",
    ],
}


def _pick_message(status_code: int) -> str:
    """Return a realistic log message based on the HTTP status code."""
    if 200 <= status_code < 300:
        return random.choice(_MESSAGE_TEMPLATES["2xx"])
    if 400 <= status_code < 500:
        return random.choice(_MESSAGE_TEMPLATES["4xx"])
    return random.choice(_MESSAGE_TEMPLATES["5xx"])


# ---------------------------------------------------------------------------
# ServiceSimulator
# ---------------------------------------------------------------------------


class ServiceSimulator:
    """Generates realistic log messages for a single service type."""

    def __init__(self, service_name: ServiceName) -> None:
        self.service_name = service_name
        self.sequence_counter = 0
        self.user_pool = [str(uuid.uuid4()) for _ in range(50)]

        cfg = SERVICE_CONFIG[service_name]
        self.endpoints: list[str] = cfg["endpoints"]

        status_weights = cfg["status_weights"]
        self._status_codes = list(status_weights.keys())
        self._status_weights = list(status_weights.values())

        level_weights = cfg["level_weights"]
        self._levels = list(level_weights.keys())
        self._level_weights = list(level_weights.values())

    def generate_message(self) -> LogMessage:
        """Generate a single realistic log message."""
        self.sequence_counter += 1

        endpoint = random.choice(self.endpoints)
        status_code = random.choices(self._status_codes, weights=self._status_weights, k=1)[0]
        level = random.choices(self._levels, weights=self._level_weights, k=1)[0]
        user_id = random.choice(self.user_pool)

        return LogMessage(
            timestamp=datetime.now(timezone.utc).isoformat(),
            service=self.service_name,
            level=level,
            endpoint=endpoint,
            status_code=status_code,
            user_id=user_id,
            message=_pick_message(status_code),
            sequence_number=self.sequence_counter,
        )


# ---------------------------------------------------------------------------
# LogProducer
# ---------------------------------------------------------------------------


class LogProducer:
    """Kafka producer that sends structured log messages."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.producer = Producer({
            "bootstrap.servers": settings.active_bootstrap_servers,
            "client.id": "log-producer",
            "batch.size": settings.producer_batch_size,
            "linger.ms": settings.producer_linger_ms,
            "compression.type": settings.producer_compression,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
        })
        self.delivered_count = 0
        self.failed_count = 0

    def _delivery_callback(self, err, msg):
        """Callback invoked once per produced message to track delivery status."""
        if err:
            self.failed_count += 1
            logger.error("delivery_failed", error=str(err), topic=msg.topic())
        else:
            self.delivered_count += 1

    def produce(self, message: LogMessage) -> None:
        """Produce a single log message to the appropriate Kafka topic."""
        self.producer.produce(
            topic=message.topic,
            value=message.to_kafka_value(),
            key=message.partition_key,
            callback=self._delivery_callback,
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all buffered messages to Kafka, blocking up to *timeout* seconds."""
        return self.producer.flush(timeout=timeout)

    def close(self) -> None:
        """Flush remaining messages and log final delivery statistics."""
        self.flush()
        logger.info(
            "producer_closed",
            delivered=self.delivered_count,
            failed=self.failed_count,
        )


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------


def run_producer(settings: Settings) -> None:
    """Run all 3 service simulators for the configured duration."""
    producer = LogProducer(settings)
    simulators = [
        ServiceSimulator(ServiceName.WEB_API),
        ServiceSimulator(ServiceName.USER_SERVICE),
        ServiceSimulator(ServiceName.PAYMENT_SERVICE),
    ]

    logger.info(
        "producer_starting",
        duration=settings.producer_duration_seconds,
        rate=settings.producer_rate_per_second,
    )

    start_time = time.time()
    interval = (
        1.0 / settings.producer_rate_per_second
        if settings.producer_rate_per_second > 0
        else 0.01
    )
    message_count = 0

    while time.time() - start_time < settings.producer_duration_seconds:
        for sim in simulators:
            msg = sim.generate_message()
            producer.produce(msg)
            message_count += 1

            # Duplicate ERROR messages to the critical-logs topic
            if msg.level == LogLevel.ERROR:
                producer.producer.produce(
                    topic=settings.critical_topic,
                    value=msg.to_kafka_value(),
                    key=msg.partition_key,
                    callback=producer._delivery_callback,
                )

        time.sleep(interval)

    producer.close()

    elapsed = time.time() - start_time
    logger.info(
        "producer_finished",
        messages=message_count,
        elapsed=f"{elapsed:.1f}s",
        rate=f"{message_count / elapsed:.0f} msg/s",
        delivered=producer.delivered_count,
        failed=producer.failed_count,
    )


if __name__ == "__main__":
    settings = load_config()
    run_producer(settings)
