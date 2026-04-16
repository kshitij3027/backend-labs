"""Sample log data generator for development and testing."""

import random
import time
import uuid

from backend.models import DocumentInput

# ---------------------------------------------------------------------------
# Template building blocks
# ---------------------------------------------------------------------------

_SERVICES = [
    "auth-service",
    "api-gateway",
    "payment-service",
    "user-service",
    "notification-service",
]

_LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

_EMAILS = [
    "admin@corp.com",
    "user42@example.com",
    "jdoe@startup.io",
    "ops@infra.net",
    "support@acme.org",
]

_IPS = [
    "192.168.1.100",
    "10.0.0.42",
    "172.16.5.8",
    "203.0.113.55",
    "198.51.100.12",
]

_PATHS = [
    "/api/users",
    "/api/orders",
    "/api/payments",
    "/api/auth/login",
    "/api/notifications/send",
]

_DOMAINS = [
    "db.primary.internal",
    "cache.redis.internal",
    "queue.rabbitmq.internal",
    "storage.s3.internal",
]

_COUNTRIES = ["US", "GB", "DE", "JP", "BR", "IN", "AU", "CA"]


def _random_amount() -> str:
    return f"{random.uniform(9.99, 999.99):.2f}"


def _random_ms() -> int:
    return random.randint(5, 3000)


def _random_order_id() -> int:
    return random.randint(10000, 99999)


# ---------------------------------------------------------------------------
# Message templates -- each is a callable returning a log message string
# ---------------------------------------------------------------------------

_TEMPLATES = [
    lambda: f"Authentication failed for user {random.choice(_EMAILS)} from {random.choice(_IPS)}",
    lambda: f"HTTP 500 Internal Server Error on endpoint {random.choice(_PATHS)}",
    lambda: f"Connection timeout after {random.randint(5000, 30000)}ms to {random.choice(_DOMAINS)}",
    lambda: f"Request completed in {_random_ms()}ms: GET {random.choice(_PATHS)}",
    lambda: f"Rate limit exceeded for IP {random.choice(_IPS)} - {random.randint(500, 2000)} requests/min",
    lambda: f"Cache miss for key session:{uuid.uuid4().hex[:12]}",
    lambda: f"Payment processed successfully: ${_random_amount()} for order #{_random_order_id()}",
    lambda: f"TLS handshake failed: certificate expired for {random.choice(_DOMAINS)}",
    lambda: f"Disk usage at {random.randint(70, 98)}% on /var/log - cleanup recommended",
    lambda: f"New user registered: {random.choice(_EMAILS)} from {random.choice(_COUNTRIES)}",
    lambda: f"Database query took {_random_ms()}ms on table users - slow query warning",
    lambda: f"Failed to send notification email to {random.choice(_EMAILS)} - SMTP timeout",
    lambda: f"JWT token expired for session {uuid.uuid4().hex[:16]}",
    lambda: f"Healthcheck passed for {random.choice(_SERVICES)} on port {random.randint(8000, 9000)}",
    lambda: f"OutOfMemoryError in {random.choice(_SERVICES)} - heap usage 95%",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_sample_logs(count: int = 10) -> list[DocumentInput]:
    """Generate *count* realistic sample log entries.

    Each entry has a randomly selected message template, a timestamp within
    the last seven days, a random service name, and a random log level.
    """
    now = time.time()
    logs: list[DocumentInput] = []

    for _ in range(count):
        template = random.choice(_TEMPLATES)
        logs.append(
            DocumentInput(
                message=template(),
                timestamp=now - random.uniform(0, 86400 * 7),
                service=random.choice(_SERVICES),
                level=random.choice(_LEVELS),
            )
        )

    return logs
