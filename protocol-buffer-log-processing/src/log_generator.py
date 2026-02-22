"""Generate realistic log entries for benchmarking and testing."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

SERVICE_NAMES: list[str] = [
    "api-gateway",
    "auth-service",
    "payment-processor",
    "user-service",
    "notification-engine",
]

LOG_LEVELS: list[str] = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LOG_LEVEL_WEIGHTS: list[int] = [10, 50, 20, 15, 5]

# ---------------------------------------------------------------------------
# Realistic log message templates per level
# ---------------------------------------------------------------------------

_MESSAGES: dict[str, list[str]] = {
    "DEBUG": [
        "Cache lookup for key '{key}' returned miss",
        "Entering request handler for {endpoint}",
        "Connection pool stats: active={active}, idle={idle}",
        "Retrying operation, attempt {attempt} of 3",
        "Serialised payload size: {size} bytes",
    ],
    "INFO": [
        "Request processed successfully in {duration}ms",
        "User {user_id} authenticated via OAuth2",
        "Health check passed for {service}",
        "Batch job completed: {processed} records processed",
        "New session created for user {user_id}",
        "Deployed version {version} to production",
    ],
    "WARNING": [
        "Response time exceeded threshold: {duration}ms > 500ms",
        "Rate limit approaching for client {client_id}: {current}/{limit}",
        "Deprecated endpoint {endpoint} called by {client_id}",
        "Memory usage at {percent}% of allocated limit",
        "Certificate expires in {days} days",
    ],
    "ERROR": [
        "Failed to connect to database: connection refused",
        "Payment processing failed for order {order_id}: timeout",
        "Authentication token expired for user {user_id}",
        "Unhandled exception in {endpoint}: {error}",
        "Message queue delivery failed after 3 retries",
    ],
    "CRITICAL": [
        "Database cluster failover initiated — primary unreachable",
        "Out of memory: service restarting",
        "Data corruption detected in partition {partition}",
        "Security breach detected: unauthorised access from {ip}",
        "All circuit breakers open — service degraded",
    ],
}

# ---------------------------------------------------------------------------
# Metadata key pools
# ---------------------------------------------------------------------------

_ENDPOINTS = ["/api/v1/users", "/api/v1/orders", "/api/v1/auth/login", "/api/v1/payments", "/api/v1/health"]
_STATUS_CODES = ["200", "201", "400", "401", "403", "404", "500", "502", "503"]


def _random_metadata() -> dict[str, str]:
    """Return a dict with 1-3 random key-value pairs."""
    pool: list[tuple[str, str]] = [
        ("request_id", str(uuid.uuid4())),
        ("user_id", f"usr_{random.randint(1000, 9999)}"),
        ("duration_ms", str(random.randint(1, 2000))),
        ("endpoint", random.choice(_ENDPOINTS)),
        ("status_code", random.choice(_STATUS_CODES)),
        ("trace_id", uuid.uuid4().hex[:16]),
        ("client_ip", f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}"),
    ]
    count = random.randint(1, 3)
    selected = random.sample(pool, k=min(count, len(pool)))
    return {k: v for k, v in selected}


def _fill_template(template: str) -> str:
    """Replace placeholders in a message template with plausible values."""
    replacements: dict[str, str] = {
        "{key}": f"session:{uuid.uuid4().hex[:8]}",
        "{endpoint}": random.choice(_ENDPOINTS),
        "{active}": str(random.randint(1, 50)),
        "{idle}": str(random.randint(0, 20)),
        "{attempt}": str(random.randint(1, 3)),
        "{size}": str(random.randint(64, 8192)),
        "{duration}": str(random.randint(1, 5000)),
        "{user_id}": f"usr_{random.randint(1000, 9999)}",
        "{service}": random.choice(SERVICE_NAMES),
        "{processed}": str(random.randint(100, 100_000)),
        "{version}": f"v{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 99)}",
        "{client_id}": f"client_{random.randint(100, 999)}",
        "{current}": str(random.randint(80, 100)),
        "{limit}": "100",
        "{percent}": str(random.randint(80, 98)),
        "{days}": str(random.randint(1, 14)),
        "{order_id}": f"ord_{uuid.uuid4().hex[:10]}",
        "{error}": random.choice(["NullPointerError", "TimeoutError", "ValueError", "ConnectionError"]),
        "{partition}": str(random.randint(0, 15)),
        "{ip}": f"203.0.113.{random.randint(1, 254)}",
    }
    result = template
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_log_entry(service_name: str | None = None) -> dict:
    """Generate a single realistic log entry as a plain dict.

    Args:
        service_name: Optional service name override. If ``None``, a random
            service is selected from :data:`SERVICE_NAMES`.

    Returns:
        A dict with keys: timestamp, service_name, level, message, metadata.
    """
    level = random.choices(LOG_LEVELS, weights=LOG_LEVEL_WEIGHTS, k=1)[0]
    template = random.choice(_MESSAGES[level])

    return {
        "timestamp": datetime.now(timezone.utc),
        "service_name": service_name or random.choice(SERVICE_NAMES),
        "level": level,
        "message": _fill_template(template),
        "metadata": _random_metadata(),
    }


def generate_log_batch(count: int, service_name: str | None = None) -> list[dict]:
    """Generate a batch of log entries.

    Args:
        count: Number of entries to generate.
        service_name: Optional service name applied to every entry.

    Returns:
        A list of log entry dicts.
    """
    return [generate_log_entry(service_name=service_name) for _ in range(count)]
