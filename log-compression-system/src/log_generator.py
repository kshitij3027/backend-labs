"""Synthetic log generator at configurable rate."""

import random
import logging
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARNING", "ERROR"]
SERVICES = ["api-gateway", "auth-service", "user-service", "payment-service", "notification-service"]
ACTIONS = ["process_request", "validate_token", "fetch_user", "charge_payment", "send_email", "query_db"]
STATUSES = ["success", "success", "success", "success", "failure", "timeout"]
MESSAGES = [
    "Processing incoming request",
    "Authenticating user credentials",
    "Fetching user profile data",
    "Processing payment transaction",
    "Sending notification email",
    "Executing database query",
    "Cache hit for user session",
    "Rate limit check passed",
    "Health check completed",
    "Connection pool acquired",
]


def generate_log_entry() -> dict:
    """Generate a single synthetic log entry as a dict."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": random.choice(LEVELS),
        "service": random.choice(SERVICES),
        "action": random.choice(ACTIONS),
        "duration_ms": round(random.uniform(0.5, 500.0), 2),
        "status": random.choice(STATUSES),
        "message": random.choice(MESSAGES),
        "user_id": f"user_{random.randint(1, 1000)}",
        "request_id": f"req-{random.randint(100000, 999999)}",
    }


class LogGenerator:
    """Generates synthetic log entries at a configurable rate."""

    def __init__(self, rate: int, run_time: int, shutdown_event: threading.Event):
        self._rate = rate  # logs per second
        self._run_time = run_time  # seconds to run
        self._shutdown = shutdown_event

    def generate(self, callback):
        """Generate logs and pass each to callback. Blocks until done or shutdown."""
        interval = 1.0 / self._rate if self._rate > 0 else 1.0
        count = 0
        total = self._rate * self._run_time

        logger.info("Generating %d logs at %d logs/sec for %ds", total, self._rate, self._run_time)

        start = time.monotonic()

        while not self._shutdown.is_set() and count < total:
            entry = generate_log_entry()
            callback(entry)
            count += 1

            # Rate limiting
            elapsed = time.monotonic() - start
            expected = count * interval
            remaining = expected - elapsed
            if remaining > 0:
                self._shutdown.wait(remaining)

        logger.info("Generated %d logs", count)
