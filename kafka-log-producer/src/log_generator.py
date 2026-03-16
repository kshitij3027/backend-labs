"""Realistic sample log generator for testing and demos."""

import random
from typing import Optional

from src.models import LogEntry, LogLevel


class LogGenerator:
    """Generate realistic log entries with weighted severity distribution."""

    SERVICES: list[str] = [
        "api-gateway",
        "auth-service",
        "user-service",
        "payment-service",
        "database-proxy",
        "security-scanner",
        "notification-service",
        "cache-service",
    ]

    COMPONENTS: list[str] = [
        "handler",
        "middleware",
        "controller",
        "repository",
        "service",
        "worker",
    ]

    MESSAGE_TEMPLATES: dict[LogLevel, list[str]] = {
        LogLevel.DEBUG: [
            "Cache lookup for key={key} completed in 2ms",
            "Entering request handler for /api/v1/resource",
            "Database connection pool stats: active=3, idle=7",
            "Parsed request body: content_length=1024 bytes",
        ],
        LogLevel.INFO: [
            "Request processed successfully in 45ms",
            "User login successful for user_id={user_id}",
            "Batch job completed: 150 records processed",
            "Health check passed: all dependencies healthy",
            "Configuration reloaded from config server",
        ],
        LogLevel.WARNING: [
            "Response time exceeded threshold: 2500ms > 2000ms",
            "Retry attempt 2/3 for upstream service call",
            "Connection pool nearing capacity: 85% utilized",
            "Deprecated API endpoint called: /api/v1/legacy",
            "Rate limit approaching: 450/500 requests this minute",
        ],
        LogLevel.ERROR: [
            "Failed to connect to upstream service: connection refused",
            "Database query timeout after 30s: SELECT * FROM orders",
            "Authentication token validation failed: token expired",
            "Message processing failed: malformed payload",
            "Disk space critically low: 95% utilized on /data",
        ],
        LogLevel.CRITICAL: [
            "Database cluster unreachable: all replicas down",
            "Out of memory: heap allocation failed",
            "TLS certificate expired: secure connections unavailable",
            "Kafka broker connection lost: message delivery halted",
        ],
    }

    ERROR_SERVICES: list[str] = [
        "database-proxy",
        "payment-service",
        "auth-service",
    ]

    # Weighted distribution: INFO 50%, WARNING 20%, DEBUG 15%, ERROR 12%, CRITICAL 3%
    _LEVEL_WEIGHTS: list[tuple[LogLevel, int]] = [
        (LogLevel.INFO, 50),
        (LogLevel.WARNING, 20),
        (LogLevel.DEBUG, 15),
        (LogLevel.ERROR, 12),
        (LogLevel.CRITICAL, 3),
    ]

    def _random_level(self) -> LogLevel:
        """Pick a log level using the weighted distribution."""
        levels, weights = zip(*self._LEVEL_WEIGHTS)
        return random.choices(levels, weights=weights, k=1)[0]

    def generate_one(
        self,
        level: Optional[LogLevel] = None,
        service: Optional[str] = None,
    ) -> LogEntry:
        """Generate a single realistic log entry.

        Args:
            level: Force a specific log level. Random weighted if *None*.
            service: Force a specific service name. Random if *None*.
        """
        chosen_level = level or self._random_level()
        chosen_service = service or random.choice(self.SERVICES)
        chosen_component = random.choice(self.COMPONENTS)
        chosen_message = random.choice(self.MESSAGE_TEMPLATES[chosen_level])

        user_id: Optional[str] = None
        session_id: Optional[str] = None

        if random.random() < 0.30:
            user_id = f"user-{random.randint(1000, 9999)}"
        if random.random() < 0.40:
            session_id = f"sess-{random.randint(100000, 999999)}"

        return LogEntry(
            level=chosen_level,
            message=chosen_message,
            service=chosen_service,
            component=chosen_component,
            user_id=user_id,
            session_id=session_id,
        )

    def generate_batch(self, count: int = 10) -> list[LogEntry]:
        """Generate a batch of random log entries."""
        return [self.generate_one() for _ in range(count)]

    def generate_error_burst(self, count: int = 5) -> list[LogEntry]:
        """Generate a burst of ERROR/CRITICAL entries from error-prone services."""
        entries: list[LogEntry] = []
        for _ in range(count):
            lvl = random.choice([LogLevel.ERROR, LogLevel.CRITICAL])
            svc = random.choice(self.ERROR_SERVICES)
            entries.append(self.generate_one(level=lvl, service=svc))
        return entries
