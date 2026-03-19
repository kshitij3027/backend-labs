"""Generates realistic structured log entries."""
import random
import time
from src.config import Settings
from src.models import LogEntry, LogLevel


class LogGenerator:
    """Generates log entries with weighted severity distribution."""

    # Common log message templates per level
    _messages = {
        LogLevel.INFO: [
            "Request processed successfully",
            "User session started",
            "Cache hit for key",
            "Health check passed",
            "Configuration reloaded",
            "Connection pool refreshed",
            "Metrics exported successfully",
            "Background job completed",
        ],
        LogLevel.WARNING: [
            "High memory usage detected",
            "Slow query execution",
            "Rate limit approaching threshold",
            "Connection pool near capacity",
            "Retry attempt for failed operation",
            "Deprecated API endpoint called",
        ],
        LogLevel.ERROR: [
            "Database connection timeout",
            "Authentication token expired",
            "Payment processing failed",
            "External API returned 500",
            "Disk space critically low",
        ],
    }

    def __init__(self, settings: Settings) -> None:
        self._services = settings.services
        self._user_id_min = settings.user_id_min
        self._user_id_max = settings.user_id_max
        self._levels = list(settings.log_level_weights.keys())
        self._weights = list(settings.log_level_weights.values())

    def generate_one(self) -> LogEntry:
        """Generate a single random log entry."""
        level_str = random.choices(self._levels, weights=self._weights, k=1)[0]
        level = LogLevel(level_str)

        return LogEntry(
            timestamp=time.time(),
            level=level,
            service=random.choice(self._services),
            message=random.choice(self._messages[level]),
            user_id=str(random.randint(self._user_id_min, self._user_id_max)),
        )

    def generate_batch(self, count: int) -> list[LogEntry]:
        """Generate a batch of log entries."""
        return [self.generate_one() for _ in range(count)]
