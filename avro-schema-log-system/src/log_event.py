"""LogEvent dataclass representing the v3 superset of all schema versions."""

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


# Fields belonging to each schema version
VERSION_FIELDS = {
    "v1": ["timestamp", "level", "message", "source"],
    "v2": ["timestamp", "level", "message", "source", "trace_id", "span_id"],
    "v3": [
        "timestamp",
        "level",
        "message",
        "source",
        "trace_id",
        "span_id",
        "tags",
        "hostname",
    ],
}

SAMPLE_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
SAMPLE_SOURCES = [
    "auth-service",
    "api-gateway",
    "payment-service",
    "user-service",
    "order-service",
]
SAMPLE_MESSAGES = [
    "Request processed successfully",
    "Connection established to database",
    "Cache miss for key user_session",
    "Retrying failed operation attempt 2/3",
    "Health check passed",
    "Rate limit exceeded for client",
    "Configuration reloaded from disk",
    "Scheduled job completed in 1.23s",
]
SAMPLE_HOSTNAMES = [
    "web-01.us-east-1",
    "web-02.us-east-1",
    "worker-01.eu-west-1",
    "api-03.ap-south-1",
]


@dataclass
class LogEvent:
    """A log event containing the v3 superset of all schema fields."""

    timestamp: str = ""
    level: str = "INFO"
    message: str = ""
    source: str = ""
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    hostname: Optional[str] = None

    def to_dict(self, version: str = "v3") -> dict:
        """Return only the fields relevant to the specified schema version.

        Args:
            version: One of "v1", "v2", or "v3".

        Returns:
            A dict containing only the fields defined in that version.

        Raises:
            ValueError: If the version string is not recognized.
        """
        if version not in VERSION_FIELDS:
            raise ValueError(
                f"Unknown version '{version}'. Must be one of: {list(VERSION_FIELDS.keys())}"
            )

        all_data = {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
            "source": self.source,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "tags": self.tags,
            "hostname": self.hostname,
        }

        return {k: all_data[k] for k in VERSION_FIELDS[version]}

    @staticmethod
    def generate_sample(version: str = "v3") -> "LogEvent":
        """Create a sample LogEvent with realistic data.

        Args:
            version: One of "v1", "v2", or "v3". Fields outside the
                     requested version are left at their defaults.

        Returns:
            A populated LogEvent instance.

        Raises:
            ValueError: If the version string is not recognized.
        """
        if version not in VERSION_FIELDS:
            raise ValueError(
                f"Unknown version '{version}'. Must be one of: {list(VERSION_FIELDS.keys())}"
            )

        event = LogEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            level=random.choice(SAMPLE_LEVELS),
            message=random.choice(SAMPLE_MESSAGES),
            source=random.choice(SAMPLE_SOURCES),
        )

        if version in ("v2", "v3"):
            event.trace_id = str(uuid.uuid4())
            event.span_id = str(uuid.uuid4())[:16]

        if version == "v3":
            event.tags = {
                "env": random.choice(["production", "staging", "development"]),
                "region": random.choice(["us-east-1", "eu-west-1", "ap-south-1"]),
            }
            event.hostname = random.choice(SAMPLE_HOSTNAMES)

        return event
