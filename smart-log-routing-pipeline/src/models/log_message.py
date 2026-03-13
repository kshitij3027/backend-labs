"""Log message model for the smart log routing pipeline."""

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


SERVICES = ["database", "api", "security", "user", "payment"]

COMPONENTS_BY_SERVICE = {
    "database": ["postgres", "mysql", "redis", "mongo"],
    "api": ["gateway", "rest", "graphql", "webhook"],
    "security": ["firewall", "ids", "auth", "scanner"],
    "user": ["auth", "profile", "session", "registration"],
    "payment": ["processor", "validator", "gateway", "ledger"],
}

LEVELS = ["error", "warning", "info", "debug", "critical"]

MESSAGE_TEMPLATES = {
    "error": [
        "Connection refused to backend service",
        "Timeout exceeded while processing request",
        "Failed to write data to disk",
        "Unexpected null reference encountered",
        "Memory allocation failure detected",
    ],
    "warning": [
        "Response time exceeding threshold",
        "Disk usage approaching capacity limit",
        "Connection pool nearing maximum",
        "Certificate expiring within 30 days",
        "Rate limit threshold reached for client",
    ],
    "info": [
        "Service started successfully",
        "Configuration reloaded from file",
        "Health check passed all validations",
        "New connection established from client",
        "Scheduled maintenance task completed",
    ],
    "debug": [
        "Parsed incoming request payload",
        "Cache hit for requested resource key",
        "Query execution plan generated",
        "Token validation middleware invoked",
        "Serializing response for client",
    ],
    "critical": [
        "System out of memory — initiating shutdown",
        "Database cluster lost quorum",
        "Security breach detected — locking accounts",
        "Unrecoverable data corruption found",
        "Service crash loop — manual intervention required",
    ],
}


@dataclass
class LogMessage:
    """Represents a structured log message for routing through RabbitMQ exchanges."""

    timestamp: str
    service: str
    component: str
    level: str
    message: str
    metadata: dict = field(default_factory=dict)

    @property
    def routing_key(self) -> str:
        """Return the hierarchical routing key: service.component.level."""
        return f"{self.service}.{self.component}.{self.level}"

    def to_dict(self) -> dict:
        """Return a dict of all fields including the computed routing_key."""
        return {
            "timestamp": self.timestamp,
            "service": self.service,
            "component": self.component,
            "level": self.level,
            "message": self.message,
            "metadata": self.metadata,
            "routing_key": self.routing_key,
        }

    def to_json(self) -> str:
        """Return a JSON string representation of the log message."""
        return json.dumps(self.to_dict())

    @classmethod
    def generate_random(cls) -> "LogMessage":
        """Create a random LogMessage with plausible field values."""
        service = random.choice(SERVICES)
        component = random.choice(COMPONENTS_BY_SERVICE[service])
        level = random.choice(LEVELS)
        message = random.choice(MESSAGE_TEMPLATES[level])
        timestamp = datetime.now(timezone.utc).isoformat()
        metadata = {
            "source_ip": f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}",
            "request_id": str(uuid.uuid4()),
        }
        return cls(
            timestamp=timestamp,
            service=service,
            component=component,
            level=level,
            message=message,
            metadata=metadata,
        )
