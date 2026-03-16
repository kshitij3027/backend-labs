"""Log entry models and topic routing logic."""

import json
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class LogLevel(str, Enum):
    """Severity levels for log entries."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogEntry(BaseModel):
    """Structured log entry with topic routing and serialization support."""

    timestamp: datetime = Field(default_factory=datetime.utcnow)
    level: LogLevel = LogLevel.INFO
    message: str
    service: str = "unknown"
    component: str = "main"
    trace_id: str = Field(default_factory=lambda: uuid4().hex[:16])
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    metadata: dict = Field(default_factory=dict)

    def route_topic(self) -> str:
        """Determine the Kafka topic based on log level and service name.

        Priority order:
          1. ERROR / CRITICAL -> logs-errors
          2. Database services -> logs-database
          3. Security / auth services -> logs-security
          4. Everything else -> logs-application
        """
        if self.level in (LogLevel.ERROR, LogLevel.CRITICAL):
            return "logs-errors"

        svc = self.service.lower()
        if "db" in svc or "database" in svc:
            return "logs-database"
        if "security" in svc or "auth" in svc:
            return "logs-security"

        return "logs-application"

    def to_kafka_key(self) -> str:
        """Return a partition key: user_id > session_id > service."""
        return self.user_id or self.session_id or self.service

    def to_kafka_value(self) -> str:
        """Serialize the log entry to a JSON string suitable for Kafka."""
        data = self.model_dump()
        data["timestamp"] = self.timestamp.isoformat()
        data["level"] = self.level.value
        return json.dumps(data)

    @classmethod
    def from_kafka_value(cls, data: str) -> "LogEntry":
        """Deserialize a JSON string back into a LogEntry."""
        parsed = json.loads(data)
        return cls(**parsed)


ALL_TOPICS: list[str] = [
    "logs-application",
    "logs-database",
    "logs-errors",
    "logs-security",
]
