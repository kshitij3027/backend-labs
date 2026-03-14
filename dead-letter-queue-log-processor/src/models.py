"""Data models for the Dead Letter Queue Log Processor."""

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class LogLevel(Enum):
    """Severity levels for log messages."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class FailureType(Enum):
    """Categories of processing failures."""

    PARSING = "PARSING"
    NETWORK = "NETWORK"
    RESOURCE = "RESOURCE"
    UNKNOWN = "UNKNOWN"


@dataclass
class LogMessage:
    """A single log entry flowing through the processing pipeline."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    level: LogLevel = LogLevel.INFO
    source: str = ""
    message: str = ""
    metadata: dict = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize to a JSON string."""
        return json.dumps(
            {
                "id": self.id,
                "timestamp": self.timestamp,
                "level": self.level.value,
                "source": self.source,
                "message": self.message,
                "metadata": self.metadata,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "LogMessage":
        """Deserialize from a JSON string."""
        obj = json.loads(data)
        return cls(
            id=obj["id"],
            timestamp=obj["timestamp"],
            level=LogLevel(obj["level"]),
            source=obj["source"],
            message=obj["message"],
            metadata=obj.get("metadata", {}),
        )


@dataclass
class FailedMessage:
    """A message that failed processing, enriched with failure context."""

    original_message: LogMessage = field(default_factory=LogMessage)
    failure_type: FailureType = FailureType.UNKNOWN
    error_details: str = ""
    retry_count: int = 0
    max_retries: int = 3
    first_failure: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    last_failure: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_json(self) -> str:
        """Serialize to a JSON string."""
        return json.dumps(
            {
                "original_message": json.loads(self.original_message.to_json()),
                "failure_type": self.failure_type.value,
                "error_details": self.error_details,
                "retry_count": self.retry_count,
                "max_retries": self.max_retries,
                "first_failure": self.first_failure,
                "last_failure": self.last_failure,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "FailedMessage":
        """Deserialize from a JSON string."""
        obj = json.loads(data)
        original = obj["original_message"]
        return cls(
            original_message=LogMessage(
                id=original["id"],
                timestamp=original["timestamp"],
                level=LogLevel(original["level"]),
                source=original["source"],
                message=original["message"],
                metadata=original.get("metadata", {}),
            ),
            failure_type=FailureType(obj["failure_type"]),
            error_details=obj["error_details"],
            retry_count=obj["retry_count"],
            max_retries=obj["max_retries"],
            first_failure=obj["first_failure"],
            last_failure=obj["last_failure"],
        )
