"""Log entry models for Kafka message serialization."""
import json
import time
import uuid
from enum import Enum
from pydantic import BaseModel, Field


class LogLevel(str, Enum):
    """Log severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LogEntry(BaseModel):
    """Structured log entry for Kafka messages."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = Field(default_factory=time.time)
    level: LogLevel = LogLevel.INFO
    service: str = ""
    message: str = ""
    user_id: str = ""
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    metadata: dict = Field(default_factory=dict)

    def to_kafka_value(self) -> bytes:
        """Serialize to JSON bytes for Kafka producer."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_value(cls, value: bytes) -> "LogEntry":
        """Deserialize from Kafka message bytes."""
        data = json.loads(value.decode("utf-8"))
        return cls(**data)

    def partition_key(self) -> str | None:
        """Return the key used for partition routing (user_id)."""
        return self.user_id if self.user_id else None
