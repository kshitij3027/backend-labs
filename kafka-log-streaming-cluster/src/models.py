"""Pydantic data models for Kafka log messages."""

from enum import Enum

from pydantic import BaseModel


class LogLevel(str, Enum):
    """Log severity levels."""

    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class ServiceName(str, Enum):
    """Microservice identifiers that produce logs."""

    WEB_API = "web-api"
    USER_SERVICE = "user-service"
    PAYMENT_SERVICE = "payment-service"


TOPIC_MAP: dict[ServiceName, str] = {
    ServiceName.WEB_API: "web-api-logs",
    ServiceName.USER_SERVICE: "user-service-logs",
    ServiceName.PAYMENT_SERVICE: "payment-service-logs",
}


class LogMessage(BaseModel):
    """A structured log entry destined for Kafka."""

    timestamp: str  # ISO 8601 UTC
    service: ServiceName
    level: LogLevel
    endpoint: str
    status_code: int
    user_id: str
    message: str = ""
    sequence_number: int = 0

    def to_kafka_value(self) -> bytes:
        """Serialize this message to JSON bytes for Kafka production."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_value(cls, value: bytes) -> "LogMessage":
        """Deserialize a Kafka message value back into a LogMessage."""
        return cls.model_validate_json(value)

    @property
    def topic(self) -> str:
        """Return the Kafka topic this message should be published to."""
        return TOPIC_MAP[self.service]

    @property
    def partition_key(self) -> bytes:
        """Return the partition key (user_id) as bytes for consistent hashing."""
        return self.user_id.encode("utf-8")
