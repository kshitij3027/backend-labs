"""Pydantic data models for Kafka log compaction state management."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel


class UpdateType(str, Enum):
    """Types of state updates for user profiles."""

    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class UserProfile(BaseModel):
    """A user profile record stored in the compacted Kafka topic."""

    user_id: str
    email: str
    first_name: str
    last_name: str
    age: int
    version: int = 1
    deleted: bool = False
    last_updated: str  # ISO 8601 datetime string

    def to_kafka_value(self) -> bytes:
        """Serialize this profile to JSON bytes for Kafka production."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_value(cls, data: bytes) -> "UserProfile":
        """Deserialize a Kafka message value back into a UserProfile."""
        return cls.model_validate_json(data)


class StateUpdate(BaseModel):
    """An event representing a state change for a user profile."""

    event_id: str = ""
    user_id: str
    update_type: UpdateType
    profile: Optional[UserProfile] = None  # None for DELETE/tombstone
    timestamp: str = ""  # ISO 8601 datetime string

    def __init__(self, **data):
        super().__init__(**data)
        if not self.event_id:
            self.event_id = str(uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_kafka_value(self) -> bytes:
        """Serialize this state update to JSON bytes for Kafka production."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka_value(cls, data: bytes) -> "StateUpdate":
        """Deserialize a Kafka message value back into a StateUpdate."""
        return cls.model_validate_json(data)
