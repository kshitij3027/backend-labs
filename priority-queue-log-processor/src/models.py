"""Core data models for the priority queue log processor."""

import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional


class Priority(IntEnum):
    """Log message priority levels (lower value = higher priority)."""

    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3


# Sentinel value used to mark removed entries in the heap
REMOVED = "<removed>"


@dataclass
class LogMessage:
    """A log message with priority metadata."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    priority: Priority = Priority.LOW
    source: str = ""
    message: str = ""
    original_priority: Optional[Priority] = None

    def __post_init__(self) -> None:
        if self.original_priority is None:
            self.original_priority = self.priority

    def to_dict(self) -> dict:
        """Serialize to a plain dictionary."""
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "created_at": self.created_at,
            "priority": self.priority.name,
            "source": self.source,
            "message": self.message,
            "original_priority": self.original_priority.name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LogMessage":
        """Deserialize from a plain dictionary."""
        return cls(
            id=data["id"],
            timestamp=data["timestamp"],
            created_at=data["created_at"],
            priority=Priority[data["priority"]],
            source=data["source"],
            message=data["message"],
            original_priority=Priority[data["original_priority"]],
        )
