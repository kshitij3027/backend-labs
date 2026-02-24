"""Core data models for the Universal Log Translator."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class LogLevel(Enum):
    """Standard log severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_string(cls, value: str) -> "LogLevel":
        """Convert string to LogLevel, case-insensitive. Returns UNKNOWN for unrecognized values."""
        normalized = value.strip().upper()
        # Handle common aliases
        aliases = {
            "WARN": "WARNING",
            "FATAL": "CRITICAL",
            "CRIT": "CRITICAL",
            "ERR": "ERROR",
            "DBG": "DEBUG",
            "INF": "INFO",
        }
        normalized = aliases.get(normalized, normalized)
        try:
            return cls(normalized)
        except ValueError:
            return cls.UNKNOWN


class UnsupportedFormatError(Exception):
    """Raised when no handler can parse the given log data."""
    pass


@dataclass
class LogEntry:
    """Standardized log entry - the universal output format."""
    timestamp: datetime
    level: LogLevel
    message: str
    source: str = ""
    hostname: str = ""
    service: str = ""
    metadata: dict = field(default_factory=dict)
    raw: bytes = b""
    source_format: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "source": self.source,
            "hostname": self.hostname,
            "service": self.service,
            "metadata": self.metadata,
            "source_format": self.source_format,
        }
