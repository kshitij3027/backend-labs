"""Core data models for the log format compatibility layer."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Optional


class SeverityLevel(IntEnum):
    """Syslog severity levels (RFC 5424)."""
    EMERGENCY = 0
    ALERT = 1
    CRITICAL = 2
    ERROR = 3
    WARNING = 4
    NOTICE = 5
    INFORMATIONAL = 6
    DEBUG = 7

    @classmethod
    def from_syslog_severity(cls, severity: int) -> "SeverityLevel":
        """Convert a syslog severity integer to a SeverityLevel.

        Args:
            severity: Integer severity value (0-7).

        Returns:
            Corresponding SeverityLevel, or DEBUG if out of range.
        """
        try:
            return cls(severity)
        except ValueError:
            return cls.DEBUG

    @classmethod
    def from_string(cls, s: str) -> "SeverityLevel":
        """Convert a string severity name to a SeverityLevel.

        Supports common aliases like 'warn', 'err', 'info', 'crit', 'fatal'.

        Args:
            s: String severity name (case-insensitive).

        Returns:
            Corresponding SeverityLevel, or DEBUG if unrecognized.
        """
        alias_map = {
            "warn": cls.WARNING,
            "err": cls.ERROR,
            "info": cls.INFORMATIONAL,
            "crit": cls.CRITICAL,
            "emerg": cls.EMERGENCY,
            "notice": cls.NOTICE,
            "debug": cls.DEBUG,
            "alert": cls.ALERT,
            "fatal": cls.EMERGENCY,
            "error": cls.ERROR,
            "warning": cls.WARNING,
            "information": cls.INFORMATIONAL,
            "informational": cls.INFORMATIONAL,
        }

        normalized = s.strip().lower()

        # Check alias map first
        if normalized in alias_map:
            return alias_map[normalized]

        # Try direct enum name match
        try:
            return cls[normalized.upper()]
        except KeyError:
            return cls.DEBUG


@dataclass
class ParsedLog:
    """Represents a parsed log entry in a normalized format."""
    timestamp: Optional[datetime] = None
    level: Optional[SeverityLevel] = None
    message: str = ""
    source_format: str = ""
    facility: Optional[str] = None
    hostname: Optional[str] = None
    priority: Optional[int] = None
    app_name: Optional[str] = None
    pid: Optional[int] = None
    metadata: dict = field(default_factory=dict)
    raw: str = ""
    confidence: float = 0.0

    def to_dict(self) -> dict:
        """Serialize the parsed log to a dictionary.

        Returns:
            Dictionary representation with timestamp as ISO format
            and level as its name string.
        """
        return {
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "level": self.level.name if self.level else None,
            "message": self.message,
            "source_format": self.source_format,
            "facility": self.facility,
            "hostname": self.hostname,
            "priority": self.priority,
            "app_name": self.app_name,
            "pid": self.pid,
            "metadata": self.metadata,
            "raw": self.raw,
            "confidence": self.confidence,
        }
