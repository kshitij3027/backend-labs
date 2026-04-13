"""Data models for the anomaly detection engine."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class LogEntry:
    """A single parsed log entry representing an HTTP request."""

    timestamp: datetime
    ip: str
    method: str
    path: str
    status_code: int
    response_time: float
    bytes_sent: int
    user_agent: str
    session_duration: float
    page_views: int
    _is_anomaly: bool = False
    _anomaly_type: str = ""

    def __post_init__(self) -> None:
        if self.response_time < 0:
            raise ValueError(f"response_time must be >= 0, got {self.response_time}")
        if self.bytes_sent < 0:
            raise ValueError(f"bytes_sent must be >= 0, got {self.bytes_sent}")
        if self.status_code < 100 or self.status_code > 599:
            raise ValueError(
                f"status_code must be between 100 and 599, got {self.status_code}"
            )


@dataclass
class DetectionResult:
    """Result from a single anomaly detector."""

    score: float
    name: str
    details: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.score = max(0.0, min(1.0, self.score))


@dataclass
class AnomalyResult:
    """Combined result from all detectors for a single log entry."""

    is_anomaly: bool
    confidence: float
    scores: dict
    log_entry: LogEntry
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        self.confidence = max(0.0, min(1.0, self.confidence))
