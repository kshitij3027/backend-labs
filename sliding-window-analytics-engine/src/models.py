"""Core dataclasses for the sliding-window analytics engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Event:
    """A single metric event flowing through the engine.

    Attributes:
        event_id: Unique identifier for this event.
        timestamp: Event time in unix epoch seconds.
        value: The numeric measurement carried by this event.
        metric: Metric name (e.g. "response_time", "throughput", "error_rate").
        metadata: Free-form key/value metadata (service, region, etc.).
    """

    event_id: str
    timestamp: float
    value: float
    metric: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WindowResult:
    """An immutable snapshot of a sliding window's computed statistics."""

    window_name: str
    resolution: str
    window_start: float
    window_end: float
    count: int
    sum: float
    average: float
    min: float
    max: float
    std_dev: float


@dataclass
class WindowConfig:
    """Configuration for a single sliding window instance."""

    metric: str
    resolution: str
    window_size: float
    slide_interval: float
    max_size: int
