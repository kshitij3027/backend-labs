"""Pydantic models for log events, windows, and API requests/responses."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class LogEvent(BaseModel):
    """A single log event to ingest."""

    timestamp: str
    level: str
    source: str
    message: str
    metadata: dict | None = None
    response_time: float | None = None


class WindowState(str, Enum):
    """Lifecycle state of a time window."""

    ACTIVE = "active"
    GRACE = "grace"
    CLOSED = "closed"


class WindowMetrics(BaseModel):
    """Aggregated metrics for a single window."""

    count: int
    error_count: int
    error_rate: float
    avg_response_time: float | None
    throughput: float
    levels: dict
    services: dict


class WindowInfo(BaseModel):
    """Full information about a time window."""

    window_key: str
    window_type: str
    start_ts: int
    end_ts: int
    state: WindowState
    metrics: WindowMetrics | None = None


class IngestResponse(BaseModel):
    """Response for a single-event ingest."""

    accepted: int
    rejected: int
    late_accepted: int
    errors: list[str]


class BatchIngestRequest(BaseModel):
    """Request body for batch ingestion."""

    events: list[LogEvent]


class BatchIngestResponse(BaseModel):
    """Response for batch ingestion."""

    total: int
    accepted: int
    rejected: int
    late_accepted: int
    errors: list[str]
