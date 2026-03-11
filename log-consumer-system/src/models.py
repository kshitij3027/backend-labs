"""Pydantic models for the log consumer system."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class LogEntry(BaseModel):
    """Parsed access log entry."""
    ip: str
    method: str
    path: str
    status_code: int
    response_size: int
    response_time_ms: float | None = None
    timestamp: datetime | None = None
    raw: str


class ConsumerStats(BaseModel):
    """Per-consumer statistics."""
    consumer_id: str
    processed_count: int = 0
    error_count: int = 0
    success_rate: float = 1.0
    last_active: datetime | None = None


class EndpointMetrics(BaseModel):
    """Per-endpoint aggregated metrics."""
    path: str
    request_count: int = 0
    avg_response_time: float = 0.0
    error_rate: float = 0.0
    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0


class DashboardStats(BaseModel):
    """Complete dashboard statistics snapshot."""
    total_processed: int = 0
    total_errors: int = 0
    requests_per_second: float = 0.0
    consumers: list[ConsumerStats] = Field(default_factory=list)
    endpoints: dict[str, EndpointMetrics] = Field(default_factory=dict)
    status_code_distribution: dict[str, int] = Field(default_factory=dict)
    top_paths: list[dict] = Field(default_factory=list)
    top_ips: list[dict] = Field(default_factory=list)
    latency_percentiles: dict[str, float] = Field(default_factory=dict)
    uptime_seconds: float = 0.0
