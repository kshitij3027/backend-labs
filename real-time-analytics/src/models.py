"""Pydantic models for the real-time analytics dashboard."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    redis_connected: bool = False


class LogEntry(BaseModel):
    timestamp: float  # unix timestamp
    service: str = "unknown"
    level: str = "INFO"
    message: str = ""
    response_time: Optional[float] = None
    method: Optional[str] = None
    endpoint: Optional[str] = None
    status_code: Optional[int] = None
    error_type: Optional[str] = None
    tags: dict[str, str] = Field(default_factory=dict)


class MetricPoint(BaseModel):
    service: str
    metric_name: str
    value: float
    timestamp: float  # unix timestamp
    tags: dict[str, str] = Field(default_factory=dict)


class AnomalyRecord(BaseModel):
    service: str
    metric_name: str
    value: float
    z_score: float
    threshold: float
    mean: float
    std: float
    timestamp: float
    is_anomaly: bool = True


class MetricResponse(BaseModel):
    service: str
    metric_name: str
    data_points: list[MetricPoint]
    count: int
    trend: Optional[dict] = None


class AnomalyResponse(BaseModel):
    anomalies: list[AnomalyRecord]
    count: int
    hours: float
