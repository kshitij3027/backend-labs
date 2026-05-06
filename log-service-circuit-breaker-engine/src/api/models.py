"""Pydantic request/response models."""
from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    uptime_seconds: float


class ProcessLogsRequest(BaseModel):
    count: int = Field(10, ge=1, le=10000)


class SimulateFailuresRequest(BaseModel):
    target: str = Field("database_primary")
    duration: int = Field(30, ge=1, le=600)
    failure_rate: float = Field(0.8, ge=0.0, le=1.0)


class CircuitMetric(BaseModel):
    name: str
    state: str
    success_rate: float
    total_calls: int
    successful_calls: int
    failed_calls: int


class MetricsResponse(BaseModel):
    circuits: dict
    processing: dict
    generated_at: float
