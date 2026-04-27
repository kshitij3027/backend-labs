from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: str
    timestamp: datetime


class DetailedHealthResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: str
    timestamp: datetime
    dependencies: dict[str, str] = Field(default_factory=dict)


class ErrorBody(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    code: str
    message: str
    suggestions: list[str] = Field(default_factory=list)
    details: list[dict[str, Any]] | None = None


class ErrorEnvelope(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    error: ErrorBody
    request_id: str
