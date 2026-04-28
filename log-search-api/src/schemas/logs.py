from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


LogLevel = Literal["DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"]


class LogEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    timestamp: datetime
    level: LogLevel
    service_name: str
    message: str
    content: dict[str, Any] | None = None
    score: float | None = None


class LogIngestRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    timestamp: datetime
    level: LogLevel
    service_name: str
    message: str
    content: dict[str, Any] | None = None


class IngestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    result: Literal["created", "updated"]
    index: str


class BulkIngestRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    entries: list[LogIngestRequest] = Field(default_factory=list)

    @field_validator("entries")
    @classmethod
    def _cap_entries(cls, value: list[LogIngestRequest]) -> list[LogIngestRequest]:
        if len(value) > 1000:
            raise ValueError("entries length must be <= 1000")
        return value


class BulkIngestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    total: int
    created: int
    errors: int
    error_items: list[dict[str, Any]] = Field(default_factory=list)
