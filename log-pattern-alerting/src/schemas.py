from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class LogEntryCreate(BaseModel):
    message: str
    level: str
    source: Optional[str] = None
    metadata: Optional[dict] = None


class AlertResponse(BaseModel):
    id: int
    pattern_name: str
    severity: str
    message: str
    count: int
    first_occurrence: datetime
    last_occurrence: datetime
    state: str
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class AcknowledgeRequest(BaseModel):
    acknowledged_by: str


class StatsResponse(BaseModel):
    active_alerts: int
    total_patterns: int
    alerts_by_severity: dict


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str
