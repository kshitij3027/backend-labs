import uuid
from datetime import datetime
from pydantic import BaseModel, Field


class TimeRange(BaseModel):
    start: datetime
    end: datetime


class QueryFilter(BaseModel):
    field: str
    operator: str = "eq"  # "eq" or "contains"
    value: str


class Query(BaseModel):
    query_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    time_range: TimeRange | None = None
    filters: list[QueryFilter] = Field(default_factory=list)
    sort_field: str = "timestamp"
    sort_order: str = "desc"  # "asc" or "desc"
    limit: int | None = None


class LogEntry(BaseModel):
    timestamp: datetime
    level: str
    service: str
    message: str
    partition_id: str = ""


class PartitionInfo(BaseModel):
    partition_id: str
    url: str
    healthy: bool = True
    time_range: TimeRange | None = None
    log_count: int = 0


class QueryResponse(BaseModel):
    query_id: str
    total_results: int
    partitions_queried: int
    partitions_successful: int
    total_execution_time_ms: float
    results: list[LogEntry]
    cached: bool = False
