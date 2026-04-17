from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TimeRange(BaseModel):
    model_config = ConfigDict(extra="ignore")

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def _check_order(self) -> "TimeRange":
        if self.end <= self.start:
            raise ValueError("end must be strictly greater than start")
        return self


class PartitionMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    url: str
    time_range: TimeRange
    indexed_fields: list[str] = []
    healthy: bool = True


class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query: str = Field(..., min_length=1)


class ExecutionStep(BaseModel):
    model_config = ConfigDict(extra="ignore")

    op: str
    partition_id: str | None = None
    filter: dict | None = None
    aggregation: dict | None = None
    group_by: list[str] = []
    estimated_cost: float = 0.0


class ExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="ignore")

    steps: list[ExecutionStep]
    total_cost: float = 0.0
    parallelism: int = 1
    optimization_notes: list[str] = []


class QueryResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query_id: str
    results: list[dict]
    records_processed: int
    execution_time_ms: float
    optimizations_applied: list[str]
    plan: ExecutionPlan
    partial_results: bool = False
    failed_partitions: list[str] = []


class PartitionExecuteRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    filter_ast_json: dict | None = None
    aggregation: dict | None = None
    group_by: list[str] = []
    limit: int | None = None
    select_fields: list[str] | None = None


class PartitionExecuteResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    rows: list[dict] = []
    partial_aggregate: dict | None = None
    records_scanned: int = 0


class ProgressEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    stage: str
    payload: dict = Field(default_factory=dict)


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: str
    partitions: list[dict] = []
