from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.schemas.logs import LogEntry


class Pagination(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    offset: int
    limit: int
    has_more: bool


class LevelBucket(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    key: str
    doc_count: int


class ServiceBucket(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    key: str
    doc_count: int


class TimelineBucket(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    key_as_string: str
    doc_count: int


class Aggregations(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    levels: list[LevelBucket]
    services: list[ServiceBucket]
    timeline: list[TimelineBucket]


class SortBy(str, Enum):
    RELEVANCE = "relevance"
    TIMESTAMP = "timestamp"


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    q: str | None = Field(default=None, max_length=512)
    start_time: datetime | None = None
    end_time: datetime | None = None
    levels: list[str] | None = None
    services: list[str] | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    include_content: bool = True
    sort_by: SortBy = SortBy.RELEVANCE
    sort_order: SortOrder = SortOrder.DESC

    @field_validator("levels")
    @classmethod
    def _normalize_levels(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        return [s.upper() for s in v]

    @model_validator(mode="after")
    def _check_time_range(self) -> "SearchRequest":
        if self.start_time and self.end_time and self.start_time > self.end_time:
            raise ValueError("start_time must be <= end_time")
        return self


class SearchResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    query: str | None
    total_hits: int
    execution_time_ms: float
    cache_hit: bool = False
    results: list[LogEntry]
    pagination: Pagination
    aggregations: Aggregations
