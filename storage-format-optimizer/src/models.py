"""Core domain types for the adaptive storage-format optimizer.

Defines the shared enums (storage format, query class, tier) and the
request/response Pydantic models used across ingest, query, and the storage
engines. Kept import-light on purpose — only stdlib ``enum``/``typing`` and
Pydantic v2.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class Format(str, Enum):
    """On-disk storage format for a partition."""

    ROW = "row"
    COLUMNAR = "columnar"
    HYBRID = "hybrid"


class QueryClass(str, Enum):
    """Access pattern a query exhibits, used to guide format selection."""

    ANALYTICAL = "analytical"
    FULL_RECORD = "full_record"
    MIXED = "mixed"


class Tier(str, Enum):
    """Storage tier for a partition, by recency and access frequency."""

    HOT = "hot"
    WARM = "warm"
    COLD = "cold"


class LogEntry(BaseModel):
    """A single stored log record: optional timestamp + arbitrary fields."""

    ts: float | None = None
    fields: dict[str, Any] = Field(default_factory=dict)


class IngestEntry(BaseModel):
    """A single entry in an ingest request; ``fields`` is required."""

    ts: float | None = None
    fields: dict[str, Any]


class IngestRequest(BaseModel):
    """Batch ingest payload for one tenant."""

    tenant: str = "default"
    entries: list[IngestEntry] = Field(min_length=1)


class Filter(BaseModel):
    """A single predicate applied to a column during a query."""

    column: str
    op: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in"]
    value: Any


class Aggregation(BaseModel):
    """An aggregation to compute; ``column`` is optional for ``count``."""

    op: Literal["count", "sum", "avg", "min", "max"]
    column: str | None = None


class QueryRequest(BaseModel):
    """A query against one tenant's stored log data."""

    tenant: str = "default"
    columns: list[str] | None = None
    filters: list[Filter] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    limit: int | None = None
