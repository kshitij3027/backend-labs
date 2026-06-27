"""Pydantic v2 request/response contracts for the Predictive Log Analytics Engine.

These models are the public shape of the HTTP API. They are intentionally small
and reusable: later commits (forecast / predictions / metrics endpoints) build on
the same :class:`MetricPoint` primitive used here for ingestion and read-back.

Conventions
-----------
* All timestamps are timezone-aware ``datetime`` objects. On input a naive
  datetime is assumed to be UTC and coerced; on output we always carry tzinfo.
* Metric values must be finite real numbers (NaN / +-inf are rejected at the
  schema boundary so they can never reach the database).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _ensure_utc(ts: datetime) -> datetime:
    """Return ``ts`` as a timezone-aware UTC datetime (naive is assumed UTC)."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class MetricPoint(BaseModel):
    """A single metric observation: ``(metric_name, timestamp, value)``.

    Used both as a stored/returned point and as the canonical shape consumed by
    the ingestion path. ``timestamp`` is required here; the more lenient
    :class:`MetricIngest` allows it to be omitted on input.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_name: str = Field(..., min_length=1, max_length=64)
    timestamp: datetime
    value: float

    @field_validator("metric_name")
    @classmethod
    def _name_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("metric_name must not be empty")
        return v

    @field_validator("value")
    @classmethod
    def _value_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("value must be a finite number (no NaN/inf)")
        return v

    @field_validator("timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        return _ensure_utc(v)


class MetricIngest(BaseModel):
    """An ingestable point where ``timestamp`` is optional.

    When ``timestamp`` is omitted the ingestion layer defaults it to ``now`` in
    UTC. Validation of name/value mirrors :class:`MetricPoint`.
    """

    model_config = ConfigDict(from_attributes=True)

    metric_name: str = Field(..., min_length=1, max_length=64)
    timestamp: datetime | None = None
    value: float

    @field_validator("metric_name")
    @classmethod
    def _name_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("metric_name must not be empty")
        return v

    @field_validator("value")
    @classmethod
    def _value_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("value must be a finite number (no NaN/inf)")
        return v

    @field_validator("timestamp")
    @classmethod
    def _tz_aware(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _ensure_utc(v)


class MetricIngestRequest(BaseModel):
    """Request body for ``POST /metrics``.

    The canonical shape is ``{"points": [<MetricIngest>, ...]}``. At least one
    point is required.
    """

    points: list[MetricIngest] = Field(..., min_length=1)


class MetricIngestResponse(BaseModel):
    """Response for a successful ingestion."""

    ingested: int
    metric_names: list[str]


class MetricQueryResponse(BaseModel):
    """Response for ``GET /metrics/{metric_name}`` — recent stored points."""

    metric_name: str
    count: int
    points: list[MetricPoint]
