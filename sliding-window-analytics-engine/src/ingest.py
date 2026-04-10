"""Request models for the ingest layer.

Commit 4 scope: a single Pydantic v2 request model used by
``POST /api/metric`` to validate user-submitted metric events. Bounded
queue + adaptive sampling backpressure arrive in Commit 6 and will live
alongside this model.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MetricRequest(BaseModel):
    """Validated shape for a metric event posted via HTTP.

    Attributes:
        metric: Metric name (non-empty, capped at 64 chars to avoid abuse).
        value: Numeric measurement.
        metadata: Optional free-form key/value metadata.
    """

    metric: str = Field(..., min_length=1, max_length=64)
    value: float
    metadata: dict[str, Any] = Field(default_factory=dict)
