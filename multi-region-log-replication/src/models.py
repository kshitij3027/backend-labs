"""Pydantic v2 data models shared across the project.

These models are wire-format only — domain logic (vector clock arithmetic,
conflict resolution, replication) lives in dedicated modules and operates on
plain dicts where appropriate.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# ``VectorClock`` is just a dict of region_id → logical-time-int. We keep it
# as a type alias so call sites read clearly without a wrapping model.
VectorClock = Dict[str, int]


class LogEntry(BaseModel):
    """A single entry stored by a region's log_store.

    ``vector_clock`` and ``logical_ts`` are populated by the writer region at
    ``local_write`` time and copied verbatim during replication. Conflict
    resolution at secondary regions reads ``vector_clock`` to determine
    causal ordering.
    """

    log_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    data: Dict[str, Any]
    region: str
    created_at: float = Field(default_factory=time.time)
    vector_clock: VectorClock = Field(default_factory=dict)
    logical_ts: int = 0


class LogWriteRequest(BaseModel):
    """Body of ``POST /api/logs``.

    Per spec §8 the user supplies ``message``, ``level``, ``service``. All
    three are required by the spec; we keep them required here so a malformed
    request is rejected with a 422 from FastAPI.
    """

    message: str
    level: str
    service: str


class RegionStatus(BaseModel):
    """Per-region snapshot returned in the dashboard / health payload."""

    region_id: str
    is_primary: bool
    is_healthy: bool
    log_count: int
    vector_clock: VectorClock = Field(default_factory=dict)
    logical_ts: int = 0
    replication_lag_ms: Optional[float] = None
    replication_success_rate: Optional[float] = None


class HealthSnapshot(BaseModel):
    """Top-level snapshot pushed over the WebSocket and returned by /api/health.

    ``recent_failovers`` is a bounded list of recent failover events (most
    recent ten), each a dict with ``at`` (epoch seconds), ``old_primary``,
    ``new_primary``, and ``elapsed_ms``. Surfaced on the snapshot so the
    dashboard can render a "time since last failover" pill without an
    extra round-trip.

    ``total_writes`` is the count of distinct writes accepted by the
    cluster — sourced from the **current primary's** ``log_count`` (not
    summed across regions, which would triple-count every replicated
    entry). When no primary is elected (e.g. all regions unhealthy) it
    falls back to 0. The dashboard's "Total writes" tile reads this
    field directly so it never needs to do client-side division by the
    region count.
    """

    overall_status: str
    regions: List[RegionStatus] = Field(default_factory=list)
    taken_at: float = Field(default_factory=time.time)
    current_primary: Optional[str] = None
    recent_failovers: List[Dict[str, Any]] = Field(default_factory=list)
    total_writes: int = 0
