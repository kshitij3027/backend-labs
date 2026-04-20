"""Pydantic models for the real-time log indexing engine.

Keeps the wire-level shapes we accept/emit — and the internal log
entry representation — in one place so the API layer, the indexer,
the stream consumer, and the WebSocket broadcaster all agree on
types. Timestamps are floats (unix-epoch seconds) so sub-second
precision flows through to the dashboard.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Core log entry
# ---------------------------------------------------------------------------

# Accept both WARN and WARNING, and FATAL/CRITICAL, so producers that
# use either dialect don't bounce off the validator.
LogLevel = Literal[
    "DEBUG",
    "INFO",
    "WARN",
    "WARNING",
    "ERROR",
    "FATAL",
    "CRITICAL",
]


class LogEntry(BaseModel):
    """A single log row as it flows through ingest -> index -> search.

    ``doc_id`` is assigned by the indexer when the entry is admitted
    into the current segment; producers do not supply it. ``timestamp``
    is unix-epoch seconds (float). ``stream_id`` captures the Redis
    stream message ID when the entry arrived via the stream consumer
    so we can XACK after indexing.
    """

    doc_id: int
    message: str
    timestamp: float
    service: str = "unknown"
    level: LogLevel = "INFO"
    stream_id: str | None = None


# ---------------------------------------------------------------------------
# Search API
# ---------------------------------------------------------------------------

class SearchResult(BaseModel):
    """One row in a search response.

    ``highlighted_message`` is the full ``message`` with matched terms
    wrapped in ``<mark>`` tags so the dashboard can render the hit
    inline without a second client-side scan. ``score`` defaults to
    zero for simple term-set matches; ranked queries will fill it in.
    """

    doc_id: int
    message: str
    highlighted_message: str
    timestamp: float
    service: str
    level: str
    score: float = 0.0


class SearchRequest(BaseModel):
    """Query parameters for ``GET /api/search``.

    ``q`` is required and must be non-empty. ``limit`` is clamped to
    [1, 500] so a malformed client can't drain the server. ``service``
    and ``level`` are post-filter narrowers applied after term lookup.
    """

    q: str = Field(min_length=1)
    service: str | None = None
    level: str | None = None
    limit: int = Field(default=50, ge=1, le=500)


class SearchResponse(BaseModel):
    """Full response body for ``/api/search``."""

    results: list[SearchResult]
    total: int
    took_ms: float
    query: str
    terms: list[str]


# ---------------------------------------------------------------------------
# Stats / diagnostics
# ---------------------------------------------------------------------------

class StatsResponse(BaseModel):
    """Response body for ``GET /api/stats``.

    Surfaces every counter the dashboard plots: ingestion volume,
    segment counts across tiers, vocabulary size, approximate memory
    footprint, and recent throughput/latency summaries. ``uptime_s``
    is measured from the FastAPI lifespan start.
    """

    docs_indexed: int
    current_segment_docs: int
    flushed_memory_segments: int
    disk_segments: int
    vocab_size: int
    memory_bytes: int
    throughput_1m: float
    ingest_lag: int
    query_p95_ms: float
    errors: int
    uptime_s: float


# ---------------------------------------------------------------------------
# Sample generation
# ---------------------------------------------------------------------------

class GenerateSampleRequest(BaseModel):
    """Request body for ``POST /api/generate-sample``.

    ``count`` is bounded so accidental requests with huge values don't
    swamp Redis. ``rate`` is optional — when None the generator emits
    as fast as it can; when set it throttles to that logs/sec rate.
    """

    count: int = Field(default=500, ge=1, le=100_000)
    rate: float | None = Field(default=None, gt=0)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    """Response body for ``GET /health``.

    ``status`` follows a 3-state model so orchestrators (compose,
    Kubernetes) can treat ``degraded`` as warn-but-live and ``down``
    as fail.
    """

    status: Literal["ok", "degraded", "down"]
    redis_connected: bool
    segments_ready: bool
    uptime_s: float
