"""Pydantic v2 request/response schemas for the HTTP API.

Kept separate from the ORM models in ``src/persistence/models.py`` so
the wire shape can drift from the persistence shape without round-trip
hashing or migration concerns. All models use ``extra='forbid'`` so a
typo in a client payload fails loudly at 422 rather than silently being
ignored.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class LogRecord(BaseModel):
    """One log record accepted by ``POST /v1/logs/ingest``.

    The minimal shape: a timestamp, a level, a source, plus optional
    category (used by the policy matcher's category selector) and an
    optional message body. Records are stored verbatim in the rolling
    JSONL segment — we do not parse or transform them beyond Pydantic
    validation.
    """

    model_config = ConfigDict(extra="forbid")

    ts: datetime
    level: str = Field(min_length=1)
    source: str = Field(min_length=1)
    category: str | None = None
    message: str = ""


class IngestRequest(BaseModel):
    """Batch of records POSTed to ``/v1/logs/ingest``.

    ``min_length=1`` rejects empty payloads (saves a no-op write +
    catalog row). ``max_length=10000`` caps a single request so a
    malformed batch can't OOM the segment writer; clients that need
    larger windows should issue multiple requests.
    """

    records: list[LogRecord] = Field(min_length=1, max_length=10000)


class IngestResponse(BaseModel):
    """Response shape for ``POST /v1/logs/ingest``.

    ``accepted`` echoes the number of records persisted in this call
    (always equals the request length on the happy path). ``segment_path``
    points at the JSONL segment the bytes were appended to — useful for
    debugging when correlating an ingest call with a later catalog row.
    """

    accepted: int
    segment_path: str


class FileSummary(BaseModel):
    """Public projection of an ORM ``File`` row for ``GET /v1/files``.

    The ORM model has a few additional columns (``compliance_tag``,
    ``immutable``) that are intentionally omitted here because they are
    derived from policy at scan time and the dashboard renders them
    separately. Keep this list focused on what the operator needs to
    answer "where is this segment, how big is it, when does it move
    next?" at a glance.
    """

    id: int
    source: str
    segment_path: str
    tier: str
    size_bytes: int
    oldest_record_ts: datetime
    newest_record_ts: datetime
    next_eval_at: datetime | None = None
    created_at: datetime


class FilesListResponse(BaseModel):
    """Paginated response for ``GET /v1/files``."""

    files: list[FileSummary]
    total: int


class EvaluateResponse(BaseModel):
    """Response shape for ``POST /v1/evaluate`` — one synchronous cycle.

    Counts mirror the fields of ``ScanReport`` / ``ApplyReport`` /
    ``SweepReport`` so the operator can correlate a manual evaluate
    call with the periodic background runs visible in the JobRun table.
    ``eval_seconds`` is wall-clock for the whole scan+apply+sweep window.
    """

    scanned: int
    transitions_planned: int
    applied: int
    failed: int
    swept: int
    eval_seconds: float


class HealthResponse(BaseModel):
    """Response shape for ``GET /api/health`` — liveness only.

    ``timestamp`` is a Unix epoch ``int`` (not a datetime / ISO string)
    so it survives JSON round-trip without timezone ambiguity and is
    cheap to compare in tests.
    """

    status: str
    timestamp: int
