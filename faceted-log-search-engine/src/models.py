"""Pydantic models for the faceted log search engine.

Keeps the wire-level shapes we accept/emit in one place so the API
layer, the synthetic generator, and the storage layer all agree on
types. Timestamps are stored as unix-epoch **seconds** (INTEGER) to
match the SQLite schema in ``src/storage/sqlite_store.py``.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

LogLevel = Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]


class LogEntry(BaseModel):
    """A single log row as it flows through ingest + storage + search.

    The ``id`` default factory generates a UUID4 hex so callers can
    omit it safely. ``ts`` is a unix-epoch **seconds** integer.
    ``metadata`` is a free-form dict; on write it is JSON-serialized
    into the SQLite ``metadata TEXT`` column.
    """

    id: str = Field(default_factory=lambda: uuid4().hex)
    ts: int = Field(default_factory=lambda: int(time.time()))
    service: str
    level: LogLevel
    region: str
    response_time_ms: float = Field(ge=0)
    source_ip: Optional[str] = None
    request_id: Optional[str] = None
    message: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Reject unknown fields so typos/bad clients surface immediately.
    model_config = ConfigDict(extra="forbid")


class IngestResponse(BaseModel):
    """Response body for ``POST /api/logs`` and ``POST /api/logs/batch``."""

    inserted_count: int
    # We return ids only for small batches to keep responses small,
    # but for now we always return; the endpoint clamps if needed.
    ids: List[str] = Field(default_factory=list)


class GenerateResponse(BaseModel):
    """Response body for ``POST /api/logs/generate``."""

    generated_count: int
    query_time_ms: float
