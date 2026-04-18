"""Pydantic models for the faceted log search engine.

Keeps the wire-level shapes we accept/emit in one place so the API
layer, the synthetic generator, and the storage layer all agree on
types. Timestamps are stored as unix-epoch **seconds** (INTEGER) to
match the SQLite schema in ``src/storage/sqlite_store.py``.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Literal, Optional, Union
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

LogLevel = Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]


# Human-readable labels for facet dimensions. Kept next to the models
# so both the backend response and any dashboard template can import
# the same mapping and stay in lock-step with FACET_DIMS.
FACET_DISPLAY_NAMES: Dict[str, str] = {
    "service": "Service",
    "level": "Level",
    "region": "Region",
    "latency_bucket": "Response time",
    "hour_bucket": "Hour of day",
}


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


# ---------------------------------------------------------------------------
# Faceted search models.
# ---------------------------------------------------------------------------

class FacetValue(BaseModel):
    """A single bucket inside one facet dimension."""

    # Values can be ints (hour_bucket) or strings (everything else).
    value: Union[str, int]
    count: int
    selected: bool = False


class FacetSummary(BaseModel):
    """Top-N values for one facet dimension, plus truncation flag."""

    name: str
    display_name: str
    values: List[FacetValue] = Field(default_factory=list)
    # True iff there were more values than the UI cap (``MAX_FACET_VALUES``)
    # so the client can render a "Show more" affordance.
    has_more_values: bool = False


class SearchRequest(BaseModel):
    """Input body for ``POST /api/search``.

    ``filters`` is a dimension → list-of-values map. ``cursor`` is the
    ``ts`` of the last row returned from the previous page (keyset
    pagination, see ``build_results_sql``). ``limit`` is clamped to a
    reasonable range to protect the server.
    """

    query: Optional[str] = None
    filters: Dict[str, List[Union[str, int]]] = Field(default_factory=dict)
    ts_start: Optional[int] = None
    ts_end: Optional[int] = None
    cursor: Optional[int] = None
    limit: int = Field(default=10, ge=1, le=200)

    model_config = ConfigDict(extra="forbid")


class SearchResponse(BaseModel):
    """Full response shape for ``POST /api/search``.

    ``logs`` carries already-materialized dicts (one per row) so the
    server can hand rows straight to the frontend without a second
    pass of Pydantic validation. Field order mirrors
    ``query_builder.RESULT_COLUMNS`` with ``metadata`` parsed back
    from JSON text into a dict.

    ``cached`` is stamped True when the response was served from the
    Redis cache-aside layer (see ``storage/redis_cache.py``). It
    defaults to False so cold paths don't need to set it explicitly.
    """

    logs: List[Dict[str, Any]] = Field(default_factory=list)
    total_count: Optional[int] = None
    has_more: bool = False
    next_cursor: Optional[int] = None
    facets: List[FacetSummary] = Field(default_factory=list)
    query_time_ms: float = 0.0
    applied_filters: Dict[str, List[Union[str, int]]] = Field(default_factory=dict)
    cached: bool = False


class FacetsOnlyResponse(BaseModel):
    """Response shape for ``GET /api/facets`` — facets without rows."""

    facets: List[FacetSummary] = Field(default_factory=list)
    query_time_ms: float = 0.0
    applied_filters: Dict[str, List[Union[str, int]]] = Field(default_factory=dict)
    cached: bool = False


# ---------------------------------------------------------------------------
# Stats / diagnostics models.
# ---------------------------------------------------------------------------

class CacheStatsModel(BaseModel):
    """Serialized view of the Redis cache-counter dataclass.

    ``hit_rate`` is ``hits / (hits + misses)`` as a float in [0, 1], or
    ``None`` when the denominator is zero (no lookups yet). Kept
    separate from the ``CacheStats`` dataclass in
    ``storage/redis_cache.py`` to keep that module free of Pydantic.
    """

    hits: int = 0
    misses: int = 0
    errors: int = 0
    hit_rate: Optional[float] = None


class StatsResponse(BaseModel):
    """Response body for ``GET /api/stats``.

    * ``total_logs`` — ``SELECT COUNT(*) FROM logs``.
    * ``facet_cardinality`` — per-dimension ``COUNT(DISTINCT ...)``
      so the UI can decide whether to surface a dimension at all.
    * ``cache`` — current hit/miss/error counters + derived hit rate.
    * ``redis_reachable`` — live ``PING`` result at request time.
    """

    total_logs: int
    facet_cardinality: Dict[str, int] = Field(default_factory=dict)
    cache: CacheStatsModel = Field(default_factory=CacheStatsModel)
    redis_reachable: bool = False
