"""Pydantic request/response models for the log search service.

These shapes are the contract between the FastAPI handlers, the
search service, and the clients. They are defined in one place so the
API layer, the reranker, and the dashboard JS all agree on field
names, types, and validation rules.

Note: this module deliberately avoids ``from __future__ import
annotations``. With pydantic 2 and ``default_factory`` patterns, the
runtime type introspection works more reliably without PEP 563
deferred evaluation.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Log levels
# ---------------------------------------------------------------------------

# Accept both WARN and WARNING (producers use either dialect) and FATAL
# alongside the standard ERROR/INFO/DEBUG set.
LogLevel = Literal["DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"]


# ---------------------------------------------------------------------------
# Log entry + ingest
# ---------------------------------------------------------------------------

class LogEntry(BaseModel):
    """A single log row accepted by ``POST /api/logs[/bulk]``.

    ``id`` is server-assigned at admit time — clients may omit it and
    any value they send is overwritten. ``service`` and ``level`` have
    sensible defaults so noisy producers can post a minimal shape.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    message: str = Field(min_length=1)
    timestamp: float
    service: str = "unknown"
    level: LogLevel = "INFO"
    metadata: dict = Field(default_factory=dict)


class LogBulkRequest(BaseModel):
    """Request body for ``POST /api/logs/bulk``.

    The upper bound on ``entries`` keeps a rogue client from OOM-ing
    the app with one enormous batch; the lower bound rejects empty
    payloads as 422s so the handler can assume non-empty input.
    """

    entries: list[LogEntry] = Field(min_length=1, max_length=10000)


class LogAckResponse(BaseModel):
    """Response body echoed after a successful ingest.

    Returning the first/last ``doc_id`` lets clients pair their batch
    with the indexer's assignment without having to re-query. The
    ``index_version`` is bumped on every successful write so callers
    can use it as an etag for subsequent reads.
    """

    accepted: int
    first_doc_id: int
    last_doc_id: int
    index_version: int


# ---------------------------------------------------------------------------
# Search API
# ---------------------------------------------------------------------------

class SearchRequest(BaseModel):
    """Query parameters for ``POST /api/search``.

    ``limit`` is clamped to ``[1, 500]`` so a malformed client can't
    ask for a million results. ``context`` is a free-form dict because
    the ranker's context-mode hooks are still evolving — locking a
    schema this early would just force churn later.
    """

    query: str = Field(min_length=1)
    limit: int = Field(default=10, ge=1, le=500)
    context: dict | None = None


class RankingExplanation(BaseModel):
    """Per-result breakdown of the multi-factor score.

    Each numeric field is the *weighted* contribution that component
    made to ``SearchResult.score`` so they sum (modulo rounding) to
    the reported score. ``reasons`` is a human-readable list of the
    boosts/penalties that fired (e.g. ``"incident_mode_boost"``).
    """

    tfidf: float
    temporal: float
    severity: float
    service: float
    context: float
    reasons: list[str] = Field(default_factory=list)


class SearchResult(BaseModel):
    """One row in a ranked search response."""

    log_entry: str
    timestamp: float
    service: str
    level: str
    score: float
    ranking_explanation: RankingExplanation


class SearchResponse(BaseModel):
    """Full response body for ``POST /api/search``.

    ``total_hits`` is the number of candidate documents the retriever
    surfaced before reranking; ``ranked_hits`` is how many survived
    into ``results``. ``execution_time_ms`` is measured at the
    service-boundary so clients can SLA against it.
    """

    query: str
    intent: str
    expanded_terms: list[str]
    results: list[SearchResult]
    total_hits: int
    ranked_hits: int
    execution_time_ms: float


class SuggestionsResponse(BaseModel):
    """Response body for ``GET /api/search/suggestions``."""

    suggestions: list[str]


# ---------------------------------------------------------------------------
# Stats / diagnostics
# ---------------------------------------------------------------------------

class StatsResponse(BaseModel):
    """Response body for ``GET /api/search/stats``.

    Surfaces the counters the dashboard plots: corpus size, vocab,
    index + idf versions, cache efficacy, and p95 latency so the
    search-latency SLO (<100ms) is observable at a glance.
    """

    total_docs: int
    unique_tokens: int
    index_version: int
    idf_version: int
    cache_hit_ratio: float
    p95_latency_ms: float


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    """Response body for ``GET /health``.

    Single-state ``status`` for now — the commit-01 smoke test asserts
    the exact JSON ``{"status": "ok"}`` and the docker healthcheck
    does the same, so broadening the shape here would break both.
    """

    status: Literal["ok"] = "ok"
