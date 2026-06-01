"""API-facing Pydantic v2 models.

These are the stable contracts other commits import. Keep field names stable;
later commits add response envelopes and richer stats models on top of these.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


class QueryRequest(BaseModel):
    """A query submitted to the cache layer."""

    query: str
    params: dict[str, Any] = Field(default_factory=dict)


class CacheMeta(BaseModel):
    """Per-response metadata describing how a result was served."""

    tier: str
    elapsed_ms: float
    key: str
    degraded: bool = False


class QueryResponse(BaseModel):
    """A query result plus the cache metadata that produced it."""

    result: Any
    meta: CacheMeta


class WarmRequest(BaseModel):
    """Request to proactively warm specific queries (or top-N recommendations)."""

    queries: list[QueryRequest] = Field(default_factory=list)
    top_n: int | None = None


class InvalidateRequest(BaseModel):
    """Selective cache invalidation by key pattern and/or tags.

    At least one of ``pattern`` or ``tags`` must be provided.
    """

    pattern: str | None = None
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_pattern_or_tags(self) -> "InvalidateRequest":
        if not self.pattern and not self.tags:
            raise ValueError("at least one of 'pattern' or 'tags' must be set")
        return self


class HotKey(BaseModel):
    """A frequently/recently accessed cache key with its frecency score."""

    key: str
    query: str
    score: float
    count: int


class Recommendation(BaseModel):
    """A ranked cache-warming recommendation produced by the pattern engine."""

    query: str
    params: dict[str, Any] = Field(default_factory=dict)
    key: str
    score: float
    reason: str = ""
