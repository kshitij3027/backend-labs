"""Pydantic v2 request/response contracts for the Log Recommendation Engine.

These models are the public shape of the HTTP API. They are intentionally small
and reusable: later commits (recommend / feedback endpoints) build on the same
incident primitives defined here.

Scope (C3): the incident corpus surface — create / read / list. Incidents are
created with ``embedding = NULL`` (vectors are computed in C5), so the wire
contract never exposes the raw 384-dim vector; instead :class:`IncidentOut`
carries a boolean :attr:`~IncidentOut.has_embedding` derived from whether the
stored row has one.

Conventions
-----------
* All timestamps are timezone-aware ``datetime`` objects (the ORM stores
  ``DateTime(timezone=True)``).
* ``severity`` is constrained to the canonical, ordinal set
  :data:`SEVERITIES` (most → least severe) so the generator and the (later)
  contextual scoring agree on the vocabulary.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Canonical severities, ordered most-severe → least-severe. This single source of
# truth is reused by the synthetic generator (:mod:`src.generator`) and, later, by
# the contextual severity-distance scoring. The ordinal position is meaningful:
# index 0 is the most severe.
SEVERITIES: list[str] = ["critical", "high", "medium", "low"]

#: Literal alias for the severity field so both request validation and the OpenAPI
#: schema advertise the closed set.
Severity = Literal["critical", "high", "medium", "low"]


class IncidentCreate(BaseModel):
    """Request body for ``POST /incidents`` — one resolved incident to ingest.

    ``embedding`` is intentionally not part of the input: it is computed later
    (C5), so an incident created here always lands with ``embedding = NULL``.
    """

    title: str = Field(..., min_length=1, max_length=256)
    description: str = Field(..., min_length=1)
    service: str = Field(..., min_length=1, max_length=128)
    severity: Severity
    tags: list[str] = Field(default_factory=list)
    resolution: str = Field(..., min_length=1)

    @field_validator("title", "description", "resolution")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        """Reject whitespace-only text for the required free-text fields."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("must not be empty or whitespace-only")
        return stripped

    @field_validator("service")
    @classmethod
    def _service_not_blank(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("service must not be empty")
        return stripped

    @field_validator("tags")
    @classmethod
    def _clean_tags(cls, v: list[str]) -> list[str]:
        """Trim tags and drop blanks, preserving order (deduplicated)."""
        seen: set[str] = set()
        cleaned: list[str] = []
        for tag in v:
            t = tag.strip()
            if t and t not in seen:
                seen.add(t)
                cleaned.append(t)
        return cleaned


class IncidentOut(BaseModel):
    """Response shape for a single incident.

    Built directly from an ORM :class:`~src.db.models.Incident` row
    (``from_attributes=True``). The raw ``embedding`` vector is never serialised;
    :attr:`has_embedding` reports whether the row carries one (``True`` once C5
    backfills it).
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    title: str
    description: str
    service: str
    severity: str
    tags: list[str] = Field(default_factory=list)
    resolution: str
    created_at: datetime
    has_embedding: bool = False

    @classmethod
    def from_orm_incident(cls, incident: object) -> "IncidentOut":
        """Build from an ORM ``Incident``, deriving ``has_embedding`` from the vector.

        ``model_validate`` alone cannot compute ``has_embedding`` from the raw
        ``embedding`` attribute (names differ), so this helper maps the row
        explicitly. Kept tolerant of either an ORM instance or a mapping.
        """
        get = (
            (lambda name: getattr(incident, name))
            if not isinstance(incident, dict)
            else incident.__getitem__
        )
        embedding = get("embedding") if _has(incident, "embedding") else None
        return cls(
            id=get("id"),
            title=get("title"),
            description=get("description"),
            service=get("service"),
            severity=get("severity"),
            tags=list(get("tags") or []),
            resolution=get("resolution"),
            created_at=get("created_at"),
            has_embedding=embedding is not None,
        )


def _has(obj: object, name: str) -> bool:
    """True if ``obj`` (ORM instance or mapping) exposes ``name``."""
    if isinstance(obj, dict):
        return name in obj
    return hasattr(obj, name)


class IncidentList(BaseModel):
    """Paginated response for ``GET /incidents``."""

    items: list[IncidentOut] = Field(default_factory=list)
    total: int
    limit: int
    offset: int
