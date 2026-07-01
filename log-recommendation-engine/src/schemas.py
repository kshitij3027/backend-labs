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


# --------------------------------------------------------------------------- #
# Recommendation surface (C9): POST /recommend request / response
# --------------------------------------------------------------------------- #
class RecommendRequest(BaseModel):
    """Request body for ``POST /recommend`` — a new (unresolved) incident to match.

    The ``title`` / ``description`` / ``tags`` are embedded (via the same canonical
    document text as the corpus) and used for semantic retrieval; ``service`` /
    ``severity`` / ``tags`` additionally feed the *contextual* signals that softly
    reward same-service / close-severity / shared-tag matches.

    Filtering policy
    ----------------
    By default the search is **pure semantic** and the contextual signals act as
    soft *preferences* (no hard filtering). Set ``restrict_service`` /
    ``restrict_severity`` to turn ``service`` / ``severity`` into hard *constraints*
    (an exact pre-filter before the K-NN). When a hard filter is too narrow to fill
    ``top_k``, the service widens the search by dropping it (see
    :func:`src.recommendation_service.recommend`).
    """

    title: str = Field(..., min_length=1, max_length=256)
    description: str = Field(..., min_length=1)
    service: str | None = Field(default=None, max_length=128)
    severity: str | None = Field(default=None)
    tags: list[str] = Field(default_factory=list)
    #: Optional per-request override of the configured ``top_k`` (1–50).
    top_k: int | None = Field(default=None, ge=1, le=50)
    #: Hard-constrain retrieval to ``service`` (else it is only a soft signal).
    restrict_service: bool = False
    #: Hard-constrain retrieval to ``severity`` (else it is only a soft signal).
    restrict_severity: bool = False

    @field_validator("title", "description")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        """Reject whitespace-only title/description (they carry the query meaning)."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("must not be empty or whitespace-only")
        return stripped

    @field_validator("service")
    @classmethod
    def _service_blank_to_none(cls, v: str | None) -> str | None:
        """Normalise a blank/whitespace ``service`` to ``None`` (no soft signal)."""
        if v is None:
            return None
        stripped = v.strip()
        return stripped or None

    @field_validator("severity")
    @classmethod
    def _severity_known(cls, v: str | None) -> str | None:
        """Validate ``severity`` (when given) is within the canonical vocabulary.

        A blank string normalises to ``None``; any non-empty value must be one of
        :data:`SEVERITIES` (case-insensitively) or the request is rejected with 422.
        """
        if v is None:
            return None
        stripped = v.strip().lower()
        if not stripped:
            return None
        if stripped not in SEVERITIES:
            raise ValueError(
                "severity must be one of " + ", ".join(SEVERITIES)
            )
        return stripped

    @field_validator("tags")
    @classmethod
    def _clean_tags(cls, v: list[str]) -> list[str]:
        """Trim tags, drop blanks, de-duplicate while preserving order."""
        seen: set[str] = set()
        cleaned: list[str] = []
        for tag in v:
            t = tag.strip()
            if t and t not in seen:
                seen.add(t)
                cleaned.append(t)
        return cleaned


class QueryEcho(BaseModel):
    """The normalised query facets echoed back on a recommendation response."""

    title: str
    description: str
    service: str | None = None
    severity: str | None = None
    tags: list[str] = Field(default_factory=list)


class Suggestion(BaseModel):
    """One ranked suggestion: the matched incident + its **resolution** and scores.

    ``resolution`` is the whole point of a suggestion — it is the fix that resolved
    the matched historical incident. ``score`` is the final blended relevance;
    ``semantic`` / ``contextual`` / ``feedback`` are the individual blended signals
    (``feedback`` is a ``0.0`` stub until C11), and ``breakdown`` carries the full
    per-signal detail (contextual sub-signals + the blend weights used) so the
    suggestion is self-explaining in the UI.
    """

    incident_id: int
    title: str
    service: str
    severity: str
    tags: list[str] = Field(default_factory=list)
    resolution: str
    score: float
    semantic: float
    contextual: float
    feedback: float
    breakdown: dict = Field(default_factory=dict)


class RecommendResponse(BaseModel):
    """Response for ``POST /recommend`` — the served ranked suggestions.

    ``recommendation_id`` is the persisted :class:`~src.db.models.Recommendation`
    row id; C10 feedback references it (together with a suggestion's
    ``incident_id``) to attribute a vote to a real prior result. ``cached`` is
    ``True`` when the whole response was served from the Redis recommendation cache
    (an identical prior query), ``False`` when freshly computed.
    """

    recommendation_id: int
    query: QueryEcho
    suggestions: list[Suggestion] = Field(default_factory=list)
    count: int
    cached: bool = False
