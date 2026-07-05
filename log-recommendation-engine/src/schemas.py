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
from typing import Literal, Optional

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


class IncidentUpdate(BaseModel):
    """Request body for ``PUT /incidents/{id}`` — a *partial* update of one incident.

    Every field is optional; only the ones supplied are applied (a merge over the
    stored row). A supplied free-text field (``title`` / ``description`` / ``service``
    / ``resolution``) must be non-blank (whitespace-only is rejected with 422), and a
    supplied ``severity`` must be one of :data:`SEVERITIES`. ``tags``, when supplied,
    is trimmed / de-blanked / de-duplicated (an explicit ``[]`` clears the tags).

    Because ``None`` means "leave unchanged", the router distinguishes a supplied field
    from an omitted one via ``model_dump(exclude_unset=True)``. Re-embedding on the
    ``PUT`` path is triggered only when one of the *text* fields that feed
    :func:`src.embeddings.build_incident_text` — ``title`` / ``description`` / ``tags``
    — is among the supplied fields; a pure ``service`` / ``severity`` / ``resolution``
    edit does not change the embedded document text and so skips re-embedding.

    Extra / unknown keys are **forbidden** (``extra="forbid"``) so a typo'd field is a
    422 at the schema boundary rather than a silently-ignored no-op.
    """

    model_config = ConfigDict(extra="forbid")

    title: Optional[str] = Field(default=None, min_length=1, max_length=256)
    description: Optional[str] = Field(default=None, min_length=1)
    service: Optional[str] = Field(default=None, min_length=1, max_length=128)
    severity: Optional[Severity] = None
    tags: Optional[list[str]] = None
    resolution: Optional[str] = Field(default=None, min_length=1)

    @field_validator("title", "description", "service", "resolution")
    @classmethod
    def _not_blank(cls, v: Optional[str]) -> Optional[str]:
        """Reject whitespace-only text for a *supplied* free-text field (None passes)."""
        if v is None:
            return None
        stripped = v.strip()
        if not stripped:
            raise ValueError("must not be empty or whitespace-only")
        return stripped

    @field_validator("tags")
    @classmethod
    def _clean_tags(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        """Trim tags and drop blanks (preserving order, de-duplicated); None passes.

        An explicit empty list is preserved (it clears the incident's tags); ``None``
        means "leave the tags unchanged".
        """
        if v is None:
            return None
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


# --------------------------------------------------------------------------- #
# Feedback surface (C10): POST /feedback request / response
# --------------------------------------------------------------------------- #
class FeedbackRequest(BaseModel):
    """Request body for ``POST /feedback`` — one helpful / not-helpful vote.

    A vote references a real prior served result: ``recommendation_id`` (the
    :class:`~src.db.models.Recommendation` returned by ``POST /recommend``) and
    ``incident_id`` (one of that recommendation's suggested incidents). The endpoint
    validates the pair — an unknown ``recommendation_id`` is a 404, and an
    ``incident_id`` that was not one of that recommendation's suggestions is a 400 —
    so the learned aggregate can only ever be built from votes on suggestions that
    were actually served.
    """

    recommendation_id: int = Field(..., ge=1)
    incident_id: int = Field(..., ge=1)
    helpful: bool


class FeedbackResponse(BaseModel):
    """Response for ``POST /feedback`` — the post-update aggregate for the voted pair.

    ``recorded`` is ``True`` when the vote was persisted. ``query_pattern`` is the
    bucket the vote was aggregated under (derived from the recommendation's stored
    ``service`` / ``severity`` / ``tags`` facets). ``helpful_count`` /
    ``unhelpful_count`` are the **cumulative** tallies for that
    ``(query_pattern, incident_id)`` pair *after* applying this vote — the learned
    signal the feedback-driven re-ranking (C11) reads.
    """

    recorded: bool
    query_pattern: str
    incident_id: int
    helpful_count: int
    unhelpful_count: int


# --------------------------------------------------------------------------- #
# Runtime config surface (C12): GET / PUT /config
# --------------------------------------------------------------------------- #
class ConfigUpdate(BaseModel):
    """Request body for ``PUT /config`` — a *partial* set of runtime-tunable knobs.

    Every field is optional; only the ones supplied are overridden (a merge over the
    current effective config). The keys mirror the tunable subset in
    :data:`src.runtime_config.TUNABLE_KEYS`. Basic bounds are advertised here for the
    OpenAPI schema, but the authoritative validation (and the 422 on violation) lives
    in :func:`src.runtime_config.set_overrides`, which also rejects an all-empty body.

    Extra / unknown keys are **forbidden** (``extra="forbid"``) so a typo'd field is a
    422 at the schema boundary rather than a silently-ignored no-op.
    """

    model_config = ConfigDict(extra="forbid")

    weight_semantic: float | None = Field(default=None, ge=0.0)
    weight_contextual: float | None = Field(default=None, ge=0.0)
    weight_feedback: float | None = Field(default=None, ge=0.0)
    epsilon_explore: float | None = Field(default=None, ge=0.0, le=1.0)
    diversity_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    recency_half_life_days: float | None = Field(default=None, gt=0.0)
    top_k: int | None = Field(default=None, ge=1)
    high_confidence_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    medium_confidence_threshold: float | None = Field(default=None, ge=0.0, le=1.0)


class ConfigResponse(BaseModel):
    """Response for ``GET | PUT /config`` — the current effective runtime config.

    ``version`` is the global config version (bumped by every successful ``PUT``, folded
    into the recommendation cache key so a retune invalidates cached results). ``config``
    is the full effective tunable map (static defaults overlaid with any live Redis
    overrides) — the exact values the next ``/recommend`` will rank with.
    """

    version: int
    config: dict


# --------------------------------------------------------------------------- #
# Stats surface (C13): GET /stats — corpus / feedback rollups
# --------------------------------------------------------------------------- #
class PatternStat(BaseModel):
    """One learned ``query_pattern`` bucket's helpful / unhelpful tally.

    A row of the ``top_patterns`` list on :class:`StatsResponse`: the query-pattern
    key the feedback aggregate is grouped by, plus its summed helpful / unhelpful
    votes across every incident in that bucket.
    """

    query_pattern: str
    helpful: int
    unhelpful: int


class StatsResponse(BaseModel):
    """Response for ``GET /stats`` — an at-a-glance summary of the whole system.

    Rolls up the durable corpus + feedback state so a dashboard can show coverage
    and learning progress without paging through the raw rows:

    * ``corpus_size`` — total incidents; ``embedded_count`` — how many carry a
      (non-null) vector and are therefore semantically searchable.
    * ``by_service`` / ``by_severity`` — incident counts grouped by each facet.
    * ``feedback_total`` / ``feedback_helpful`` / ``feedback_unhelpful`` — raw vote
      tallies (``helpful + unhelpful == total``).
    * ``recommendations_served`` — how many recommendations have been persisted.
    * ``top_patterns`` — the busiest learned query-pattern buckets (by total votes).
    """

    corpus_size: int
    embedded_count: int
    by_service: dict[str, int] = Field(default_factory=dict)
    by_severity: dict[str, int] = Field(default_factory=dict)
    feedback_total: int
    feedback_helpful: int
    feedback_unhelpful: int
    recommendations_served: int
    top_patterns: list[PatternStat] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Deep health surface (C13): GET /health — per-subsystem readiness
# --------------------------------------------------------------------------- #
class ComponentsHealth(BaseModel):
    """Per-subsystem readiness booleans reported by the deep ``GET /health``.

    Each flag is the result of a fast, failure-tolerant probe (any error degrades
    to ``False`` — the health check never raises):

    * ``database`` — a ``SELECT 1`` against Postgres succeeded.
    * ``vector_extension`` — the pgvector ``vector`` extension is installed (only
      meaningful when ``database`` is ``True``).
    * ``redis`` — Redis answered a ``PING``.
    * ``embedding_model`` — the model singleton is **already loaded** in this
      process (a cheap inspection of the ``lru_cache`` — the check never forces a
      load, so a cold-but-healthy process legitimately reports ``False`` here).
    """

    database: bool = False
    vector_extension: bool = False
    redis: bool = False
    embedding_model: bool = False


class HealthResponse(BaseModel):
    """Response for the deep ``GET /health`` — liveness + per-component readiness.

    ``status`` is ``"ok"`` only when every *required* dependency (database, redis)
    is up, else ``"degraded"``. The endpoint always returns **HTTP 200** while the
    process is alive (so the container healthcheck stays green); a degraded stack is
    reported in this body rather than via a non-2xx status. ``corpus_size`` is a
    best-effort incident count (``0`` when the database is unreachable).

    ``instance`` is the serving process's hostname (``socket.gethostname()``). Under
    ``docker compose --scale api=N`` each replica runs in its own container with a
    unique hostname/container id, so repeated ``/api/health`` calls through the
    dashboard's nginx return **different** ``instance`` values across replicas — making
    the per-request Docker-DNS round-robin observable (see docker-compose.scale.yml).
    """

    status: Literal["ok", "degraded"]
    service: str
    version: str
    instance: str
    components: ComponentsHealth
    corpus_size: int = 0
