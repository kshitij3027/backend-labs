"""Repository helpers: plain functions over a :class:`~sqlalchemy.orm.Session`.

These are thin, well-typed CRUD operations the rest of the system calls. They
keep the ORM details in one place so services / the API never build raw queries.

Transaction convention
-----------------------
Every mutating helper accepts ``commit: bool = False``:

* ``commit=False`` (default) — the helper :meth:`Session.flush`-es so the row is
  assigned a primary key and is visible within the transaction, but leaves the
  final ``COMMIT`` to the caller. This lets a caller batch several writes into
  one transaction.
* ``commit=True`` — the helper commits before returning, refreshing the instance
  so all server-side defaults are populated.

Read helpers never write.

Scope: incidents are implemented in full (create / get / list / set embedding /
delete / bulk). The recommendation helpers (C9) persist and fetch a served result;
the feedback + suggestion-score helpers (C10) insert a raw vote and upsert the
learned ``(query_pattern, incident_id)`` aggregate that the feedback-driven
re-ranking (C11) reads.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

import numpy as np
from sqlalchemy import func, literal, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.models import Feedback, Incident, Recommendation, SuggestionScore


def _finalize(session: Session, instance: Any, commit: bool) -> Any:
    """Flush (always) and optionally commit + refresh ``instance``."""
    session.add(instance)
    if commit:
        session.commit()
        session.refresh(instance)
    else:
        session.flush()
    return instance


# --------------------------------------------------------------------------- #
# Incident (the corpus)
# --------------------------------------------------------------------------- #
def add_incident(
    session: Session,
    *,
    title: str,
    description: str,
    service: str,
    severity: str,
    tags: Sequence[str] | None = None,
    resolution: str,
    embedding: Sequence[float] | None = None,
    commit: bool = False,
) -> Incident:
    """Insert a single incident and return it.

    ``embedding`` stays ``None`` until the embedding service (C5) populates it;
    ``tags`` defaults to an empty list. The instance is flushed (PK assigned)
    unless ``commit=True``.
    """
    incident = Incident(
        title=title,
        description=description,
        service=service,
        severity=severity,
        tags=list(tags) if tags is not None else [],
        resolution=resolution,
        embedding=list(embedding) if embedding is not None else None,
    )
    return _finalize(session, incident, commit)


def add_incidents_bulk(
    session: Session,
    rows: Iterable[Mapping[str, Any]],
    *,
    commit: bool = False,
) -> list[Incident]:
    """Insert many incidents.

    Each item in ``rows`` must provide ``title``, ``description``, ``service``,
    ``severity`` and ``resolution``; ``tags`` and ``embedding`` are optional.
    Returns the created instances in input order.
    """
    incidents = [
        Incident(
            title=r["title"],
            description=r["description"],
            service=r["service"],
            severity=r["severity"],
            tags=list(r["tags"]) if r.get("tags") is not None else [],
            resolution=r["resolution"],
            embedding=(
                list(r["embedding"]) if r.get("embedding") is not None else None
            ),
        )
        for r in rows
    ]
    session.add_all(incidents)
    if commit:
        session.commit()
        for inc in incidents:
            session.refresh(inc)
    else:
        session.flush()
    return incidents


def get_incident(session: Session, incident_id: int) -> Incident | None:
    """Return the incident with ``incident_id`` (or ``None``)."""
    return session.get(Incident, incident_id)


def _incident_filters(
    stmt: "Any",
    *,
    service: str | None,
    severity: str | None,
    q: str | None,
    tags: Sequence[str] | None,
) -> "Any":
    """Apply the shared, *composable* incident filter predicates to ``stmt``.

    Single source of truth for :func:`list_incidents` / :func:`count_incidents` so
    the page query and its ``total`` count can never drift apart. All filters are
    optional and additive (ANDed together when more than one is given):

    * ``service`` / ``severity`` — exact-match.
    * ``q`` — case-insensitive substring (``ILIKE '%q%'``) over ``title`` **or**
      ``description`` (surrounding whitespace is stripped; a blank ``q`` is ignored).
    * ``tags`` — Postgres array **overlap** (``&&``): match incidents whose ``tags``
      share at least one element with the requested list (blanks dropped; an all-blank
      list is ignored).
    """
    if service is not None:
        stmt = stmt.where(Incident.service == service)
    if severity is not None:
        stmt = stmt.where(Incident.severity == severity)
    if q is not None:
        needle = q.strip()
        if needle:
            like = f"%{needle}%"
            stmt = stmt.where(
                Incident.title.ilike(like) | Incident.description.ilike(like)
            )
    if tags:
        wanted = [t.strip() for t in tags if t and t.strip()]
        if wanted:
            # Postgres array overlap (``&&``): row matches if its tags share ANY
            # element with ``wanted``. ``.overlap`` maps to the ``&&`` operator.
            stmt = stmt.where(Incident.tags.overlap(wanted))
    return stmt


def list_incidents(
    session: Session,
    *,
    limit: int = 100,
    offset: int = 0,
    service: str | None = None,
    severity: str | None = None,
    q: str | None = None,
    tags: Sequence[str] | None = None,
) -> list[Incident]:
    """Return incidents newest-first, filtered by any of the optional predicates.

    ``limit``/``offset`` paginate. ``service`` / ``severity`` are exact-match; ``q``
    is a case-insensitive substring over title+description (ILIKE); ``tags`` matches
    incidents whose ``tags`` array overlaps the requested tags. Filters are additive
    (see :func:`_incident_filters`).
    """
    stmt = select(Incident)
    stmt = _incident_filters(
        stmt, service=service, severity=severity, q=q, tags=tags
    )
    stmt = stmt.order_by(Incident.created_at.desc(), Incident.id.desc())
    stmt = stmt.limit(limit).offset(offset)
    return list(session.scalars(stmt).all())


def count_incidents(
    session: Session,
    *,
    service: str | None = None,
    severity: str | None = None,
    q: str | None = None,
    tags: Sequence[str] | None = None,
) -> int:
    """Return the total number of incidents matching the given filters.

    Uses the exact same predicates as :func:`list_incidents` (via
    :func:`_incident_filters`) but ignores pagination, so a paginated response can
    report an accurate ``total`` for the full filtered result set.
    """
    stmt = select(func.count()).select_from(Incident)
    stmt = _incident_filters(
        stmt, service=service, severity=severity, q=q, tags=tags
    )
    return int(session.scalar(stmt) or 0)


def get_incidents_missing_embedding(
    session: Session,
    *,
    limit: int = 500,
) -> list[Incident]:
    """Return incidents whose ``embedding IS NULL``, oldest-id first.

    Used by the C5 backfill (``scripts.backfill_embeddings``) to page through the
    rows that still need a vector computed. Ordered by ``id`` (ascending) so a
    resumable batch loop makes steady forward progress; ``limit`` caps the page
    size.
    """
    stmt = (
        select(Incident)
        .where(Incident.embedding.is_(None))
        .order_by(Incident.id.asc())
        .limit(limit)
    )
    return list(session.scalars(stmt).all())


def set_incident_embedding(
    session: Session,
    incident_id: int,
    embedding: Sequence[float],
    *,
    commit: bool = False,
) -> Incident | None:
    """Set the ``embedding`` vector on an incident and return it (or ``None``).

    Used by the embedding service / backfill (C5) once vectors are computed.
    """
    incident = session.get(Incident, incident_id)
    if incident is None:
        return None
    incident.embedding = list(embedding)
    if commit:
        session.commit()
        session.refresh(incident)
    else:
        session.flush()
    return incident


_INCIDENT_MUTABLE_FIELDS = frozenset(
    {"title", "description", "service", "severity", "tags", "resolution"}
)


def update_incident(
    session: Session,
    incident_id: int,
    *,
    fields: Mapping[str, Any],
    commit: bool = False,
) -> Incident | None:
    """Apply the given ``fields`` to an incident and return it (or ``None``).

    ``fields`` is a *partial* mapping (from ``IncidentUpdate.model_dump(exclude_unset=
    True)``): only the keys present are written, so an omitted field is left unchanged.
    Only the known mutable columns
    (:data:`_INCIDENT_MUTABLE_FIELDS` — ``title`` / ``description`` / ``service`` /
    ``severity`` / ``tags`` / ``resolution``) are applied; any other key is ignored
    (the schema layer already forbids unknown fields, this is defence in depth). The
    ``embedding`` is **not** touched here — the caller re-embeds separately when a text
    field changed (see the ``PUT`` route), keeping this a pure column write.

    Returns ``None`` if no incident with ``incident_id`` exists. Follows the repository
    transaction convention: flushes by default, commits + refreshes when ``commit=True``.
    """
    incident = session.get(Incident, incident_id)
    if incident is None:
        return None
    for key, value in fields.items():
        if key not in _INCIDENT_MUTABLE_FIELDS:
            continue
        if key == "tags":
            setattr(incident, key, list(value) if value is not None else [])
        else:
            setattr(incident, key, value)
    if commit:
        session.commit()
        session.refresh(incident)
    else:
        session.flush()
    return incident


def delete_incident(
    session: Session,
    incident_id: int,
    *,
    commit: bool = False,
) -> bool:
    """Delete the incident with ``incident_id``, cleaning up its dependent rows first.

    ``Feedback`` and ``SuggestionScore`` both carry a ``incident_id`` foreign key to
    this row, so they are deleted **first** (in the same transaction) to satisfy the FK
    constraints before the incident itself is removed — otherwise Postgres would reject
    the incident delete. (``Recommendation`` rows are intentionally left: they reference
    incidents only inside their opaque ``query_json`` payload, not via a FK, so they are
    a historical log that need not be purged and cannot break the delete.)

    Returns ``True`` if the incident existed and was deleted, ``False`` if none existed
    (in which case no dependent rows are touched). Follows the repository transaction
    convention: flushes by default, commits when ``commit=True`` — so the dependent-row
    deletes and the incident delete land atomically.
    """
    incident = session.get(Incident, incident_id)
    if incident is None:
        return False
    # Remove FK-dependent rows first (bulk deletes, no ORM instances needed).
    session.query(Feedback).filter(
        Feedback.incident_id == incident_id
    ).delete(synchronize_session=False)
    session.query(SuggestionScore).filter(
        SuggestionScore.incident_id == incident_id
    ).delete(synchronize_session=False)
    session.delete(incident)
    if commit:
        session.commit()
    else:
        session.flush()
    return True


def knn_by_embedding(
    session: Session,
    query_vec: "np.ndarray | Sequence[float]",
    *,
    k: int,
    service: str | None = None,
    severities: Sequence[str] | None = None,
) -> list[tuple[Incident, float]]:
    """Return the ``k`` nearest incidents to ``query_vec`` by **cosine distance**.

    The heart of C6 semantic retrieval. Builds a KNN ``SELECT`` that orders rows by
    ``embedding <=> :query_vec`` (pgvector's ``cosine_distance`` — the operator the
    HNSW ``vector_cosine_ops`` index accelerates), so Postgres can serve this from
    the ANN index rather than a sequential scan.

    * Incidents whose ``embedding IS NULL`` are excluded — a NULL vector cannot be
      scored (these are rows the C5 backfill has not reached yet).
    * ``service`` (exact match) and ``severities`` (``severity IN (...)``) are
      **optional, additive** hard pre-filters, applied only when provided. No
      auto-widening happens here (that is C9's concern).

    Returns a list of ``(incident, distance)`` tuples ordered by ``distance``
    ascending (nearest first), at most ``k`` long. ``distance`` is the cosine
    distance in ``[0, 2]``; callers convert it to a similarity via ``1 - distance``
    (see :func:`src.retrieval.retrieve_candidates`).

    ``query_vec`` may be a NumPy array or a plain float sequence; pgvector's
    ``Vector`` bind handles both. It must have the configured dimensionality (384)
    or Postgres raises a dimension-mismatch error.
    """
    # ``list(...)`` normalises a NumPy array (or any sequence) into the plain list
    # of floats that pgvector's Vector bind expects.
    vec = list(query_vec)
    distance = Incident.embedding.cosine_distance(vec).label("distance")
    stmt = select(Incident, distance).where(Incident.embedding.is_not(None))
    if service is not None:
        stmt = stmt.where(Incident.service == service)
    if severities:
        stmt = stmt.where(Incident.severity.in_(list(severities)))
    stmt = stmt.order_by(distance.asc()).limit(k)
    rows = session.execute(stmt).all()
    return [(incident, float(dist)) for incident, dist in rows]


# --------------------------------------------------------------------------- #
# Recommendation (served queries) — thin; extended in C9
# --------------------------------------------------------------------------- #
def create_recommendation(
    session: Session,
    *,
    query_hash: str,
    query_json: Mapping[str, Any],
    commit: bool = False,
) -> Recommendation:
    """Persist a served recommendation (query + returned suggestion ids)."""
    recommendation = Recommendation(
        query_hash=query_hash,
        query_json=dict(query_json),
    )
    return _finalize(session, recommendation, commit)


def get_recommendation(
    session: Session, recommendation_id: int
) -> Recommendation | None:
    """Return the recommendation with ``recommendation_id`` (or ``None``)."""
    return session.get(Recommendation, recommendation_id)


# --------------------------------------------------------------------------- #
# Feedback (votes) — the raw event log behind the learned aggregate (C10)
# --------------------------------------------------------------------------- #
def add_feedback(
    session: Session,
    *,
    recommendation_id: int,
    incident_id: int,
    query_pattern: str,
    helpful: bool,
    commit: bool = False,
) -> Feedback:
    """Persist a single helpful / not-helpful vote and return it."""
    feedback = Feedback(
        recommendation_id=recommendation_id,
        incident_id=incident_id,
        query_pattern=query_pattern,
        helpful=helpful,
    )
    return _finalize(session, feedback, commit)


# --------------------------------------------------------------------------- #
# SuggestionScore (learned aggregate) — the C11 re-ranking signal
# --------------------------------------------------------------------------- #
def get_suggestion_score(
    session: Session, query_pattern: str, incident_id: int
) -> SuggestionScore | None:
    """Return the aggregate for one ``(query_pattern, incident_id)`` pair (or ``None``).

    Composite-PK point lookup. Used by :func:`upsert_suggestion_score` (get-or-create)
    and available to C11 when it needs a single pair's tally.
    """
    return session.get(SuggestionScore, (query_pattern, incident_id))


def upsert_suggestion_score(
    session: Session,
    *,
    query_pattern: str,
    incident_id: int,
    helpful: bool,
    commit: bool = False,
) -> SuggestionScore:
    """Increment the helpful / unhelpful tally for a ``(pattern, incident)`` pair.

    Get-or-create over the composite primary key: creates the aggregate row on the
    first vote (with the voted counter at 1), otherwise bumps the relevant counter
    and refreshes ``updated_at``. This is the write half of the C10 feedback loop —
    every vote lands here so the ``(pattern -> incident)`` signal C11 reads stays
    current.

    Concurrency: two votes for the same brand-new pair could each see ``None`` and
    try to insert, tripping the PK on the second flush. We handle that simply — on
    an :class:`~sqlalchemy.exc.IntegrityError` we roll back, re-fetch the now-present
    row and increment it — so a duplicate insert degrades to an increment rather than
    a 500.
    """
    score = session.get(SuggestionScore, (query_pattern, incident_id))
    if score is None:
        score = SuggestionScore(
            query_pattern=query_pattern,
            incident_id=incident_id,
            helpful_count=1 if helpful else 0,
            unhelpful_count=0 if helpful else 1,
        )
        session.add(score)
        try:
            session.flush()
        except IntegrityError:
            # A concurrent vote created the row first: recover by incrementing it.
            session.rollback()
            score = session.get(SuggestionScore, (query_pattern, incident_id))
            if score is None:  # pragma: no cover - defensive; row must exist now
                raise
            _bump_score(score, helpful)
    else:
        _bump_score(score, helpful)

    if commit:
        session.commit()
        session.refresh(score)
    else:
        session.flush()
    return score


def _bump_score(score: SuggestionScore, helpful: bool) -> None:
    """Increment the relevant counter on an existing aggregate and touch ``updated_at``."""
    if helpful:
        score.helpful_count += 1
    else:
        score.unhelpful_count += 1
    score.updated_at = func.now()


def get_suggestion_scores(
    session: Session, query_pattern: str
) -> list[SuggestionScore]:
    """Return all learned aggregate rows for a given ``query_pattern``.

    The read half of the feedback loop: C11 calls this with the current query's
    pattern to fold each candidate's ``(helpful_count, unhelpful_count)`` into a
    feedback boost/dampen term before re-ranking.
    """
    stmt = select(SuggestionScore).where(
        SuggestionScore.query_pattern == query_pattern
    )
    return list(session.scalars(stmt).all())


# --------------------------------------------------------------------------- #
# Aggregate / stats helpers (C13) — powers GET /stats and the deep /health count
# --------------------------------------------------------------------------- #
def count_embedded_incidents(session: Session) -> int:
    """Return how many incidents carry a (non-null) embedding — i.e. are searchable."""
    stmt = (
        select(func.count())
        .select_from(Incident)
        .where(Incident.embedding.is_not(None))
    )
    return int(session.scalar(stmt) or 0)


def incident_counts_by_service(session: Session) -> dict[str, int]:
    """Return ``{service: incident_count}`` over the whole corpus (``GROUP BY service``)."""
    stmt = (
        select(Incident.service, func.count())
        .group_by(Incident.service)
        .order_by(func.count().desc())
    )
    return {service: int(count) for service, count in session.execute(stmt).all()}


def incident_counts_by_severity(session: Session) -> dict[str, int]:
    """Return ``{severity: incident_count}`` over the whole corpus (``GROUP BY severity``)."""
    stmt = (
        select(Incident.severity, func.count())
        .group_by(Incident.severity)
        .order_by(func.count().desc())
    )
    return {severity: int(count) for severity, count in session.execute(stmt).all()}


def feedback_totals(session: Session) -> tuple[int, int, int]:
    """Return ``(total, helpful, unhelpful)`` raw feedback-vote counts.

    A single grouped scan over ``feedback.helpful``: the helpful / unhelpful splits are
    read off the ``True`` / ``False`` buckets and summed for the total (so
    ``helpful + unhelpful == total`` by construction).
    """
    stmt = select(Feedback.helpful, func.count()).group_by(Feedback.helpful)
    helpful = 0
    unhelpful = 0
    for is_helpful, count in session.execute(stmt).all():
        if is_helpful:
            helpful = int(count)
        else:
            unhelpful = int(count)
    return helpful + unhelpful, helpful, unhelpful


def count_recommendations(session: Session) -> int:
    """Return the number of persisted (served) recommendation rows."""
    stmt = select(func.count()).select_from(Recommendation)
    return int(session.scalar(stmt) or 0)


def top_patterns(
    session: Session, *, limit: int = 5
) -> list[tuple[str, int, int]]:
    """Return the busiest learned query-pattern buckets by *total* votes.

    Sums each ``query_pattern``'s helpful / unhelpful counts across every incident in
    that bucket (``GROUP BY query_pattern``), ordered by total votes descending.
    Returns up to ``limit`` ``(query_pattern, helpful, unhelpful)`` tuples — the input
    for :class:`~src.schemas.StatsResponse.top_patterns`.
    """
    helpful_sum = func.coalesce(func.sum(SuggestionScore.helpful_count), 0)
    unhelpful_sum = func.coalesce(func.sum(SuggestionScore.unhelpful_count), 0)
    stmt = (
        select(SuggestionScore.query_pattern, helpful_sum, unhelpful_sum)
        .group_by(SuggestionScore.query_pattern)
        .order_by((helpful_sum + unhelpful_sum).desc())
        .limit(limit)
    )
    return [
        (pattern, int(helpful), int(unhelpful))
        for pattern, helpful, unhelpful in session.execute(stmt).all()
    ]


def database_ready(session: Session) -> bool:
    """Return ``True`` if a trivial ``SELECT 1`` succeeds (a liveness probe of the DB).

    Never raises — any error (connection down, etc.) degrades to ``False`` so the deep
    ``/health`` endpoint can report DB status without failing the request.
    """
    try:
        return int(session.scalar(select(literal(1)))) == 1
    except Exception:  # noqa: BLE001 - health probe must never raise
        return False


def vector_extension_present(session: Session) -> bool:
    """Return ``True`` if the pgvector ``vector`` extension is installed.

    Queries ``pg_extension`` for ``extname = 'vector'``. Never raises — any error
    degrades to ``False`` (the deep ``/health`` treats this as an optional sub-field).
    """
    try:
        stmt = text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        return session.execute(stmt).first() is not None
    except Exception:  # noqa: BLE001 - health probe must never raise
        return False
