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

Scope (C2): incidents are implemented in full (create / get / list / set
embedding / delete / bulk). The recommendation, feedback and suggestion-score
helpers are thin, correct primitives that later commits (C9–C11) extend.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import func, select
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


def list_incidents(
    session: Session,
    *,
    limit: int = 100,
    offset: int = 0,
    service: str | None = None,
    severity: str | None = None,
) -> list[Incident]:
    """Return incidents newest-first, optionally filtered by service/severity.

    ``limit``/``offset`` paginate; ``service`` and ``severity`` (when given) are
    exact-match filters.
    """
    stmt = select(Incident)
    if service is not None:
        stmt = stmt.where(Incident.service == service)
    if severity is not None:
        stmt = stmt.where(Incident.severity == severity)
    stmt = stmt.order_by(Incident.created_at.desc(), Incident.id.desc())
    stmt = stmt.limit(limit).offset(offset)
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


def delete_incident(
    session: Session,
    incident_id: int,
    *,
    commit: bool = False,
) -> bool:
    """Delete the incident with ``incident_id``.

    Returns ``True`` if a row was deleted, ``False`` if none existed.
    """
    incident = session.get(Incident, incident_id)
    if incident is None:
        return False
    session.delete(incident)
    if commit:
        session.commit()
    else:
        session.flush()
    return True


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
# Feedback (votes) — thin; extended in C10
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
# SuggestionScore (learned aggregate) — thin; extended in C10/C11
# --------------------------------------------------------------------------- #
def upsert_suggestion_score(
    session: Session,
    *,
    query_pattern: str,
    incident_id: int,
    helpful: bool,
    commit: bool = False,
) -> SuggestionScore:
    """Increment the helpful / unhelpful tally for a ``(pattern, incident)`` pair.

    Creates the aggregate row on first vote, otherwise bumps the relevant counter
    and refreshes ``updated_at``.
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
    else:
        if helpful:
            score.helpful_count += 1
        else:
            score.unhelpful_count += 1
        score.updated_at = func.now()
    if commit:
        session.commit()
        session.refresh(score)
    else:
        session.flush()
    return score


def get_suggestion_scores(
    session: Session, query_pattern: str
) -> list[SuggestionScore]:
    """Return all learned aggregate rows for a given ``query_pattern``."""
    stmt = select(SuggestionScore).where(
        SuggestionScore.query_pattern == query_pattern
    )
    return list(session.scalars(stmt).all())
