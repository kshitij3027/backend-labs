"""Incident-corpus routes — the historical knowledge base a new incident is
matched against.

* ``POST   /incidents`` — add one resolved incident to the corpus. From C5 the
  MiniLM embedding is computed on ingest and persisted with the row, so
  ``has_embedding`` on the returned payload is ``True`` and the incident is
  immediately searchable.
* ``GET    /incidents`` — list the corpus, paginated and optionally filtered by
  ``service`` / ``severity``. ``total`` is the full match count (ignoring
  pagination) so a UI can page through it.
* ``GET    /incidents/{incident_id}`` — fetch one incident, ``404`` if absent.
* ``PUT    /incidents/{incident_id}`` — partial-update one incident (C22); a change
  to a *text* field (title/description/tags) re-embeds the row so semantic retrieval
  reflects the new text, and ``404`` if absent.
* ``DELETE /incidents/{incident_id}`` — remove one incident (C22), first cleaning up
  its dependent feedback / suggestion-score rows; ``204`` on success, ``404`` if absent.

Cache invalidation (C22): every corpus mutation (create / update / delete) bumps the
global **corpus epoch** in Redis, which is folded into the recommendation cache key —
so a mutated corpus invalidates all cached recommendations and the next ``/recommend``
recomputes against the change. The bump is best-effort (Redis down never fails the
mutation — the durable DB write is what matters).

All routes go through the repository helpers over a request-scoped
:class:`~sqlalchemy.orm.Session` (``Depends(get_db)``); the router never builds
raw queries.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from src import embeddings, observability, services
from src.clients import redis as redis_client
from src.db import repository
from src.db.session import get_db
from src.schemas import (
    SEVERITIES,
    IncidentCreate,
    IncidentList,
    IncidentOut,
    IncidentUpdate,
)

logger = observability.get_logger(__name__)

router = APIRouter(prefix="/incidents", tags=["incidents"])


@router.post(
    "",
    response_model=IncidentOut,
    status_code=status.HTTP_201_CREATED,
    summary="Add a resolved incident to the historical corpus",
)
def create_incident(
    body: IncidentCreate,
    db: Session = Depends(get_db),
) -> IncidentOut:
    """Persist one resolved incident (with its embedding) and return it.

    From C5 the MiniLM embedding is computed on ingest from the incident's
    ``title`` / ``description`` / ``tags`` (via the same
    :func:`src.embeddings.build_incident_text` used for queries, so corpus and
    query vectors are comparable) and stored alongside the row — the returned
    ``has_embedding`` is therefore ``True``.

    Invalid input (blank title/description/resolution, or a ``severity`` outside
    the canonical set) is rejected at the schema boundary with HTTP 422. If the
    embedding service is unavailable the request fails loudly with HTTP 503
    rather than silently persisting a NULL-embedded (unsearchable) row — full
    graceful degradation is deferred to C21.
    """
    try:
        embedding = embeddings.embed_incident(
            body.title, body.description, body.tags
        )
    except Exception as exc:  # noqa: BLE001 - surface as a clear 503
        logger.error("embedding computation failed on ingest", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="embedding service unavailable",
        ) from exc

    incident = repository.add_incident(
        db,
        title=body.title,
        description=body.description,
        service=body.service,
        severity=body.severity,
        tags=body.tags,
        resolution=body.resolution,
        embedding=embedding,  # C5: MiniLM vector computed on ingest.
        commit=True,
    )
    # C22: a new incident changes the corpus, so invalidate cached recommendations
    # (best-effort — a Redis failure must not undo the persisted create).
    redis_client.bump_corpus_epoch()
    return IncidentOut.from_orm_incident(incident)


@router.get(
    "",
    response_model=IncidentList,
    summary="List / filter the historical incident corpus",
)
def list_incidents(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: str | None = Query(
        default=None, description="Exact-match filter on the incident's service."
    ),
    severity: str | None = Query(
        default=None,
        description=(
            "Exact-match filter on severity; one of " + ", ".join(SEVERITIES) + "."
        ),
    ),
    q: str | None = Query(
        default=None,
        description=(
            "Case-insensitive substring search (ILIKE) over the incident's title "
            "and description."
        ),
    ),
    tags: list[str] | None = Query(
        default=None,
        description=(
            "Repeatable tag filter (e.g. ?tags=db&tags=timeout). Matches incidents "
            "whose tags array overlaps ANY of the requested tags."
        ),
    ),
    db: Session = Depends(get_db),
) -> IncidentList:
    """Return a page of incidents (newest-first) plus the total match count.

    ``limit`` (1–200) and ``offset`` paginate. All filters are optional and additive
    (ANDed): ``service`` / ``severity`` are exact-match, ``q`` is a case-insensitive
    substring over title+description (ILIKE), and ``tags`` matches any incident whose
    ``tags`` array **overlaps** the requested tags (repeat the param for several
    tags). ``total`` counts every matching row (ignoring pagination) so the caller can
    page through the full filtered result set.
    """
    rows = repository.list_incidents(
        db,
        limit=limit,
        offset=offset,
        service=service,
        severity=severity,
        q=q,
        tags=tags,
    )
    total = repository.count_incidents(
        db, service=service, severity=severity, q=q, tags=tags
    )
    items = [IncidentOut.from_orm_incident(r) for r in rows]
    return IncidentList(items=items, total=total, limit=limit, offset=offset)


@router.get(
    "/{incident_id}",
    response_model=IncidentOut,
    summary="Fetch a single incident by id",
)
def get_incident(
    incident_id: int,
    db: Session = Depends(get_db),
) -> IncidentOut:
    """Return the incident with ``incident_id``, or HTTP 404 if it does not exist."""
    incident = repository.get_incident(db, incident_id)
    if incident is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"incident {incident_id} not found",
        )
    return IncidentOut.from_orm_incident(incident)


# Text fields that feed ``build_incident_text`` and therefore the stored embedding.
# A ``PUT`` that changes any of these must re-embed the row; a change limited to
# ``service`` / ``severity`` / ``resolution`` does not affect the document text and so
# skips the (relatively expensive) re-embed.
_EMBEDDING_TEXT_FIELDS = frozenset({"title", "description", "tags"})


@router.put(
    "/{incident_id}",
    response_model=IncidentOut,
    summary="Update an incident (re-embeds when its text changes)",
)
def update_incident(
    incident_id: int,
    body: IncidentUpdate,
    db: Session = Depends(get_db),
) -> IncidentOut:
    """Partial-update the incident with ``incident_id`` and return it (HTTP 404 if absent).

    Only the fields supplied in the body are applied (a merge over the stored row);
    each is validated at the schema boundary (non-blank free text, a known
    ``severity``). Unknown fields are rejected with 422.

    Re-embedding policy
    -------------------
    The stored MiniLM vector is derived (via
    :func:`src.embeddings.build_incident_text`) only from ``title`` / ``description`` /
    ``tags``. So the row is **re-embedded only when one of those text fields is
    changed**; a pure ``service`` / ``severity`` / ``resolution`` edit leaves the
    document text (and thus the vector) unchanged and skips the re-embed. If re-embedding
    fails (embedding service down) the request fails with HTTP 503 and the transaction
    is rolled back — the incident is *not* left half-updated with a stale vector.

    On success the change is committed and the global **corpus epoch** is bumped
    (best-effort) so every cached recommendation is invalidated and the next
    ``/recommend`` reflects the edit.
    """
    incident = repository.get_incident(db, incident_id)
    if incident is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"incident {incident_id} not found",
        )

    # Only the explicitly-supplied fields (None means "leave unchanged" is NOT how we
    # treat it — an omitted field simply is not in this dict; an explicit tags=[] is).
    fields = body.model_dump(exclude_unset=True)

    # Whether any text field that feeds the embedding was actually supplied.
    needs_reembed = bool(_EMBEDDING_TEXT_FIELDS & fields.keys())

    # Apply the column changes (flush only — we commit once after any re-embed so the
    # field write + new vector land in a single transaction).
    repository.update_incident(db, incident_id, fields=fields, commit=False)

    if needs_reembed:
        # Recompute the vector from the now-updated title/description/tags. A failure
        # here rolls back the whole update (no partial write / stale vector) and maps
        # to a clean 503, mirroring the create path's contract.
        try:
            services.embed_and_store_incident(db, incident_id, commit=False)
        except Exception as exc:  # noqa: BLE001 - surface as a clear 503
            db.rollback()
            logger.error(
                "embedding recomputation failed on update",
                incident_id=incident_id,
                error=str(exc),
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="embedding service unavailable",
            ) from exc

    db.commit()
    db.refresh(incident)

    # C22: the corpus changed — invalidate cached recommendations (best-effort; a Redis
    # failure must not undo the persisted update).
    redis_client.bump_corpus_epoch()

    logger.info(
        "incident updated",
        incident_id=incident_id,
        fields=sorted(fields.keys()),
        reembedded=needs_reembed,
    )
    return IncidentOut.from_orm_incident(incident)


@router.delete(
    "/{incident_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an incident (and its dependent feedback rows)",
)
def delete_incident(
    incident_id: int,
    db: Session = Depends(get_db),
) -> Response:
    """Delete the incident with ``incident_id`` (HTTP 404 if it does not exist).

    Dependent ``Feedback`` / ``SuggestionScore`` rows (which FK the incident) are
    removed first in the same transaction so the delete satisfies the FK constraints.
    On success the change is committed, the global **corpus epoch** is bumped
    (best-effort) to invalidate cached recommendations, and **HTTP 204** (no body) is
    returned. A second delete of the same id is a 404.
    """
    deleted = repository.delete_incident(db, incident_id, commit=True)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"incident {incident_id} not found",
        )

    # C22: the corpus changed — invalidate cached recommendations (best-effort; a Redis
    # failure must not undo the persisted delete).
    redis_client.bump_corpus_epoch()

    logger.info("incident deleted", incident_id=incident_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
