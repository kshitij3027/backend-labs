"""Incident-corpus routes — the historical knowledge base a new incident is
matched against.

* ``POST /incidents`` — add one resolved incident to the corpus. From C5 the
  MiniLM embedding is computed on ingest and persisted with the row, so
  ``has_embedding`` on the returned payload is ``True`` and the incident is
  immediately searchable.
* ``GET  /incidents`` — list the corpus, paginated and optionally filtered by
  ``service`` / ``severity``. ``total`` is the full match count (ignoring
  pagination) so a UI can page through it.
* ``GET  /incidents/{incident_id}`` — fetch one incident, ``404`` if absent.

All routes go through the repository helpers over a request-scoped
:class:`~sqlalchemy.orm.Session` (``Depends(get_db)``); the router never builds
raw queries.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src import embeddings, observability
from src.db import repository
from src.db.session import get_db
from src.schemas import SEVERITIES, IncidentCreate, IncidentList, IncidentOut

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
    db: Session = Depends(get_db),
) -> IncidentList:
    """Return a page of incidents (newest-first) plus the total match count.

    ``limit`` (1–200) and ``offset`` paginate; ``service`` / ``severity`` (when
    given) are exact-match filters. ``total`` counts every matching row so the
    caller can page through the full result set.
    """
    rows = repository.list_incidents(
        db,
        limit=limit,
        offset=offset,
        service=service,
        severity=severity,
    )
    total = repository.count_incidents(db, service=service, severity=severity)
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
