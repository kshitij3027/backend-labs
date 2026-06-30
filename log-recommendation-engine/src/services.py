"""Application service helpers that compose the repository + embedding layers.

These sit above :mod:`src.db.repository` (pure CRUD) and :mod:`src.embeddings`
(pure vectorisation) and wire the two together for reuse across call sites — the
router, the seed/backfill scripts, and (C22) the incident-update endpoint. Keeping
the composition here avoids duplicating "load row -> build doc text -> embed ->
store" in every place that needs it, and avoids import cycles (routers import this;
this imports repository + embeddings, neither of which imports back).
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from src import embeddings
from src.db import repository
from src.db.models import Incident


def embed_and_store_incident(
    session: Session,
    incident_id: int,
    *,
    commit: bool = False,
) -> Incident | None:
    """(Re)compute an incident's embedding from its current fields and persist it.

    Loads the incident, builds its canonical document text via
    :func:`src.embeddings.build_incident_text` (the *same* text used to embed
    queries, so the stored vector is directly comparable), encodes it with MiniLM,
    and writes the result back through
    :func:`src.db.repository.set_incident_embedding`.

    Returns the updated :class:`~src.db.models.Incident`, or ``None`` if no
    incident with ``incident_id`` exists. Follows the repository transaction
    convention: flushes by default and commits when ``commit=True``. Reused by the
    C5 ingest/backfill paths and the C22 update endpoint (an edit to title /
    description / tags must refresh the vector).
    """
    incident = repository.get_incident(session, incident_id)
    if incident is None:
        return None

    vector = embeddings.embed_incident(
        incident.title, incident.description, incident.tags
    )
    return repository.set_incident_embedding(
        session, incident_id, vector, commit=commit
    )


__all__ = ["embed_and_store_incident"]
