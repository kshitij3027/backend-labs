"""Semantic retrieval layer (pgvector K-NN over the incident corpus).

This is the *candidate-generation* stage of the recommendation pipeline. Given a
query embedding it returns the top-K most semantically-similar incidents using
pgvector's **cosine** operator (``embedding <=> :query_vec``), which the HNSW
``vector_cosine_ops`` index accelerates. Optionally it hard pre-filters on
``service`` / ``severity`` before the K-NN so a caller can constrain the search
space.

Scope (C6)
----------
Retrieval + a single **semantic** score per candidate only. There is deliberately
**no** contextual scoring, no signal blending, no feedback re-ranking and no HTTP
endpoint here — those arrive in C7 (contextual), C8 (blend) and C9 (endpoint).
The one thing later stages build on is :class:`Candidate` and :func:`semantic_search`.

Score convention
----------------
The DB returns cosine *distance* (nearest-first, ascending). We expose the
similarity ``semantic = 1 - distance``: because every stored / query vector is
L2-normalised (see :mod:`src.embeddings`), ``1 - cosine_distance`` equals the
cosine similarity of the unit vectors, in ``[-1, 1]`` (and ``~[0, 1]`` for the
non-antipodal, MiniLM-style vectors we deal with). Higher = more similar.
Candidates are returned sorted by ``semantic`` descending (== distance ascending),
so ``candidates[0]`` is always the best match.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
from sqlalchemy.orm import Session

from src import embeddings, observability
from src.config import get_settings
from src.db import repository
from src.db.models import Incident

logger = observability.get_logger(__name__)


@dataclass(frozen=True)
class Candidate:
    """One retrieved incident plus its semantic similarity to the query.

    ``semantic`` is the cosine similarity (``1 - cosine_distance``) of the query
    embedding to this incident's embedding; higher = more similar. All other
    fields mirror the source :class:`~src.db.models.Incident` so downstream
    scoring / serialisation stages (C7+) never have to re-load the row.
    """

    incident_id: int
    title: str
    description: str
    service: str
    severity: str
    tags: list[str]
    resolution: str
    created_at: datetime
    semantic: float

    @classmethod
    def from_incident(cls, incident: Incident, *, semantic: float) -> "Candidate":
        """Build a :class:`Candidate` from an ORM incident and a semantic score."""
        return cls(
            incident_id=incident.id,
            title=incident.title,
            description=incident.description,
            service=incident.service,
            severity=incident.severity,
            # Copy the list so the candidate never aliases the ORM row's attribute.
            tags=list(incident.tags or []),
            resolution=incident.resolution,
            created_at=incident.created_at,
            semantic=semantic,
        )


def retrieve_candidates(
    session: Session,
    query_vec: "np.ndarray | list[float]",
    *,
    k: int | None = None,
    service: str | None = None,
    severities: list[str] | None = None,
) -> list[Candidate]:
    """Return the top-``k`` incidents most similar to ``query_vec``.

    Runs a pgvector cosine K-NN via
    :func:`src.db.repository.knn_by_embedding` (``embedding <=> :query_vec``,
    HNSW-accelerated), then shapes each ``(incident, distance)`` row into a
    :class:`Candidate` with ``semantic = 1 - distance``.

    Parameters
    ----------
    query_vec:
        The 384-dim query embedding (NumPy array or float list). Must match the
        configured embedding dimensionality or Postgres raises a mismatch error.
    k:
        Number of candidates to retrieve. Defaults to ``settings.candidate_k``
        (the wide candidate pool later stages re-rank down to ``top_k``).
    service:
        Optional exact-match ``service`` pre-filter. Applied only when provided.
    severities:
        Optional ``severity IN (...)`` pre-filter. Applied only when provided and
        non-empty.

    Returns
    -------
    list[Candidate]
        Candidates sorted by ``semantic`` **descending** (best match first).
        NULL-embedding incidents are never included (they cannot be scored). At
        most ``k`` items.
    """
    candidate_k = k if k is not None else get_settings().candidate_k

    rows = repository.knn_by_embedding(
        session,
        query_vec,
        k=candidate_k,
        service=service,
        severities=severities,
    )

    # ``rows`` already arrives ordered by distance ascending (== semantic
    # descending) from the SQL ``ORDER BY``; mapping preserves that order, so the
    # result is best-match-first without an extra sort. semantic = 1 - distance.
    candidates = [
        Candidate.from_incident(incident, semantic=1.0 - distance)
        for incident, distance in rows
    ]

    logger.debug(
        "semantic retrieval",
        k=candidate_k,
        service=service,
        severities=severities,
        returned=len(candidates),
        top_semantic=candidates[0].semantic if candidates else None,
    )
    return candidates


def semantic_search(
    session: Session,
    *,
    title: str,
    description: str,
    tags: list[str] | None = None,
    k: int | None = None,
    service: str | None = None,
    severities: list[str] | None = None,
) -> list[Candidate]:
    """Embed a query incident and retrieve its most semantically-similar matches.

    Convenience wrapper over :func:`src.embeddings.embed_query` +
    :func:`retrieve_candidates` — the query entry point later commits (C9) build
    the endpoint on. Uses the *same* canonical document text
    (:func:`src.embeddings.build_incident_text`) as corpus incidents, so the query
    vector is directly comparable to the stored ones.

    ``k`` / ``service`` / ``severities`` behave exactly as in
    :func:`retrieve_candidates`. Returns candidates sorted most-similar-first.
    """
    query_vec = embeddings.embed_query(title, description, tags)
    return retrieve_candidates(
        session,
        query_vec,
        k=k,
        service=service,
        severities=severities,
    )


__all__ = ["Candidate", "retrieve_candidates", "semantic_search"]
