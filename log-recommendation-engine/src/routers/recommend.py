"""Recommendation route — the core deliverable (C9).

``POST /recommend`` submits a new (unresolved) incident and gets back a ranked list
of solution suggestions, each carrying the matched historical incident's
**resolution** and a per-signal score breakdown (semantic / contextual / feedback).

The endpoint is a thin adapter: it validates the request at the schema boundary and
delegates the whole pipeline — embed -> pgvector retrieve -> contextual score ->
weighted-blend rank -> persist -> cache — to
:func:`src.recommendation_service.recommend`, over a request-scoped
:class:`~sqlalchemy.orm.Session` (``Depends(get_db)``). An empty corpus returns an
empty (``count=0``) response, not an error.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from src import observability, recommendation_service
from src.db.session import get_db
from src.schemas import RecommendRequest, RecommendResponse

logger = observability.get_logger(__name__)

router = APIRouter(tags=["recommend"])


@router.post(
    "/recommend",
    response_model=RecommendResponse,
    status_code=status.HTTP_200_OK,
    summary="Submit a new incident and get ranked solution suggestions",
)
def recommend(
    body: RecommendRequest,
    db: Session = Depends(get_db),
) -> RecommendResponse:
    """Return the top-K ranked suggestions for the submitted incident.

    The submitted ``title`` / ``description`` / ``tags`` are embedded and matched
    against the historical corpus by semantic similarity, then re-ranked by a blend
    of semantic + contextual signals (``service`` / ``severity`` / ``tags`` softly
    reward similar incidents unless ``restrict_service`` / ``restrict_severity`` make
    them hard constraints). Each suggestion includes the **resolution** that fixed
    the matched incident and a score ``breakdown``.

    The served result is persisted (``recommendation_id`` in the response) so
    feedback (C10) can reference it, and cached in Redis so an identical repeated
    query returns quickly with ``cached=True``. Invalid input (blank
    title/description, an unknown ``severity``, ``top_k`` outside 1–50) is rejected
    at the schema boundary with HTTP 422.
    """
    return recommendation_service.recommend(db, body)
