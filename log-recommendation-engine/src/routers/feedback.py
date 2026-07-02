"""Feedback route — capture helpful / not-helpful votes on a suggestion (C10).

``POST /feedback`` records one engineer vote on a suggestion that a prior
``POST /recommend`` returned. The vote is validated against the real served
recommendation, bucketed by *query pattern*, and folded into the
``(query_pattern -> incident)`` learned aggregate that the feedback-driven
re-ranking (C11) will read. The response echoes that aggregate's post-update
helpful / unhelpful tallies.

The endpoint is a thin adapter: it delegates the validate -> insert -> upsert work
to :func:`src.feedback.record_feedback` over a request-scoped
:class:`~sqlalchemy.orm.Session` (``Depends(get_db)``) and maps the feedback-domain
errors to HTTP status codes:

* unknown ``recommendation_id`` -> **404**
  (:class:`~src.feedback.RecommendationNotFoundError`)
* ``incident_id`` not among that recommendation's suggestions -> **400**
  (:class:`~src.feedback.SuggestionNotInRecommendationError`)

This commit records and aggregates only — it does **not** change ranking (C11).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from src import feedback as feedback_service
from src import observability
from src.db.session import get_db
from src.schemas import FeedbackRequest, FeedbackResponse

logger = observability.get_logger(__name__)

router = APIRouter(tags=["feedback"])


@router.post(
    "/feedback",
    response_model=FeedbackResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Record a helpful / not-helpful vote on a suggestion",
)
def submit_feedback(
    body: FeedbackRequest,
    db: Session = Depends(get_db),
) -> FeedbackResponse:
    """Record one vote and return the post-update aggregate for the voted pair.

    The vote must reference a real prior served result: ``recommendation_id`` (the
    recommendation ``POST /recommend`` returned) and an ``incident_id`` that was one
    of *that recommendation's* suggestions. The vote is bucketed by the query pattern
    derived from the recommendation's stored ``service`` / ``severity`` / ``tags``
    facets and merged into the ``(pattern -> incident)`` learned aggregate.

    Error mapping:

    * unknown ``recommendation_id`` -> **404**;
    * ``incident_id`` not among that recommendation's suggestions -> **400**;
    * malformed body (missing / non-positive ids, non-boolean ``helpful``) is
      rejected at the schema boundary with **422**.
    """
    try:
        _fb, score = feedback_service.record_feedback(
            db,
            recommendation_id=body.recommendation_id,
            incident_id=body.incident_id,
            helpful=body.helpful,
            commit=True,
        )
    except feedback_service.RecommendationNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except feedback_service.SuggestionNotInRecommendationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return FeedbackResponse(
        recorded=True,
        query_pattern=score.query_pattern,
        incident_id=score.incident_id,
        helpful_count=score.helpful_count,
        unhelpful_count=score.unhelpful_count,
    )
