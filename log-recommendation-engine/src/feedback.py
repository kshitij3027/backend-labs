"""Feedback capture — record helpful / not-helpful votes and learn from them (C10).

An engineer who acted on a served suggestion tells us whether it *actually helped*.
This module turns that single vote into the learned signal the feedback-driven
re-ranking (C11) will fold into the blend:

    vote  ->  validate against the real served recommendation
          ->  bucket by *query pattern*
          ->  upsert the (pattern -> incident) helpful/unhelpful aggregate

Two ideas do the work:

* **Query pattern** — the *bucket* a vote is aggregated under. Two different
  recommendations that share the same contextual facets (service / severity /
  sorted tags) share a pattern, so feedback generalises across repeated but
  non-identical queries. It is derived from the recommendation's **stored facets**
  (``query_json``), never from the opaque ``query_hash`` (which also folds in the
  free-text and would fragment the buckets per wording).
* **SuggestionScore** — the per-``(pattern, incident)`` tally
  (``helpful_count`` / ``unhelpful_count``), upserted on every vote. This is what
  C11 reads (via :func:`src.db.repository.get_suggestion_scores`) to boost or
  dampen a suggestion for the current query pattern.

Validation is strict on purpose (see :func:`record_feedback`): a vote is only
accepted against a *real prior* :class:`~src.db.models.Recommendation` **and** an
``incident_id`` that recommendation actually returned (its
``query_json["suggestion_ids"]``). This guarantees the learned aggregate can never
be poisoned by a vote on a suggestion that was never served.

This commit records and aggregates only — **no ranking changes** (that is C11).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from src import observability
from src.db import repository
from src.db.models import Feedback, SuggestionScore

logger = observability.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Errors — the router maps these to HTTP status codes
# --------------------------------------------------------------------------- #
class FeedbackError(Exception):
    """Base class for feedback-domain errors (mapped to HTTP by the router)."""


class RecommendationNotFoundError(FeedbackError):
    """The referenced ``recommendation_id`` does not exist -> the router returns 404."""

    def __init__(self, recommendation_id: int) -> None:
        self.recommendation_id = recommendation_id
        super().__init__(f"recommendation {recommendation_id} not found")


class SuggestionNotInRecommendationError(FeedbackError):
    """Voted ``incident_id`` was not a suggestion of that recommendation -> 400.

    A vote is only valid against a suggestion the recommendation actually returned
    (its ``query_json["suggestion_ids"]``); anything else is rejected so the learned
    aggregate stays trustworthy.
    """

    def __init__(self, incident_id: int, recommendation_id: int) -> None:
        self.incident_id = incident_id
        self.recommendation_id = recommendation_id
        super().__init__(
            f"incident {incident_id} was not a suggestion of "
            f"recommendation {recommendation_id}"
        )


# --------------------------------------------------------------------------- #
# Query-pattern bucket key
# --------------------------------------------------------------------------- #
def query_pattern(
    service: str | None,
    severity: str | None,
    tags: list[str] | None,
) -> str:
    """Return the normalized ``"service|severity|tags"`` aggregation bucket key.

    This is the key feedback is grouped by, so votes on semantically-similar
    queries (same contextual facets, possibly different wording) accumulate in the
    same ``(pattern -> incident)`` :class:`~src.db.models.SuggestionScore` row.

    Normalisation (mirrors the query-hash facet normalisation in
    :mod:`src.recommendation_service`, minus the free-text):

    * ``service`` / ``severity`` — stripped and lower-cased; ``None`` / blank -> ``""``.
    * ``tags`` — each stripped + lower-cased, blanks dropped, **de-duplicated**,
      **sorted**, then comma-joined (so tag order never changes the bucket).

    The three parts are joined with ``"|"``. When every facet is empty the key is
    the catch-all ``"||"`` bucket — acceptable, but coarse: all facet-less queries
    then share one aggregate. The dashboard / API therefore encourage passing
    ``service`` / ``severity`` / ``tags`` so feedback is bucketed meaningfully.
    """
    svc = (service or "").strip().lower()
    sev = (severity or "").strip().lower()
    cleaned_tags = sorted(
        {t.strip().lower() for t in (tags or []) if t and t.strip()}
    )
    tags_part = ",".join(cleaned_tags)
    return f"{svc}|{sev}|{tags_part}"


def _pattern_from_recommendation(query_json: dict[str, Any]) -> str:
    """Derive the query pattern from a recommendation's stored facets.

    Reads ``service`` / ``severity`` / ``tags`` out of the persisted ``query_json``
    (the recommendation's normalised query) — **not** the opaque ``query_hash`` —
    and buckets them via :func:`query_pattern`.
    """
    return query_pattern(
        query_json.get("service"),
        query_json.get("severity"),
        query_json.get("tags"),
    )


# --------------------------------------------------------------------------- #
# Record a vote (validate -> insert -> upsert aggregate)
# --------------------------------------------------------------------------- #
def record_feedback(
    session: Session,
    *,
    recommendation_id: int,
    incident_id: int,
    helpful: bool,
    commit: bool = False,
) -> tuple[Feedback, SuggestionScore]:
    """Record one vote and fold it into the learned aggregate.

    Steps:

    1. Load the :class:`~src.db.models.Recommendation`; raise
       :class:`RecommendationNotFoundError` (-> 404) if it does not exist.
    2. Validate ``incident_id`` is in that recommendation's
       ``query_json["suggestion_ids"]``; else raise
       :class:`SuggestionNotInRecommendationError` (-> 400) — a vote is only valid
       against a suggestion actually served for this recommendation.
    3. Derive the ``query_pattern`` from the recommendation's stored facets.
    4. Insert a :class:`~src.db.models.Feedback` row (the raw event).
    5. Upsert the ``(pattern, incident_id)``
       :class:`~src.db.models.SuggestionScore`, incrementing ``helpful_count`` (or
       ``unhelpful_count``) and bumping ``updated_at``.

    Returns the ``(feedback_row, suggestion_score_row)`` pair. The ``SuggestionScore``
    reflects the *post-update* tally for that ``(pattern, incident)``. Commits before
    returning iff ``commit`` is true (both writes land in one transaction).

    Note: the raw ``Feedback`` row is inserted even for a repeat vote — the aggregate
    counts every event, so N helpful votes on the same suggestion yield
    ``helpful_count == N`` (there is intentionally no per-user de-duplication in
    this commit; the corpus of votes *is* the signal).
    """
    recommendation = repository.get_recommendation(session, recommendation_id)
    if recommendation is None:
        raise RecommendationNotFoundError(recommendation_id)

    query_json = recommendation.query_json or {}
    suggestion_ids = query_json.get("suggestion_ids") or []
    if incident_id not in suggestion_ids:
        raise SuggestionNotInRecommendationError(incident_id, recommendation_id)

    pattern = _pattern_from_recommendation(query_json)

    feedback = repository.add_feedback(
        session,
        recommendation_id=recommendation_id,
        incident_id=incident_id,
        query_pattern=pattern,
        helpful=helpful,
        commit=False,
    )
    score = repository.upsert_suggestion_score(
        session,
        query_pattern=pattern,
        incident_id=incident_id,
        helpful=helpful,
        commit=commit,
    )

    logger.info(
        "feedback recorded",
        recommendation_id=recommendation_id,
        incident_id=incident_id,
        query_pattern=pattern,
        helpful=helpful,
        helpful_count=score.helpful_count,
        unhelpful_count=score.unhelpful_count,
    )

    return feedback, score


__all__ = [
    "query_pattern",
    "record_feedback",
    "FeedbackError",
    "RecommendationNotFoundError",
    "SuggestionNotInRecommendationError",
]
