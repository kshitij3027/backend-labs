"""Integration tests for ``POST /feedback`` — capture + aggregate votes (C10).

Run against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). As in ``test_recommend.py``, a ``db_session``
(transaction rolled back on teardown) is shared with the FastAPI ``get_db``
dependency so writes the ``TestClient`` makes over HTTP are visible to direct
session queries here and are undone afterwards.

Flow under test
---------------
A vote must reference a *real prior served result*. So each test first seeds a tiny
coherent corpus, issues a ``POST /recommend`` **with ``service`` / ``severity`` /
``tags`` set** (so the derived query pattern is a meaningful non-catch-all bucket),
captures the returned ``recommendation_id`` + one served ``incident_id``, then votes
against that pair and asserts the response + the persisted ``Feedback`` /
``SuggestionScore`` rows.

Coverage (C10):
  * 1st helpful vote -> 201, ``helpful_count==1`` / ``unhelpful_count==0``; DB has
    exactly 1 ``Feedback`` row + 1 ``SuggestionScore`` (helpful_count=1) for
    ``(pattern, incident_id)``;
  * 2nd vote (same pair, ``helpful:false``) -> 201, ``1/1``; 2 ``Feedback`` rows,
    still 1 ``SuggestionScore`` (updated in place);
  * ``incident_id`` not among that recommendation's suggestions -> 400;
  * unknown ``recommendation_id`` -> 404;
  * malformed body (missing field / non-bool ``helpful``) -> 422;
  * the persisted ``SuggestionScore.query_pattern`` equals
    ``query_pattern(service, severity, tags)`` for the recommendation's facets.
"""

from __future__ import annotations

import uuid
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src import embeddings
from src.api import create_app
from src.db import repository as repo
from src.db.models import Feedback, SuggestionScore
from src.db.session import get_db
from src.feedback import query_pattern

# The contextual facets we attach to every /recommend request so the derived query
# pattern is a real, non-catch-all bucket (and so the SuggestionScore.query_pattern
# assertion is meaningful). These are the *query* facets — deliberately independent
# of the seeded incidents' namespaced services — because the pattern is derived from
# the recommendation's stored request facets, never from the matched incident.
_QUERY_SERVICE = "payments"
_QUERY_SEVERITY = "high"
_QUERY_TAGS = ["database", "pool", "timeout"]


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded rows and query hash are isolated."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``.

    Overriding the dependency runs every request inside the same outer transaction
    as ``db_session``, so HTTP writes are visible to direct session queries here and
    are discarded when the fixture tears down.
    """
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# Corpus: a small DB connection-pool-timeout family (real MiniLM vectors)
# --------------------------------------------------------------------------- #
def _family_specs() -> list[dict]:
    """A few incidents of the SAME DB connection-pool-timeout family."""
    return [
        {
            "title": "Database connection pool exhausted causing request timeouts",
            "description": (
                "Under peak load the Postgres connection pool was fully checked "
                "out; new queries queued waiting for a connection and then timed "
                "out, returning 500s to callers."
            ),
            "severity": "high",
            "tags": ["database", "timeout", "pool", "postgres"],
            "resolution": "Raised the max pool size and added a statement timeout.",
        },
        {
            "title": "Postgres connections saturated, checkout requests hang",
            "description": (
                "All database connections in the pool were busy; the checkout "
                "service blocked acquiring a connection and requests hung until "
                "the client-side timeout fired."
            ),
            "severity": "high",
            "tags": ["database", "connections", "pool", "hang"],
            "resolution": "Added PgBouncer connection pooling in transaction mode.",
        },
        {
            "title": "DB pool timeout under load spikes 500 error rate",
            "description": (
                "A traffic spike exhausted the connection pool; threads waited on "
                "a free database connection past the pool timeout and the error "
                "rate climbed as requests failed."
            ),
            "severity": "critical",
            "tags": ["database", "timeout", "pool", "errors"],
            "resolution": "Tuned pool min/max and shed load with a circuit breaker.",
        },
    ]


def _seed(session: Session, suffix: str) -> set[int]:
    """Seed the family corpus with real MiniLM vectors; return the seeded ids.

    Every ``service`` is namespaced with ``suffix`` so this test's rows are exactly
    scopable regardless of what the persistent DB volume already holds.
    """
    ids: set[int] = set()
    for i, spec in enumerate(_family_specs()):
        vec = embeddings.embed_incident(
            spec["title"], spec["description"], spec["tags"]
        )
        inc = repo.add_incident(
            session,
            title=spec["title"],
            description=spec["description"],
            service=f"fam{i}-{suffix}",
            severity=spec["severity"],
            tags=spec["tags"],
            resolution=spec["resolution"],
            embedding=vec,
            commit=True,
        )
        ids.add(inc.id)
    return ids


def _recommend(client: TestClient, suffix: str) -> dict:
    """POST /recommend with the fixed contextual facets; return the JSON body.

    The ``title`` / ``description`` are namespaced with ``suffix`` so the normalised
    query hash (hence the Redis recommendation cache key) is unique per test and a
    prior run's cached entry can never be reused.
    """
    resp = client.post(
        "/recommend",
        json={
            "title": (
                f"Requests timing out waiting on a database connection ({suffix})"
            ),
            "description": (
                "During a load spike our API started returning timeouts; the "
                "Postgres connection pool ran out of free connections and callers "
                f"queued until they gave up. correlation-id {suffix}"
            ),
            "service": _QUERY_SERVICE,
            "severity": _QUERY_SEVERITY,
            "tags": _QUERY_TAGS,
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _expected_pattern() -> str:
    """The bucket key the votes must aggregate under, from the fixed query facets."""
    return query_pattern(_QUERY_SERVICE, _QUERY_SEVERITY, _QUERY_TAGS)


def _count_feedback(session: Session, recommendation_id: int) -> int:
    """Number of raw ``Feedback`` rows for a recommendation (this test's votes)."""
    return int(
        session.scalar(
            select(func.count())
            .select_from(Feedback)
            .where(Feedback.recommendation_id == recommendation_id)
        )
        or 0
    )


# --------------------------------------------------------------------------- #
# 1. First helpful vote -> 201, counts (1,0), one Feedback + one SuggestionScore
# --------------------------------------------------------------------------- #
def test_feedback_first_helpful_vote_records_and_aggregates(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A single helpful vote returns 201 with ``helpful_count==1`` /
    ``unhelpful_count==0`` and persists exactly one ``Feedback`` row and one
    ``SuggestionScore`` (helpful_count=1) for ``(pattern, incident_id)``."""
    _seed(db_session, unique)
    body = _recommend(client, unique)
    rec_id = body["recommendation_id"]
    assert body["count"] > 0
    incident_id = body["suggestions"][0]["incident_id"]

    resp = client.post(
        "/feedback",
        json={
            "recommendation_id": rec_id,
            "incident_id": incident_id,
            "helpful": True,
        },
    )
    assert resp.status_code == 201, resp.text
    fb = resp.json()
    assert fb["recorded"] is True
    assert fb["incident_id"] == incident_id
    assert fb["helpful_count"] == 1
    assert fb["unhelpful_count"] == 0
    pattern = _expected_pattern()
    assert fb["query_pattern"] == pattern

    # DB: exactly one raw Feedback row for this recommendation ...
    assert _count_feedback(db_session, rec_id) == 1

    # ... and exactly one SuggestionScore aggregate for the (pattern, incident) pair.
    score = repo.get_suggestion_score(db_session, pattern, incident_id)
    assert score is not None
    assert score.helpful_count == 1
    assert score.unhelpful_count == 0
    scores = repo.get_suggestion_scores(db_session, pattern)
    assert len(scores) == 1


# --------------------------------------------------------------------------- #
# 2. Second (unhelpful) vote on same pair -> 201, counts (1,1), 2 FB, 1 score
# --------------------------------------------------------------------------- #
def test_feedback_second_vote_updates_same_aggregate(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A second vote (same pair, ``helpful:false``) returns 201 with
    ``helpful_count==1`` / ``unhelpful_count==1``; there are now two ``Feedback``
    rows but still a single ``SuggestionScore`` (updated in place, not duplicated)."""
    _seed(db_session, unique)
    body = _recommend(client, unique)
    rec_id = body["recommendation_id"]
    incident_id = body["suggestions"][0]["incident_id"]

    first = client.post(
        "/feedback",
        json={"recommendation_id": rec_id, "incident_id": incident_id, "helpful": True},
    )
    assert first.status_code == 201, first.text
    assert first.json()["helpful_count"] == 1

    second = client.post(
        "/feedback",
        json={"recommendation_id": rec_id, "incident_id": incident_id, "helpful": False},
    )
    assert second.status_code == 201, second.text
    fb = second.json()
    assert fb["helpful_count"] == 1
    assert fb["unhelpful_count"] == 1

    pattern = _expected_pattern()
    # Two raw votes, one updated aggregate.
    assert _count_feedback(db_session, rec_id) == 2
    scores = repo.get_suggestion_scores(db_session, pattern)
    assert len(scores) == 1
    assert scores[0].helpful_count == 1
    assert scores[0].unhelpful_count == 1
    assert scores[0].incident_id == incident_id


# --------------------------------------------------------------------------- #
# 3. incident_id not among that recommendation's suggestions -> 400
# --------------------------------------------------------------------------- #
def test_feedback_incident_not_in_recommendation_returns_400(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """Voting an ``incident_id`` that was NOT one of the recommendation's served
    suggestions is rejected with 400 (and writes nothing)."""
    _seed(db_session, unique)
    body = _recommend(client, unique)
    rec_id = body["recommendation_id"]
    served = {s["incident_id"] for s in body["suggestions"]}

    # Pick a positive id that is definitely not in the served set.
    bad_incident = max(served) + 10_000

    resp = client.post(
        "/feedback",
        json={
            "recommendation_id": rec_id,
            "incident_id": bad_incident,
            "helpful": True,
        },
    )
    assert resp.status_code == 400, resp.text
    # Nothing persisted for this recommendation.
    assert _count_feedback(db_session, rec_id) == 0


# --------------------------------------------------------------------------- #
# 4. Unknown recommendation_id -> 404
# --------------------------------------------------------------------------- #
def test_feedback_unknown_recommendation_returns_404(client: TestClient) -> None:
    """An unknown ``recommendation_id`` is rejected with 404."""
    resp = client.post(
        "/feedback",
        json={"recommendation_id": 999_999, "incident_id": 1, "helpful": True},
    )
    assert resp.status_code == 404, resp.text


# --------------------------------------------------------------------------- #
# 5. Malformed body -> 422
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "payload",
    [
        {"incident_id": 1, "helpful": True},  # missing recommendation_id
        {"recommendation_id": 1, "helpful": True},  # missing incident_id
        {"recommendation_id": 1, "incident_id": 1},  # missing helpful
        # A string Pydantic v2 will NOT coerce to bool ("yes"/"true"/1 *are*
        # coerced, so they would sail past the schema — use a genuine non-bool).
        {"recommendation_id": 1, "incident_id": 1, "helpful": "notabool"},  # non-bool
        {"recommendation_id": 1, "incident_id": 1, "helpful": []},  # non-bool type
        {"recommendation_id": 0, "incident_id": 1, "helpful": True},  # ge=1 violated
        {"recommendation_id": 1, "incident_id": 0, "helpful": True},  # ge=1 violated
    ],
)
def test_feedback_malformed_body_returns_422(
    client: TestClient, payload: dict
) -> None:
    """Missing / mistyped / out-of-range fields are rejected at the schema
    boundary with 422 (before any domain logic runs)."""
    resp = client.post("/feedback", json=payload)
    assert resp.status_code == 422, resp.text


# --------------------------------------------------------------------------- #
# 6. Persisted SuggestionScore.query_pattern == query_pattern(service,severity,tags)
# --------------------------------------------------------------------------- #
def test_feedback_persisted_pattern_matches_query_facets(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """The bucket the vote lands in is exactly
    ``query_pattern(service, severity, tags)`` for the recommendation's stored
    facets — verified on both the raw ``Feedback`` row and the ``SuggestionScore``."""
    _seed(db_session, unique)
    body = _recommend(client, unique)
    rec_id = body["recommendation_id"]
    incident_id = body["suggestions"][0]["incident_id"]

    resp = client.post(
        "/feedback",
        json={"recommendation_id": rec_id, "incident_id": incident_id, "helpful": True},
    )
    assert resp.status_code == 201, resp.text

    expected = query_pattern(_QUERY_SERVICE, _QUERY_SEVERITY, _QUERY_TAGS)
    assert resp.json()["query_pattern"] == expected

    # The persisted SuggestionScore is keyed by exactly that pattern.
    score = repo.get_suggestion_score(db_session, expected, incident_id)
    assert score is not None

    # And the raw Feedback row carries the same bucket string.
    fb_pattern = db_session.scalar(
        select(Feedback.query_pattern).where(
            Feedback.recommendation_id == rec_id,
            Feedback.incident_id == incident_id,
        )
    )
    assert fb_pattern == expected
