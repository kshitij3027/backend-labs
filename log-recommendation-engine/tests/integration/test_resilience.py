"""Integration tests for graceful degradation / resilience (C21).

Run against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). As in ``test_recommend.py`` / ``test_feedback.py``
a ``db_session`` (transaction rolled back on teardown) is shared with the FastAPI
``get_db`` dependency so writes the ``TestClient`` makes over HTTP are visible to
direct session queries here and are undone afterwards.

What C21 delivered, and what these tests pin down
-------------------------------------------------
* **DB / pgvector failure → 503, not 500.** ``recommendation_service.recommend``
  catches a :class:`sqlalchemy.exc.SQLAlchemyError` from retrieval / persistence,
  rolls the session back, and raises ``RecommendationUnavailableError`` which the
  router maps to **HTTP 503** ``{"detail":"recommendation temporarily unavailable"}``.
  We simulate the outage by monkeypatching
  ``src.recommendation_service.retrieve_candidates`` to raise an
  :class:`~sqlalchemy.exc.OperationalError`.

* **Empty corpus → 200 count:0, NOT an error.** The no-candidate case is *not*
  caught (it does not raise), so it must stay a normal ``200`` with ``count==0`` /
  ``suggestions==[]`` — distinct from the 503 store-unreachable case.

* **Redis down → recommend still 200 (``cached:false``) and feedback still 201.**
  Every cache op is best-effort, so with Redis unreachable the pipeline still
  embeds → retrieves → ranks → persists → returns ``cached=False`` and a vote on a
  served pair still commits to Postgres. We repoint the redis client at a dead port
  (the same technique as ``test_embedding_degradation.py``) and assert the
  ``SuggestionScore`` / ``Feedback`` rows persist.

* **warmup().** ``embeddings.warmup()`` returns ``True`` and leaves the
  ``get_model`` ``lru_cache`` populated (``currsize > 0``) — the same cache
  ``/health.components.embedding_model`` inspects. This loads the real (baked)
  model in the test container.
"""

from __future__ import annotations

import uuid
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from src import embeddings
from src import recommendation_service
from src.api import create_app
from src.clients import redis as redis_client
from src.config import get_settings
from src.db import repository as repo
from src.db.models import Feedback, Incident, SuggestionScore
from src.db.session import get_db
from src.feedback import query_pattern

# A port nothing listens on → Redis connects fail fast (refused / 2s timeout).
_DEAD_REDIS_URL = "redis://127.0.0.1:6390/0"

# Fixed contextual facets so the query pattern is a real, non-catch-all bucket
# (mirrors test_feedback.py). Derived from the *request* facets, independent of the
# seeded incidents' namespaced services.
_QUERY_SERVICE = "orders-api"
_QUERY_SEVERITY = "high"
_QUERY_TAGS = ["db", "timeout"]


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


@pytest.fixture
def dead_redis(monkeypatch) -> Iterator[None]:  # noqa: ANN001
    """Repoint the redis client at an unreachable endpoint for the test's duration.

    Builds a real ``Settings`` (so every other field is valid) but overrides
    ``redis_url`` to a dead port, patches ``redis.get_settings`` to return it, and
    resets the cached client. On teardown the client is reset again so subsequent
    tests rebuild against the real ``REDIS_URL`` from the compose ``test`` service.

    Every cache helper flows through ``get_redis()`` (built from these settings), so
    this makes reads degrade to ``None`` / a miss, the epoch degrade to ``0`` and
    writes no-op — exactly the Redis-down path C21 must survive — without touching
    the recommendation-service or embedding code at all.
    """
    dead_settings = get_settings().model_copy(update={"redis_url": _DEAD_REDIS_URL})
    monkeypatch.setattr(redis_client, "get_settings", lambda: dead_settings)
    redis_client.reset_client()
    try:
        yield
    finally:
        # monkeypatch auto-undoes the get_settings patch; drop the poisoned client
        # so the next get_redis() rebuilds from the real (restored) settings.
        redis_client.reset_client()


# --------------------------------------------------------------------------- #
# Corpus seeding (a small DB-pool-timeout family with real MiniLM vectors)
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


def _family_query(suffix: str) -> dict:
    """A paraphrase of the DB-pool-timeout family with the fixed contextual facets.

    ``title`` / ``description`` are namespaced with ``suffix`` so the normalised
    query hash (hence any Redis recommendation cache key) is unique per test.
    """
    return {
        "title": f"Requests timing out waiting on a database connection ({suffix})",
        "description": (
            "During a load spike our API started returning timeouts; the Postgres "
            "connection pool ran out of free connections and callers queued until "
            f"they gave up. correlation-id {suffix}"
        ),
        "service": _QUERY_SERVICE,
        "severity": _QUERY_SEVERITY,
        "tags": _QUERY_TAGS,
    }


# --------------------------------------------------------------------------- #
# 1. DB / pgvector failure → 503 (NOT 500), with the documented detail
# --------------------------------------------------------------------------- #
def test_db_failure_returns_503_not_500(
    client: TestClient, db_session: Session, unique: str, monkeypatch
) -> None:  # noqa: ANN001
    """A ``SQLAlchemyError`` from retrieval degrades to a clean **503**.

    We patch the ``retrieve_candidates`` symbol *as bound in the recommendation
    service* to raise an :class:`~sqlalchemy.exc.OperationalError` (a real DB /
    pgvector connectivity failure). The service must catch it, roll back, and raise
    ``RecommendationUnavailableError`` → the router returns 503 with the documented
    detail, never letting the raw driver error escape as a 500.
    """

    def _boom(*_args, **_kwargs):
        # 3-arg OperationalError(statement, params, orig) — the shape SQLAlchemy uses.
        raise OperationalError("SELECT 1", {}, Exception("connection refused"))

    monkeypatch.setattr(recommendation_service, "retrieve_candidates", _boom)

    resp = client.post("/recommend", json=_family_query(unique))

    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"] == "recommendation temporarily unavailable"


# --------------------------------------------------------------------------- #
# 2. Empty corpus → 200 count:0 (NOT 503/500)
# --------------------------------------------------------------------------- #
def test_empty_corpus_returns_200_count_zero(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """With no incidents to match, ``/recommend`` returns 200 ``count==0`` —
    the no-candidate case is *not* the store-unreachable case and must not 503/500.

    Guards against pre-existing rows in the persistent DB volume: the empty-result
    contract is asserted only when the corpus is genuinely empty; a clean 200 (never
    503/500) is asserted unconditionally.
    """
    embedded = db_session.scalar(
        select(func.count())
        .select_from(Incident)
        .where(Incident.embedding.is_not(None))
    )

    resp = client.post(
        "/recommend",
        json={
            "title": f"utterly unmatchable obscure gibberish query {unique}",
            "description": f"there is deliberately no related incident here {unique}",
            "service": _QUERY_SERVICE,
            "severity": _QUERY_SEVERITY,
            "tags": _QUERY_TAGS,
        },
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body["recommendation_id"], int)
    if not embedded:
        assert body["count"] == 0
        assert body["suggestions"] == []


# --------------------------------------------------------------------------- #
# 3. Redis down (in-process) → recommend 200 cached:false + feedback 201 persists
# --------------------------------------------------------------------------- #
def test_redis_down_recommend_and_feedback_still_work(
    client: TestClient, db_session: Session, unique: str, dead_redis
) -> None:  # noqa: ANN001
    """With Redis unreachable, the full recommend → feedback loop still works.

    * ``POST /recommend`` → **200**, ``cached is False``, real suggestions present
      (the cache read degrades to a miss, the epoch to 0, the write no-ops — the
      pipeline recomputes and serves).
    * ``POST /feedback`` on a served pair → **201**, and the ``SuggestionScore`` +
      ``Feedback`` rows persist in Postgres (queried directly). The best-effort
      epoch bump silently no-ops; the vote is committed regardless.

    No exception surfaces at any point despite Redis being down.
    """
    _seed(db_session, unique)

    rec_resp = client.post("/recommend", json=_family_query(unique))
    assert rec_resp.status_code == 200, rec_resp.text
    body = rec_resp.json()
    assert body["cached"] is False
    assert body["count"] > 0, "expected real suggestions with Redis down"
    assert len(body["suggestions"]) == body["count"]

    rec_id = body["recommendation_id"]
    incident_id = body["suggestions"][0]["incident_id"]

    fb_resp = client.post(
        "/feedback",
        json={
            "recommendation_id": rec_id,
            "incident_id": incident_id,
            "helpful": True,
        },
    )
    assert fb_resp.status_code == 201, fb_resp.text
    fb = fb_resp.json()
    assert fb["recorded"] is True
    assert fb["helpful_count"] == 1
    assert fb["unhelpful_count"] == 0

    pattern = query_pattern(_QUERY_SERVICE, _QUERY_SEVERITY, _QUERY_TAGS)
    assert fb["query_pattern"] == pattern

    # The SuggestionScore aggregate committed to Postgres despite Redis being down.
    score = repo.get_suggestion_score(db_session, pattern, incident_id)
    assert score is not None, "SuggestionScore did not persist with Redis down"
    assert score.helpful_count == 1
    assert score.unhelpful_count == 0

    # And the raw Feedback row is there too.
    fb_rows = db_session.scalar(
        select(func.count())
        .select_from(Feedback)
        .where(
            Feedback.recommendation_id == rec_id,
            Feedback.incident_id == incident_id,
        )
    )
    assert fb_rows == 1, "raw Feedback row did not persist with Redis down"


# --------------------------------------------------------------------------- #
# 4. warmup() loads the model singleton (never raises)
# --------------------------------------------------------------------------- #
def test_warmup_loads_model_singleton() -> None:
    """``embeddings.warmup()`` returns True and populates the ``get_model`` cache.

    Loads the real (baked) MiniLM model in the test container, then asserts the same
    ``lru_cache`` that ``/health.components.embedding_model`` inspects is populated
    (``currsize > 0``). This is the in-process analogue of the container warmup E2E.
    """
    result = embeddings.warmup()

    assert result is True
    assert embeddings.get_model.cache_info().currsize > 0
