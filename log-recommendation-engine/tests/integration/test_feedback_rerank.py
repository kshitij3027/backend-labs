"""Integration test for feedback-driven re-ranking + epoch cache invalidation (C11).

Runs against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). As in ``test_recommend.py`` / ``test_feedback.py``,
the FastAPI ``get_db`` dependency is overridden to yield the rolled-back
``db_session`` so HTTP writes are visible to direct queries and undone on teardown.
The **Redis** side (recommendation cache + per-pattern feedback epoch) is NOT
transactional — it is a live store — which is exactly why the epoch-invalidation
behaviour can be exercised end-to-end here.

What this proves (C11)
----------------------
1. ``POST /recommend`` with explicit ``service`` / ``severity`` / ``tags`` (a real
   non-``||`` pattern) over a tightly-clustered incident family returns ≥3
   suggestions, and with no prior votes every suggestion's ``feedback`` term is 0.0.
2. A **decisive reorder**: take the current #2 (``X``) and #1 (``Y``). Vote ``X``
   helpful 8× and ``Y`` not-helpful 5× (all 201). Because each vote bumps the
   pattern's feedback epoch, the *identical* re-``POST /recommend`` is a cache MISS
   (``cached is False``) and re-ranks with the fresh signal: ``X.feedback > 0``,
   ``Y.feedback < 0``, **X now ranks above Y** (ideally X is #1). X's blended score
   rose vs baseline and Y's fell.
3. Bucketing sanity: a **different** pattern's recommendation is unaffected — its
   suggestions still show 0.0 feedback (feedback is bucketed per query pattern).
"""

from __future__ import annotations

import uuid
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from src import embeddings
from src.api import create_app
from src.db import repository as repo
from src.db.session import get_db
from src.feedback import net_help, query_pattern

# Explicit contextual facets on every /recommend so the derived pattern is a real,
# non-catch-all bucket. These are the *query* facets (independent of the seeded
# incidents' namespaced services — the pattern comes from the request, not the match).
_QUERY_SERVICE = "payments"
_QUERY_SEVERITY = "high"
_QUERY_TAGS = ["database", "pool", "timeout"]


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded rows + query hash are isolated."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``."""
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# Corpus: a tightly-clustered DB connection-pool-timeout family (real vectors).
# Members are deliberately near-paraphrases so their semantic scores cluster
# closely — this makes the ranking sensitive to the feedback term, so a decisive
# vote can reorder the top of the list.
# --------------------------------------------------------------------------- #
def _family_specs() -> list[dict]:
    """Five near-identical DB connection-pool-timeout incidents (distinct fixes)."""
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
            "title": "Postgres connection pool exhausted, requests time out waiting",
            "description": (
                "During peak traffic the Postgres connection pool was fully "
                "checked out; incoming queries queued for a free connection and "
                "then timed out, returning 500s to clients."
            ),
            "severity": "high",
            "tags": ["database", "timeout", "pool", "postgres"],
            "resolution": "Added PgBouncer connection pooling in transaction mode.",
        },
        {
            "title": "Postgres pool fully checked out, queries queue then time out",
            "description": (
                "At peak load every Postgres connection in the pool was in use; "
                "new queries waited for a connection to free up and eventually "
                "timed out, surfacing 500s to the caller."
            ),
            "severity": "high",
            "tags": ["database", "timeout", "pool", "postgres"],
            "resolution": "Tuned pool min/max and shed load with a circuit breaker.",
        },
        {
            "title": "Connection pool saturated so DB queries time out under load",
            "description": (
                "Heavy load saturated the Postgres connection pool; requests "
                "blocked acquiring a connection and hit their timeout, returning "
                "500 errors to upstream callers."
            ),
            "severity": "high",
            "tags": ["database", "timeout", "pool", "postgres"],
            "resolution": "Capped transaction duration and enlarged the pool.",
        },
        {
            "title": "DB connection pool exhaustion leads to query timeouts at peak",
            "description": (
                "When traffic peaked the Postgres connection pool ran dry; queries "
                "sat waiting for an available connection and then timed out, "
                "returning 500s to the API."
            ),
            "severity": "high",
            "tags": ["database", "timeout", "pool", "postgres"],
            "resolution": "Increased pool size and enabled a statement timeout.",
        },
    ]


def _seed(session: Session, suffix: str) -> list[int]:
    """Seed the clustered family with real MiniLM vectors; return the seeded ids.

    Each ``service`` is namespaced with ``suffix`` so this test's rows are exactly
    scopable regardless of what the persistent DB volume already holds.
    """
    ids: list[int] = []
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
        ids.append(inc.id)
    return ids


def _recommend(client: TestClient, suffix: str) -> dict:
    """POST /recommend with the fixed contextual facets; return the JSON body.

    ``title`` / ``description`` are namespaced with ``suffix`` so the normalised
    query hash (hence base Redis cache key) is unique per test.
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


def _vote(client: TestClient, rec_id: int, incident_id: int, helpful: bool) -> None:
    """Cast one vote and assert it was recorded (201)."""
    resp = client.post(
        "/feedback",
        json={
            "recommendation_id": rec_id,
            "incident_id": incident_id,
            "helpful": helpful,
        },
    )
    assert resp.status_code == 201, resp.text


def _by_id(suggestions: list[dict]) -> dict[int, dict]:
    return {s["incident_id"]: s for s in suggestions}


def _index_of(suggestions: list[dict], incident_id: int) -> int:
    for i, s in enumerate(suggestions):
        if s["incident_id"] == incident_id:
            return i
    raise AssertionError(f"incident {incident_id} not in suggestions")


# --------------------------------------------------------------------------- #
# The decisive re-rank: vote #2 up + #1 down → #2 overtakes #1 on the next query
# --------------------------------------------------------------------------- #
def test_feedback_reranks_voted_up_suggestion_above_voted_down(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """Vote the current #2 helpful (8×) and the current #1 not-helpful (5×); the
    identical re-``/recommend`` misses the cache (epoch bumped) and re-ranks so the
    voted-up suggestion outranks the voted-down one, with signed feedback terms."""
    seeded = set(_seed(db_session, unique))

    # --- Baseline recommendation (no votes yet) ---
    baseline = _recommend(client, unique)
    assert baseline["cached"] is False
    base_suggestions = baseline["suggestions"]
    assert len(base_suggestions) >= 3, (
        f"need >=3 suggestions to demonstrate a reorder, got {len(base_suggestions)}"
    )
    rec_id = baseline["recommendation_id"]

    # Every baseline suggestion should have a neutral (0.0) feedback term.
    for s in base_suggestions:
        assert s["feedback"] == 0.0, f"baseline feedback not 0.0 on {s['incident_id']}"
        assert s["breakdown"]["feedback"] == 0.0

    # Y = current #1 (to be voted DOWN), X = current #2 (to be voted UP).
    y = base_suggestions[0]["incident_id"]  # currently ranked first
    x = base_suggestions[1]["incident_id"]  # currently ranked second
    assert x in seeded and y in seeded
    base_by_id = _by_id(base_suggestions)
    x_base_score = base_by_id[x]["score"]
    y_base_score = base_by_id[y]["score"]
    # The top two feedback terms are 0.0 (no votes yet) — sanity from the spec.
    assert base_by_id[x]["feedback"] == 0.0
    assert base_by_id[y]["feedback"] == 0.0

    # --- Cast the decisive votes: X helpful 8×, Y not-helpful 5× (all 201) ---
    for _ in range(8):
        _vote(client, rec_id, x, helpful=True)
    for _ in range(5):
        _vote(client, rec_id, y, helpful=False)

    # The learned aggregate now reflects the votes (visible via the shared session).
    pattern = query_pattern(_QUERY_SERVICE, _QUERY_SEVERITY, _QUERY_TAGS)
    x_score_row = repo.get_suggestion_score(db_session, pattern, x)
    y_score_row = repo.get_suggestion_score(db_session, pattern, y)
    assert x_score_row is not None and (x_score_row.helpful_count, x_score_row.unhelpful_count) == (8, 0)
    assert y_score_row is not None and (y_score_row.helpful_count, y_score_row.unhelpful_count) == (0, 5)
    # Expected net-help terms folded into the blend (smoothing default 2.0):
    #   X: (8-0)/(8+0+2) = 0.8 ;  Y: (0-5)/(0+5+2) = -5/7 ≈ -0.714
    assert net_help(8, 0) == pytest.approx(0.8)
    assert net_help(0, 5) == pytest.approx(-5 / 7)

    # --- Identical re-recommend → cache MISS (epoch bumped) + re-rank ---
    after = _recommend(client, unique)
    assert after["cached"] is False, (
        "post-vote identical /recommend must MISS the cache (feedback epoch bumped)"
    )
    after_suggestions = after["suggestions"]
    after_by_id = _by_id(after_suggestions)
    assert x in after_by_id and y in after_by_id

    # Signed feedback terms surfaced on the re-ranked suggestions.
    assert after_by_id[x]["feedback"] > 0, "voted-up X should carry positive feedback"
    assert after_by_id[y]["feedback"] < 0, "voted-down Y should carry negative feedback"
    assert after_by_id[x]["feedback"] == pytest.approx(0.8)
    assert after_by_id[y]["feedback"] == pytest.approx(-5 / 7)

    # THE decisive assertion: X now ranks above Y.
    x_idx = _index_of(after_suggestions, x)
    y_idx = _index_of(after_suggestions, y)
    assert x_idx < y_idx, (
        f"expected voted-up X (id={x}) to outrank voted-down Y (id={y}); "
        f"got order {[s['incident_id'] for s in after_suggestions]}"
    )
    # Ideally the voted-up suggestion is now #1.
    assert x_idx == 0, (
        f"expected voted-up X (id={x}) to be #1; order "
        f"{[s['incident_id'] for s in after_suggestions]}"
    )

    # Scores moved the expected direction: X up, Y down vs baseline.
    assert after_by_id[x]["score"] > x_base_score, (
        f"X score should rise: {x_base_score} -> {after_by_id[x]['score']}"
    )
    assert after_by_id[y]["score"] < y_base_score, (
        f"Y score should fall: {y_base_score} -> {after_by_id[y]['score']}"
    )


# --------------------------------------------------------------------------- #
# Bucketing sanity: a DIFFERENT pattern's recommendation is unaffected
# --------------------------------------------------------------------------- #
def test_feedback_is_bucketed_per_pattern(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """Votes under one query pattern must not leak into a different pattern's
    recommendation — a query with different facets still shows 0.0 feedback terms."""
    _seed(db_session, unique)

    # Vote heavily under the primary pattern.
    primary = _recommend(client, unique)
    rec_id = primary["recommendation_id"]
    voted_incident = primary["suggestions"][0]["incident_id"]
    for _ in range(6):
        _vote(client, rec_id, voted_incident, helpful=True)

    # A recommendation under a DIFFERENT pattern (different service/severity/tags).
    other = client.post(
        "/recommend",
        json={
            "title": (
                f"Requests timing out waiting on a database connection ({unique})"
            ),
            "description": (
                "During a load spike our API started returning timeouts; the "
                "Postgres connection pool ran out of free connections and callers "
                f"queued until they gave up. correlation-id {unique}"
            ),
            # Different facets → a different query pattern bucket.
            "service": "billing",
            "severity": "low",
            "tags": ["cache", "latency"],
        },
    )
    assert other.status_code == 200, other.text
    other_body = other.json()
    assert other_body["count"] > 0

    # None of the other-pattern suggestions carry the primary pattern's feedback.
    for s in other_body["suggestions"]:
        assert s["feedback"] == 0.0, (
            f"feedback leaked across patterns onto {s['incident_id']}: {s['feedback']}"
        )
        assert s["breakdown"]["feedback"] == 0.0

    # Confirm the two patterns are genuinely distinct buckets.
    assert query_pattern(_QUERY_SERVICE, _QUERY_SEVERITY, _QUERY_TAGS) != query_pattern(
        "billing", "low", ["cache", "latency"]
    )
