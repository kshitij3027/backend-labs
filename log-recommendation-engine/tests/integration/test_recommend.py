"""Integration tests for the core ``POST /recommend`` endpoint (C9).

Run against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). The ``db_session`` fixture (see ``conftest.py``)
gives a transaction that is rolled back on teardown, and the FastAPI ``get_db``
dependency is overridden to yield that same session so writes the ``TestClient``
makes over HTTP are visible here and are undone afterwards.

A tiny, semantically-coherent corpus is seeded **inside each test** with genuine
MiniLM vectors (``embeddings.embed_incident``): ~4 incidents of one DB
connection-pool-timeout family (distinct resolutions) plus ~4 clearly-unrelated
incidents, so real cosine distances flow through pgvector and the top suggestion
is deterministically from the seeded family.

Query text is namespaced with a per-test unique suffix so the Redis
recommendation cache key (a hash of the normalised query) never collides with a
prior test run's entry — the cache is a persistent, non-rolled-back store, so
deterministic cache assertions require a fresh key per test.

Coverage (C9):
  * a family paraphrase → 200, ``count>0``, ``suggestions[0]`` from the seeded
    family, every suggestion carries a non-empty ``resolution`` and a
    ``breakdown`` with the documented keys, ``recommendation_id`` present;
  * the persisted ``Recommendation`` row's ``query_json["suggestion_ids"]``
    equals the returned suggestions' ``incident_id``s;
  * an identical repeat → ``cached is True`` with the SAME ``recommendation_id``;
  * an empty corpus → 200, ``count==0``, ``suggestions==[]`` (never 500);
  * validation: unknown ``severity`` → 422; ``top_k`` of 0 / 51 → 422.
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
from src.db.models import Incident, Recommendation
from src.db.session import get_db

# The full set of per-signal keys the C9 ``breakdown`` must expose (from the
# ranker): the three blended signals, the contextual sub-signal detail, and the
# blend weights used — so a suggestion is self-explaining in the UI.
_BREAKDOWN_KEYS = {
    "semantic",
    "contextual",
    "feedback",
    "contextual_detail",
    "weights",
}


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded rows and query hash are isolated."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``.

    Overriding the dependency runs every request inside the same outer transaction
    as ``db_session``, so HTTP writes are visible to direct session queries here
    and are discarded when the fixture tears down.
    """
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# Corpus specs: ONE coherent DB connection-pool-timeout family + unrelated noise
# --------------------------------------------------------------------------- #
def _family_specs() -> list[dict]:
    """~4 incidents of the SAME DB connection-pool-timeout family (distinct fixes)."""
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
        {
            "title": "Connection pool starvation causing slow database queries",
            "description": (
                "Long-running transactions held connections open, starving the "
                "pool; other requests queued for a connection and observed high "
                "latency and occasional timeouts."
            ),
            "severity": "high",
            "tags": ["database", "pool", "starvation", "latency"],
            "resolution": "Capped transaction duration and enlarged the pool.",
        },
    ]


def _unrelated_specs() -> list[dict]:
    """~4 clearly-unrelated incidents (different families) to act as noise."""
    return [
        {
            "title": "Expired TLS certificate breaking HTTPS handshakes",
            "description": (
                "Clients could not establish HTTPS connections because the API "
                "gateway's certificate had expired overnight."
            ),
            "severity": "high",
            "tags": ["tls", "certificate", "https", "expiry"],
            "resolution": "Renewed the certificate and automated rotation.",
        },
        {
            "title": "Service killed by the OOM killer after a memory leak",
            "description": (
                "Worker resident memory grew unbounded until the Linux OOM killer "
                "terminated the container repeatedly."
            ),
            "severity": "critical",
            "tags": ["memory", "oom", "leak", "crash"],
            "resolution": "Fixed the leaked buffer and set a memory limit.",
        },
        {
            "title": "Disk full on the log volume halting writes",
            "description": (
                "The log partition reached 100% utilisation; the application could "
                "no longer write and began dropping events."
            ),
            "severity": "medium",
            "tags": ["disk", "storage", "logs", "capacity"],
            "resolution": "Rotated logs and expanded the volume.",
        },
        {
            "title": "Authentication failures after OAuth token misconfiguration",
            "description": (
                "Users were rejected at login because the OAuth client secret was "
                "rotated but never propagated to the auth service."
            ),
            "severity": "high",
            "tags": ["auth", "oauth", "login", "token"],
            "resolution": "Propagated the new secret and reloaded config.",
        },
    ]


def _seed(session: Session, suffix: str) -> tuple[set[int], set[int]]:
    """Seed the family + unrelated corpus with real MiniLM vectors.

    Every ``service`` is namespaced with ``suffix`` so this test's rows are exactly
    scopable regardless of what the persistent DB volume already holds. Returns
    ``(family_ids, unrelated_ids)``.
    """
    family_ids: set[int] = set()
    unrelated_ids: set[int] = set()

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
        family_ids.add(inc.id)

    for i, spec in enumerate(_unrelated_specs()):
        vec = embeddings.embed_incident(
            spec["title"], spec["description"], spec["tags"]
        )
        inc = repo.add_incident(
            session,
            title=spec["title"],
            description=spec["description"],
            service=f"other{i}-{suffix}",
            severity=spec["severity"],
            tags=spec["tags"],
            resolution=spec["resolution"],
            embedding=vec,
            commit=True,
        )
        unrelated_ids.add(inc.id)

    return family_ids, unrelated_ids


def _family_query(suffix: str) -> dict:
    """A paraphrase of the DB connection-pool-timeout family (no restrict flags).

    The wording is distinct from every seeded title/description (so it tests real
    semantic matching, not verbatim overlap) and namespaced with ``suffix`` so its
    normalised-query hash is unique per test → the Redis recommendation cache key
    never collides with a prior run.
    """
    return {
        "title": f"Requests timing out waiting on a database connection ({suffix})",
        "description": (
            "During a load spike our API started returning timeouts; it looks like "
            "the Postgres connection pool ran out of free connections and callers "
            f"queued until they gave up. correlation-id {suffix}"
        ),
        "tags": ["database", "pool", "timeout"],
    }


# --------------------------------------------------------------------------- #
# 1. Family query → top suggestion from the seeded family, actionable + explainable
# --------------------------------------------------------------------------- #
def test_recommend_returns_family_suggestion_with_resolution_and_breakdown(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A DB-pool-timeout paraphrase → 200; ``suggestions[0]`` is from the seeded
    family; every suggestion has a non-empty ``resolution`` and a full
    ``breakdown``; ``recommendation_id`` is present."""
    family_ids, unrelated_ids = _seed(db_session, unique)

    resp = client.post("/recommend", json=_family_query(unique))
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert isinstance(body["recommendation_id"], int)
    assert body["count"] > 0
    assert body["count"] == len(body["suggestions"])
    assert body["cached"] is False

    suggestions = body["suggestions"]
    # The single best match must be from the DB-pool-timeout family, not the noise.
    top = suggestions[0]
    assert top["incident_id"] in family_ids, (
        f"top suggestion {top['incident_id']} not in seeded family {family_ids}; "
        f"service={top['service']!r} title={top['title']!r}"
    )

    # Every suggestion is actionable (resolution) and explainable (breakdown).
    for s in suggestions:
        assert isinstance(s["resolution"], str) and s["resolution"].strip(), (
            f"empty resolution on suggestion {s['incident_id']}"
        )
        assert set(s["breakdown"].keys()) == _BREAKDOWN_KEYS, (
            f"breakdown keys {set(s['breakdown'].keys())} != {_BREAKDOWN_KEYS}"
        )
        # feedback is a 0.0 stub until C11.
        assert s["feedback"] == 0.0
        assert 0.0 <= s["semantic"] <= 1.0

    # The top match is a real semantic hit, comfortably ahead of pure noise.
    assert top["semantic"] > 0.3


# --------------------------------------------------------------------------- #
# 2. Persistence: query_json["suggestion_ids"] == returned incident_ids
# --------------------------------------------------------------------------- #
def test_recommend_persists_row_with_matching_suggestion_ids(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """The persisted ``Recommendation.query_json['suggestion_ids']`` equals the
    returned suggestions' ``incident_id``s in order."""
    _seed(db_session, unique)

    body = client.post("/recommend", json=_family_query(unique)).json()
    rec_id = body["recommendation_id"]
    returned_ids = [s["incident_id"] for s in body["suggestions"]]

    row = repo.get_recommendation(db_session, rec_id)
    assert row is not None, f"recommendation row {rec_id} not persisted"
    assert row.query_json["suggestion_ids"] == returned_ids


# --------------------------------------------------------------------------- #
# 3. Cache hit: identical repeat → cached True, SAME recommendation_id
# --------------------------------------------------------------------------- #
def test_recommend_identical_repeat_is_cache_hit(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """An identical second ``POST /recommend`` is served from the Redis cache:
    ``cached is True`` and the SAME ``recommendation_id`` as the first."""
    _seed(db_session, unique)
    query = _family_query(unique)

    first = client.post("/recommend", json=query).json()
    assert first["cached"] is False

    second = client.post("/recommend", json=query).json()
    assert second["cached"] is True
    assert second["recommendation_id"] == first["recommendation_id"]
    # The cached payload reconstructs an identical suggestion set.
    assert [s["incident_id"] for s in second["suggestions"]] == [
        s["incident_id"] for s in first["suggestions"]
    ]


# --------------------------------------------------------------------------- #
# 4. Empty corpus → 200, count 0, suggestions [] (never 500)
# --------------------------------------------------------------------------- #
def test_recommend_empty_corpus_returns_empty_not_error(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """With no embedded incidents to match, ``/recommend`` returns an empty
    (``count==0``) response, not a 500.

    Guards against any pre-existing rows in the persistent DB volume by asserting
    the empty-result contract only when the corpus is genuinely empty; otherwise
    it still asserts a clean 200 (never a 500)."""
    embedded = db_session.scalar(
        select(func.count())
        .select_from(Incident)
        .where(Incident.embedding.is_not(None))
    )

    resp = client.post(
        "/recommend",
        json={
            "title": f"nothing should match this obscure query {unique}",
            "description": f"there is deliberately no related incident {unique}",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert isinstance(body["recommendation_id"], int)
    if not embedded:
        assert body["count"] == 0
        assert body["suggestions"] == []


# --------------------------------------------------------------------------- #
# 5. Validation: bad severity / out-of-range top_k → 422
# --------------------------------------------------------------------------- #
def test_recommend_bad_severity_returns_422(client: TestClient) -> None:
    """An unknown ``severity`` is rejected at the schema boundary with 422."""
    resp = client.post(
        "/recommend",
        json={"title": "x", "description": "y", "severity": "bogus"},
    )
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize("bad_top_k", [0, 51])
def test_recommend_out_of_range_top_k_returns_422(
    client: TestClient, bad_top_k: int
) -> None:
    """``top_k`` outside the allowed 1–50 range → 422."""
    resp = client.post(
        "/recommend",
        json={"title": "x", "description": "y", "top_k": bad_top_k},
    )
    assert resp.status_code == 422, resp.text
