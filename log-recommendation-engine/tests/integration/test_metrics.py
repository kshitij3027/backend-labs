"""Integration tests for the Prometheus metrics surface (C14).

Run against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). As in ``test_recommend.py`` / ``test_feedback.py``,
a ``db_session`` (transaction rolled back on teardown) is shared with the FastAPI
``get_db`` dependency so writes the ``TestClient`` makes over HTTP are visible here
and are undone afterwards.

The metric collectors are **module-level singletons on a private CollectorRegistry**
(see ``src.observability``): a single registration for the whole process, shared by
every ``create_app()`` the suite builds. That means:

* This whole suite instantiating ``create_app()`` dozens of times must NOT raise
  ``Duplicated timeseries in CollectorRegistry`` — if it did, the suite would fail at
  import/collection, not here. So its mere green run is the duplicate-registration
  guard.
* Counters are process-global and monotonic, so every assertion below reads a
  **before** snapshot and asserts a **delta** (never an absolute value) — other tests
  in the session also move these counters.

Query text is namespaced with a per-test unique suffix so the Redis recommendation
cache key (a hash of the normalised query, scoped by config version + feedback epoch)
never collides with a prior run's entry — required so the FIRST ``/recommend`` in the
cache-hit test is a genuine MISS and the SECOND is the first HIT.

Coverage (C14):
  * ``GET /metrics`` -> 200, ``content-type`` starts with ``text/plain``, body contains
    all 8 metric names;
  * ``POST /recommend`` bumps ``recommend_requests_total`` by 1 and
    ``recommend_latency_seconds_count`` by >=1 (latency observed);
  * an identical 2nd ``POST /recommend`` increments
    ``cache_hits_total{cache="recommendation"}`` (the first was a miss);
  * a valid ``POST /feedback`` increments ``feedback_total{helpful="true"}``;
  * ``GET /metrics/json`` -> 200 JSON with the documented keys.
"""

from __future__ import annotations

import re
import uuid
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from src import embeddings
from src.api import create_app
from src.db import repository as repo
from src.db.session import get_db

# Every metric name the C14 exposition must carry (bare names; the text exposition
# renders counters with a ``_total`` suffix and histograms with ``_count`` / ``_sum`` /
# ``_bucket`` families, but each base name appears in a ``# HELP`` / ``# TYPE`` line).
_EXPECTED_METRIC_NAMES = [
    "http_requests_total",
    "http_request_duration_seconds",
    "recommend_requests_total",
    "recommend_latency_seconds",
    "cache_hits_total",
    "cache_misses_total",
    "feedback_total",
    "corpus_size",
]


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded rows and query hash are isolated."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``.

    Overriding the dependency runs every request inside the same outer transaction as
    ``db_session``, so HTTP writes are visible to direct session queries here and are
    discarded when the fixture tears down.
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


def _query(suffix: str) -> dict:
    """A DB-pool-timeout paraphrase with explicit facets, namespaced by ``suffix``.

    Explicit ``service`` / ``severity`` / ``tags`` give a meaningful (non-catch-all)
    feedback bucket, and the suffix in ``title`` / ``description`` makes the normalised
    query hash unique per test so the Redis recommendation cache key can never be
    pre-warmed by a prior run — the first request is a guaranteed MISS.
    """
    return {
        "title": f"Requests timing out waiting on a database connection ({suffix})",
        "description": (
            "During a load spike our API started returning timeouts; the Postgres "
            "connection pool ran out of free connections and callers queued until "
            f"they gave up. correlation-id {suffix}"
        ),
        "service": "payments",
        "severity": "high",
        "tags": ["database", "pool", "timeout"],
    }


# --------------------------------------------------------------------------- #
# Small parsers over the Prometheus text / JSON exposition
# --------------------------------------------------------------------------- #
def _snapshot(client: TestClient) -> dict:
    """Return the ``GET /metrics/json`` snapshot (200-checked)."""
    resp = client.get("/metrics/json")
    assert resp.status_code == 200, resp.text
    return resp.json()


def _sample_from_text(body: str, series: str) -> float:
    """Parse a single sample value out of the Prometheus text exposition.

    ``series`` is the fully-qualified series line prefix, e.g.
    ``recommend_latency_seconds_count`` or
    ``cache_hits_total{cache="recommendation"}``. Returns the float value of the
    first matching (non-comment) sample line, or ``0.0`` if absent (the series may
    not exist yet before its first observation).
    """
    # Match a line: "<series> <float>" allowing the labelled or bare form. Escape
    # regex metacharacters in the fixed series prefix (braces, quotes).
    pattern = re.compile(
        r"^" + re.escape(series) + r"\s+([0-9eE.+-]+)\s*$", re.MULTILINE
    )
    m = pattern.search(body)
    return float(m.group(1)) if m else 0.0


# --------------------------------------------------------------------------- #
# 1. GET /metrics -> 200, text/plain, all 8 metric names present
# --------------------------------------------------------------------------- #
def test_metrics_endpoint_exposes_all_metric_names(client: TestClient) -> None:
    """``GET /metrics`` answers 200 with a ``text/plain`` content type and a body
    that mentions every one of the 8 documented metric names."""
    resp = client.get("/metrics")
    assert resp.status_code == 200, resp.text

    content_type = resp.headers.get("content-type", "")
    assert content_type.startswith("text/plain"), content_type
    # C14 pins the classic Prometheus exposition version.
    assert "version=0.0.4" in content_type, content_type

    body = resp.text
    for name in _EXPECTED_METRIC_NAMES:
        assert name in body, f"metric name {name!r} missing from /metrics body"


# --------------------------------------------------------------------------- #
# 2. POST /recommend bumps recommend_requests_total by 1 and observes latency
# --------------------------------------------------------------------------- #
def test_recommend_increments_request_counter_and_latency(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """One ``POST /recommend`` increments ``recommend_requests_total`` by exactly 1
    and ``recommend_latency_seconds_count`` by at least 1 (the pipeline latency was
    observed)."""
    _seed(db_session, unique)

    before = _snapshot(client)["recommend_requests_total"]
    latency_before = _sample_from_text(
        client.get("/metrics").text, "recommend_latency_seconds_count"
    )

    resp = client.post("/recommend", json=_query(unique))
    assert resp.status_code == 200, resp.text
    assert resp.json()["cached"] is False

    after = _snapshot(client)["recommend_requests_total"]
    latency_after = _sample_from_text(
        client.get("/metrics").text, "recommend_latency_seconds_count"
    )

    assert after == pytest.approx(before + 1.0), (
        f"recommend_requests_total {before} -> {after}, expected +1"
    )
    assert latency_after >= latency_before + 1.0, (
        f"recommend_latency_seconds_count {latency_before} -> {latency_after}, "
        "expected the observation count to rise (latency observed)"
    )


# --------------------------------------------------------------------------- #
# 3. Identical 2nd POST /recommend increments cache_hits_total{recommendation}
# --------------------------------------------------------------------------- #
def test_identical_recommend_increments_recommendation_cache_hit(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A second, byte-identical ``POST /recommend`` is served from Redis:
    ``cache_hits_total{cache="recommendation"}`` increments (the first call was the
    miss) and the response reports ``cached is True``."""
    _seed(db_session, unique)
    query = _query(unique)

    def _rec_hits() -> float:
        return _sample_from_text(
            client.get("/metrics").text,
            'cache_hits_total{cache="recommendation"}',
        )

    hits_before = _rec_hits()

    first = client.post("/recommend", json=query)
    assert first.status_code == 200, first.text
    assert first.json()["cached"] is False  # genuine miss (unique query hash)

    second = client.post("/recommend", json=query)
    assert second.status_code == 200, second.text
    assert second.json()["cached"] is True  # served from the recommendation cache

    hits_after = _rec_hits()
    assert hits_after >= hits_before + 1.0, (
        f'cache_hits_total{{cache="recommendation"}} {hits_before} -> {hits_after}, '
        "expected the identical repeat to register at least one hit"
    )
    # Cross-check the JSON snapshot exposes the same hit under cache.recommendation.
    snap = _snapshot(client)
    assert snap["cache"]["recommendation"]["hits"] >= hits_after


# --------------------------------------------------------------------------- #
# 4. Valid POST /feedback increments feedback_total{helpful="true"}
# --------------------------------------------------------------------------- #
def test_feedback_increments_helpful_counter(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A valid helpful vote (against a real prior recommendation) increments
    ``feedback_total{helpful="true"}`` (equivalently ``feedback.helpful`` in the JSON
    snapshot) by 1."""
    _seed(db_session, unique)

    rec = client.post("/recommend", json=_query(unique))
    assert rec.status_code == 200, rec.text
    body = rec.json()
    assert body["count"] > 0
    rec_id = body["recommendation_id"]
    incident_id = body["suggestions"][0]["incident_id"]

    before = _snapshot(client)["feedback"]["helpful"]

    fb = client.post(
        "/feedback",
        json={
            "recommendation_id": rec_id,
            "incident_id": incident_id,
            "helpful": True,
        },
    )
    assert fb.status_code == 201, fb.text
    assert fb.json()["recorded"] is True

    after = _snapshot(client)["feedback"]["helpful"]
    assert after == pytest.approx(before + 1.0), (
        f'feedback_total{{helpful="true"}} {before} -> {after}, expected +1'
    )


# --------------------------------------------------------------------------- #
# 5. GET /metrics/json -> 200 JSON with the documented keys
# --------------------------------------------------------------------------- #
def test_metrics_json_snapshot_shape(client: TestClient) -> None:
    """``GET /metrics/json`` returns a 200 JSON body carrying the documented keys:
    ``recommend_requests_total``, ``cache``, ``feedback`` and ``corpus_size`` (with
    the nested per-cache and helpful/unhelpful structure)."""
    snap = _snapshot(client)

    assert "recommend_requests_total" in snap
    assert "cache" in snap
    assert "feedback" in snap
    assert "corpus_size" in snap

    # Nested cache shape: both cache kinds, each with hits + misses.
    for cache_name in ("embedding", "recommendation"):
        assert cache_name in snap["cache"], cache_name
        assert "hits" in snap["cache"][cache_name]
        assert "misses" in snap["cache"][cache_name]

    # Feedback shape: helpful + unhelpful tallies.
    assert "helpful" in snap["feedback"]
    assert "unhelpful" in snap["feedback"]

    # Every figure is numeric.
    assert isinstance(snap["recommend_requests_total"], (int, float))
    assert isinstance(snap["corpus_size"], (int, float))
