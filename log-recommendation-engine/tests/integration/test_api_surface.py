"""Integration tests for the C13 operability surface: richer ``GET /incidents``
search, ``GET /stats`` rollups, and the deep ``GET /health`` probe.

Run against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). The ``db_session`` fixture (see
``tests/integration/conftest.py``) gives a transaction that is rolled back on
teardown, and the FastAPI ``get_db`` dependency is overridden to yield that same
session, so writes the ``TestClient`` makes over HTTP are visible to direct session
queries here and are undone afterwards. Because nothing is committed to the durable
DB outside these rolled-back transactions, the durable baseline is empty at the
start of every test — but the ``/stats`` and ``/health`` tests still snapshot a
baseline and assert on it defensively (so a stray pre-existing row can never make an
assertion flaky).

Seeding
-------
Incidents are seeded **directly** via ``repository.add_incident`` (not the HTTP
``POST /incidents``, which always embeds on ingest) so we can control exactly which
rows carry an embedding and which are ``embedding=None`` — that is what makes
``embedded_count`` (``/stats``) meaningfully differ from ``corpus_size``.

Coverage (C13):
  * ``GET /incidents?q=`` — case-insensitive substring over title+description only;
  * ``GET /incidents?tags=a&tags=b`` — Postgres array OVERLAP (row matches on ANY
    shared tag);
  * combined ``?service=&severity=&q=&tags=`` — AND semantics; ``total`` == full
    filtered count (proved with a small ``limit`` so ``total`` != page length);
  * ``?limit=0`` / ``?limit=500`` / ``?offset=-1`` → 422;
  * ``GET /stats`` — ``corpus_size`` / ``embedded_count`` / ``by_service`` /
    ``by_severity`` reconcile with the seed; after ``POST /recommend`` + a couple
    ``POST /feedback``: ``feedback_total == helpful + unhelpful``,
    ``recommendations_served >= 1``, ``top_patterns`` non-empty;
  * ``GET /health`` — 200, ``status:"ok"``, ``components`` booleans up;
    ``embedding_model`` is ``False`` before any embed, then ``True`` after a
    ``POST /recommend`` warms the model singleton.
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


@pytest.fixture
def unique() -> str:
    """Short unique suffix so a test's seeded rows are exactly scopable."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``.

    Every request runs inside the same outer transaction as ``db_session``, so HTTP
    writes are visible to direct session queries here and are discarded on teardown.
    """
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _add(
    session: Session,
    *,
    service: str,
    severity: str,
    title: str,
    description: str,
    tags: list[str],
    embed: bool,
) -> int:
    """Seed one incident directly through the repository; return its id.

    ``embed=True`` computes a real MiniLM vector (row counts toward
    ``embedded_count``); ``embed=False`` leaves ``embedding=None`` (does not).
    """
    vec = (
        embeddings.embed_incident(title, description, tags) if embed else None
    )
    inc = repo.add_incident(
        session,
        title=title,
        description=description,
        service=service,
        severity=severity,
        tags=tags,
        resolution="Some resolution text.",
        embedding=vec,
        commit=True,
    )
    return inc.id


# --------------------------------------------------------------------------- #
# GET /incidents — q (substring over title+description)
# --------------------------------------------------------------------------- #
def test_search_q_matches_title_or_description_case_insensitively(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """``?q=`` is a case-insensitive substring over title+description only.

    Rows whose title/description contain the needle match (regardless of case);
    a row that shares only the service/tags but not the needle text does NOT.
    """
    svc = f"qsvc-{unique}"
    needle = f"KafkaLag{unique}"  # mixed-case, unique so no other rows collide

    # (a) needle in the TITLE.
    id_title = _add(
        db_session,
        service=svc,
        severity="high",
        title=f"Consumer {needle} spiked on the orders topic",
        description="Lag climbed while the consumer group rebalanced.",
        tags=["kafka", "lag"],
        embed=False,
    )
    # (b) needle in the DESCRIPTION only (title has no needle).
    id_desc = _add(
        db_session,
        service=svc,
        severity="low",
        title="Broker rebalanced during a rolling restart",
        description=f"Observed {needle.lower()} until partitions reassigned.",
        tags=["kafka"],
        embed=False,
    )
    # (c) same service + overlapping tag but the needle appears NOWHERE → excluded.
    id_none = _add(
        db_session,
        service=svc,
        severity="high",
        title="Unrelated disk pressure incident",
        description="The log volume filled to capacity.",
        tags=["kafka", "disk"],
        embed=False,
    )

    # Query case-insensitively (lowercased needle) and scope to our service so the
    # assertion is exact even though `q` itself is global.
    resp = client.get(f"/incidents?service={svc}&q={needle.lower()}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    got = {item["id"] for item in data["items"]}

    assert got == {id_title, id_desc}, got
    assert id_none not in got
    assert data["total"] == 2


# --------------------------------------------------------------------------- #
# GET /incidents — tags (array OVERLAP: matches ANY shared tag)
# --------------------------------------------------------------------------- #
def test_tags_filter_is_array_overlap_any(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """``?tags=a&tags=b`` matches incidents whose tags overlap {a, b} (ANY shared)."""
    svc = f"tagsvc-{unique}"
    ta, tb, tc = f"a{unique}", f"b{unique}", f"c{unique}"

    id_a = _add(
        db_session, service=svc, severity="high",
        title="only tag a", description="d", tags=[ta], embed=False,
    )
    id_b = _add(
        db_session, service=svc, severity="high",
        title="only tag b", description="d", tags=[tb], embed=False,
    )
    id_ab = _add(
        db_session, service=svc, severity="high",
        title="tags a and b", description="d", tags=[ta, tb], embed=False,
    )
    id_c = _add(
        db_session, service=svc, severity="high",
        title="only tag c", description="d", tags=[tc], embed=False,
    )

    resp = client.get(f"/incidents?service={svc}&tags={ta}&tags={tb}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    got = {item["id"] for item in data["items"]}

    # Overlap with {a, b}: rows tagged a, b, or {a,b} — but NOT the c-only row.
    assert got == {id_a, id_b, id_ab}, got
    assert id_c not in got
    assert data["total"] == 3


# --------------------------------------------------------------------------- #
# GET /incidents — combined service + severity + q + tags (AND) + total vs page
# --------------------------------------------------------------------------- #
def test_combined_filters_are_anded_and_total_is_full_count(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """``?service=&severity=&q=&tags=`` AND together; ``total`` is the full match
    count even when a small ``limit`` returns fewer items."""
    svc = f"combo-{unique}"
    other_svc = f"combo-other-{unique}"
    needle = f"payments{unique}"
    tag = f"billing{unique}"

    # 4 rows that satisfy ALL of: service=svc, severity=high, q=needle, tag in tags.
    wanted_ids = set()
    for i in range(4):
        wanted_ids.add(
            _add(
                db_session, service=svc, severity="high",
                title=f"{needle} outage {i}",
                description="checkout failed for a subset of users",
                tags=[tag, "gateway"], embed=False,
            )
        )

    # Near-misses, each violating exactly one predicate (must all be excluded):
    _add(  # wrong severity
        db_session, service=svc, severity="low",
        title=f"{needle} minor blip", description="d", tags=[tag], embed=False,
    )
    _add(  # wrong service
        db_session, service=other_svc, severity="high",
        title=f"{needle} elsewhere", description="d", tags=[tag], embed=False,
    )
    _add(  # missing needle
        db_session, service=svc, severity="high",
        title="generic incident", description="no needle here", tags=[tag],
        embed=False,
    )
    _add(  # non-overlapping tag
        db_session, service=svc, severity="high",
        title=f"{needle} but other tag", description="d",
        tags=[f"unrelated{unique}"], embed=False,
    )

    url = (
        f"/incidents?service={svc}&severity=high&q={needle}&tags={tag}"
        f"&limit=2"
    )
    resp = client.get(url)
    assert resp.status_code == 200, resp.text
    data = resp.json()

    # total counts every match (4), the page returns only `limit` (2).
    assert data["total"] == 4
    assert len(data["items"]) == 2
    assert data["limit"] == 2
    # Every returned item is one of the fully-matching rows.
    assert {item["id"] for item in data["items"]} <= wanted_ids
    assert all(item["service"] == svc for item in data["items"])
    assert all(item["severity"] == "high" for item in data["items"])


@pytest.mark.parametrize(
    "query",
    ["limit=0", "limit=500", "offset=-1"],
)
def test_pagination_bounds_return_422(client: TestClient, query: str) -> None:
    """``limit`` outside 1–200 or a negative ``offset`` → 422 at the boundary."""
    resp = client.get(f"/incidents?{query}")
    assert resp.status_code == 422, resp.text


# --------------------------------------------------------------------------- #
# GET /stats — corpus/embedded/by_service/by_severity reconcile with the seed
# --------------------------------------------------------------------------- #
def test_stats_corpus_and_grouping_reconcile_with_seed(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """``/stats`` corpus_size / embedded_count / grouped counts match the seed.

    A baseline is snapshotted first (durable rows are rolled back between tests, so
    it should be empty, but we assert on deltas defensively). We seed 5 incidents:
    3 embedded + 2 NULL-embedded, across 2 services and 2 severities.
    """
    base = client.get("/stats").json()
    base_corpus = base["corpus_size"]
    base_embedded = base["embedded_count"]

    svc_a = f"statsA-{unique}"
    svc_b = f"statsB-{unique}"

    # svc_a: 2 high (both embedded), 1 medium (NULL-embedded)
    _add(db_session, service=svc_a, severity="high", title="a1", description="d",
         tags=["x"], embed=True)
    _add(db_session, service=svc_a, severity="high", title="a2", description="d",
         tags=["x"], embed=True)
    _add(db_session, service=svc_a, severity="medium", title="a3", description="d",
         tags=["x"], embed=False)
    # svc_b: 1 high (embedded), 1 low (NULL-embedded)
    _add(db_session, service=svc_b, severity="high", title="b1", description="d",
         tags=["x"], embed=True)
    _add(db_session, service=svc_b, severity="low", title="b2", description="d",
         tags=["x"], embed=False)

    stats = client.get("/stats").json()

    # 5 seeded incidents, 3 of them embedded.
    assert stats["corpus_size"] == base_corpus + 5
    assert stats["embedded_count"] == base_embedded + 3

    by_service = stats["by_service"]
    assert by_service.get(svc_a) == 3
    assert by_service.get(svc_b) == 2

    # by_severity is a GLOBAL grouping; assert our seed's contribution via deltas.
    by_severity = stats["by_severity"]
    base_by_sev = base.get("by_severity", {})
    assert by_severity.get("high", 0) - base_by_sev.get("high", 0) == 3
    assert by_severity.get("medium", 0) - base_by_sev.get("medium", 0) == 1
    assert by_severity.get("low", 0) - base_by_sev.get("low", 0) == 1

    # Grouped counts sum to the corpus size.
    assert sum(by_service.values()) == stats["corpus_size"]
    assert sum(by_severity.values()) == stats["corpus_size"]


# --------------------------------------------------------------------------- #
# GET /stats — feedback tallies + recommendations_served + top_patterns
# --------------------------------------------------------------------------- #
def test_stats_reflects_recommend_and_feedback(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """After a ``POST /recommend`` + two ``POST /feedback`` votes, ``/stats`` shows
    ``recommendations_served >= 1``, ``feedback_total == helpful + unhelpful`` (== 2
    over baseline), and a non-empty ``top_patterns``."""
    base = client.get("/stats").json()
    base_recs = base["recommendations_served"]
    base_fb_total = base["feedback_total"]

    # A tiny coherent DB-pool-timeout family so /recommend returns real suggestions.
    svc = f"fbsvc-{unique}"
    _add(
        db_session, service=svc, severity="high",
        title="Database connection pool exhausted causing request timeouts",
        description=(
            "Under peak load the Postgres connection pool was fully checked out; "
            "queries queued waiting for a connection and timed out."
        ),
        tags=["database", "timeout", "pool"], embed=True,
    )
    _add(
        db_session, service=svc, severity="critical",
        title="DB pool timeout under load spikes 500 error rate",
        description=(
            "A traffic spike exhausted the connection pool; threads waited on a "
            "free database connection past the pool timeout and errors climbed."
        ),
        tags=["database", "timeout", "pool"], embed=True,
    )

    rec = client.post(
        "/recommend",
        json={
            "title": f"Requests timing out on a DB connection {unique}",
            "description": (
                "During a load spike the Postgres connection pool ran out of free "
                f"connections and callers queued until they gave up. {unique}"
            ),
            "service": "payments",
            "severity": "high",
            "tags": ["database", "pool", "timeout"],
        },
    )
    assert rec.status_code == 200, rec.text
    rec_body = rec.json()
    rec_id = rec_body["recommendation_id"]
    assert rec_body["count"] > 0, "expected the seeded family to produce suggestions"
    suggestion_ids = [s["incident_id"] for s in rec_body["suggestions"]]

    # Two votes on real served suggestions: one helpful, one not.
    v1 = client.post(
        "/feedback",
        json={"recommendation_id": rec_id, "incident_id": suggestion_ids[0],
              "helpful": True},
    )
    assert v1.status_code == 201, v1.text
    target2 = suggestion_ids[1] if len(suggestion_ids) > 1 else suggestion_ids[0]
    v2 = client.post(
        "/feedback",
        json={"recommendation_id": rec_id, "incident_id": target2,
              "helpful": False},
    )
    assert v2.status_code == 201, v2.text

    stats = client.get("/stats").json()

    assert stats["recommendations_served"] == base_recs + 1
    assert stats["feedback_total"] == base_fb_total + 2
    # helpful + unhelpful == total by construction.
    assert (
        stats["feedback_helpful"] + stats["feedback_unhelpful"]
        == stats["feedback_total"]
    )
    # At least our one helpful + one unhelpful vote are reflected.
    assert stats["feedback_helpful"] >= 1
    assert stats["feedback_unhelpful"] >= 1

    # top_patterns must be non-empty now that a learned aggregate exists, and each
    # row carries the documented (query_pattern, helpful, unhelpful) shape.
    assert stats["top_patterns"], "expected a non-empty top_patterns after feedback"
    for row in stats["top_patterns"]:
        assert set(row.keys()) == {"query_pattern", "helpful", "unhelpful"}
        assert isinstance(row["query_pattern"], str)
        assert isinstance(row["helpful"], int)
        assert isinstance(row["unhelpful"], int)


# --------------------------------------------------------------------------- #
# GET /health — deep probe: components up, embedding_model false → true
# --------------------------------------------------------------------------- #
def test_health_deep_probe_components_and_model_warmup(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """``/health`` → 200 ``status:ok`` with database/vector_extension/redis up.

    ``embedding_model`` reflects the lru_cache: it must flip to ``True`` once a
    ``POST /recommend`` (or any embed) has warmed the model singleton. Since the
    model is a process-wide singleton, we drive the transition by comparing the flag
    before and after an embed rather than asserting an absolute ``False`` first
    (an earlier test in the same process may already have loaded it).
    """
    before = client.get("/health")
    assert before.status_code == 200, before.text
    body = before.json()

    assert body["status"] == "ok"
    assert body["service"] == "log-recommendation-engine"
    assert body["version"] == "0.1.0"

    comp = body["components"]
    assert comp["database"] is True
    assert comp["vector_extension"] is True
    assert comp["redis"] is True
    assert isinstance(comp["embedding_model"], bool)

    # Force the model to load via a real recommendation call, then re-probe: the
    # embedding_model flag must be True afterwards (the cache is now populated).
    _add(
        db_session, service=f"warm-{unique}", severity="high",
        title="warm the model", description="load the embedding singleton",
        tags=["warm"], embed=False,
    )
    rec = client.post(
        "/recommend",
        json={
            "title": f"anything to trigger an embed {unique}",
            "description": f"the model singleton must load for this call {unique}",
        },
    )
    assert rec.status_code == 200, rec.text

    after = client.get("/health").json()
    assert after["components"]["embedding_model"] is True, (
        "embedding_model must be True after a /recommend warms the model singleton"
    )
