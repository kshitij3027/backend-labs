"""Integration tests for the C22 incident lifecycle: ``PUT`` / ``DELETE
/incidents/{id}`` + corpus-epoch cache invalidation.

Runs against the REAL migrated Postgres + pgvector and a REAL Redis (the compose
``test`` service provides both). As in ``test_feedback_rerank.py``, the FastAPI
``get_db`` dependency is overridden to yield the rolled-back ``db_session`` so HTTP
writes are visible to direct queries and undone on teardown. The **Redis** side
(recommendation cache + the global *corpus epoch*) is NOT transactional — it is a
live store — which is exactly why the corpus-epoch invalidation can be exercised
end-to-end here.

What this proves (C22)
----------------------
1. **PUT re-embed + rank shift + invalidation**: editing an incident's ``description``
   to text much closer to the query re-embeds the row, so the identical repeat
   ``/recommend`` MISSes (``cached is False``) and that incident's ``semantic`` score
   (and rank) shifts vs the pre-edit baseline.
2. **PUT metadata-only skips re-embed but still invalidates**: editing only
   ``severity`` leaves the stored vector byte-identical (so the ``semantic`` term for a
   fixed query is unchanged), yet the identical repeat ``/recommend`` is still
   ``cached is False`` (the corpus epoch was bumped anyway).
3. **PUT error / partial-update semantics**: unknown field / blank title / bad
   severity → 422; missing id → 404; an omitted field is left unchanged; an explicit
   ``tags: []`` clears the tags.
4. **DELETE**: create an incident + a real recommendation + a real feedback vote on it
   (so both a ``Feedback`` and a ``SuggestionScore`` row exist), then ``DELETE`` → 204;
   ``GET`` → 404; the incident is gone from ``/recommend``; the dependent ``Feedback``
   and ``SuggestionScore`` rows are gone; a re-``DELETE`` is 404.
5. **POST invalidation** with a **no-mutation control**: two identical ``/recommend``
   with nothing between → the 2nd is ``cached is True``; but a ``POST`` of a new
   incident between two identical ``/recommend`` makes the 2nd ``cached is False``.
"""

from __future__ import annotations

import uuid
from typing import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from src import embeddings
from src.api import create_app
from src.db import models
from src.db import repository as repo
from src.db.session import get_db
from src.feedback import query_pattern

# Explicit contextual facets on every /recommend so the derived pattern is a real,
# non-catch-all bucket (mirrors test_feedback_rerank.py).
_QUERY_SERVICE = "payments"
_QUERY_SEVERITY = "high"
_QUERY_TAGS = ["kubernetes", "oom", "crashloop"]


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded rows + query hash are isolated."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A TestClient whose ``get_db`` yields the rolled-back ``db_session``.

    Every request in a test runs inside the same outer transaction as ``db_session``,
    so HTTP writes are visible to direct session queries and discarded on teardown.
    Redis is a live store (not rolled back) — that is what makes the corpus-epoch
    invalidation observable here.
    """
    app = create_app()

    def _override_get_db() -> Iterator[Session]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# --------------------------------------------------------------------------- #
# The query the tests recommend against — a Kubernetes OOM / crashloop incident.
# The seeded corpus starts semantically FAR from this (an unrelated topic per
# incident) so a PUT that rewrites one incident's description toward this text
# produces a large, unambiguous semantic jump.
# --------------------------------------------------------------------------- #
_QUERY_TITLE = "Pods OOMKilled and stuck in CrashLoopBackOff after a memory spike"
_QUERY_DESCRIPTION = (
    "After a traffic surge several Kubernetes pods were OOMKilled by the kubelet and "
    "then entered CrashLoopBackOff; the container memory limit was too low for the "
    "working set and the deployment never became ready."
)

# Text that closely paraphrases the query — used to rewrite one incident's
# description so its embedding moves right next to the query vector.
_NEAR_QUERY_TEXT = (
    "Kubernetes pods were repeatedly OOMKilled and fell into CrashLoopBackOff after a "
    "memory spike; the pod memory limit was far below the working set so the kubelet "
    "killed the container and the deployment could not become ready under load."
)


def _far_specs() -> list[dict]:
    """Five incidents on unrelated topics (deliberately far from the K8s-OOM query)."""
    return [
        {
            "title": "TLS certificate for the public API expired",
            "description": (
                "The Let's Encrypt certificate on the edge load balancer expired and "
                "clients began rejecting the handshake with certificate-expired errors."
            ),
            "tags": ["tls", "certificate", "expiry"],
            "resolution": "Renewed the certificate and automated renewal via cert-manager.",
        },
        {
            "title": "Nightly analytics ETL produced duplicate rows",
            "description": (
                "A non-idempotent upsert in the nightly batch job double-counted "
                "revenue when a retry re-ran a partially-committed partition."
            ),
            "tags": ["etl", "batch", "idempotency"],
            "resolution": "Made the upsert idempotent with a natural key and dedupe step.",
        },
        {
            "title": "Search relevance regressed after an index rebuild",
            "description": (
                "A reindex dropped a custom analyzer, so multi-word queries stopped "
                "matching and top results for common terms became irrelevant."
            ),
            "tags": ["search", "elasticsearch", "analyzer"],
            "resolution": "Restored the analyzer mapping and reindexed with it applied.",
        },
        {
            "title": "Email delivery delayed by an SMTP provider outage",
            "description": (
                "The transactional email provider had a regional outage and the send "
                "queue backed up, delaying password-reset and receipt emails for hours."
            ),
            "tags": ["email", "smtp", "queue"],
            "resolution": "Failed over to the secondary email provider and drained the queue.",
        },
        {
            "title": "Feature flag rollout toggled the wrong cohort",
            "description": (
                "A misconfigured targeting rule enabled a beta checkout flow for all "
                "users instead of the internal cohort, causing a spike in errors."
            ),
            "tags": ["feature-flag", "rollout", "targeting"],
            "resolution": "Corrected the targeting rule and added a rollout guardrail.",
        },
    ]


def _seed_far(session: Session, suffix: str) -> list[int]:
    """Seed the unrelated-topic corpus with real MiniLM vectors; return the ids.

    Each ``service`` is namespaced with ``suffix`` so this test's rows are exactly
    scopable regardless of what the persistent DB volume already holds.
    """
    ids: list[int] = []
    for i, spec in enumerate(_far_specs()):
        vec = embeddings.embed_incident(
            spec["title"], spec["description"], spec["tags"]
        )
        inc = repo.add_incident(
            session,
            title=spec["title"],
            description=spec["description"],
            service=f"life{i}-{suffix}",
            severity="high",
            tags=spec["tags"],
            resolution=spec["resolution"],
            embedding=vec,
            commit=True,
        )
        ids.append(inc.id)
    return ids


def _recommend(client: TestClient, suffix: str, *, top_k: int = 50) -> dict:
    """POST /recommend with the fixed facets + a suffix-stamped query; return JSON.

    ``title`` / ``description`` are stamped with ``suffix`` so the normalised query
    hash (hence base Redis cache key) is unique per test; ``top_k`` is wide so every
    seeded incident is returned and rank shifts are visible.
    """
    resp = client.post(
        "/recommend",
        json={
            "title": f"{_QUERY_TITLE} ({suffix})",
            "description": f"{_QUERY_DESCRIPTION} correlation-id {suffix}",
            "service": _QUERY_SERVICE,
            "severity": _QUERY_SEVERITY,
            "tags": _QUERY_TAGS,
            "top_k": top_k,
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _by_id(suggestions: list[dict]) -> dict[int, dict]:
    return {s["incident_id"]: s for s in suggestions}


def _rank_of(suggestions: list[dict], incident_id: int) -> int | None:
    for i, s in enumerate(suggestions):
        if s["incident_id"] == incident_id:
            return i
    return None


def _create_incident(client: TestClient, suffix: str, **overrides: object) -> dict:
    """POST /incidents and return the created body (asserts 201)."""
    body = {
        "title": f"Baseline incident {suffix}",
        "description": f"A baseline incident description for {suffix}.",
        "service": f"svc-{suffix}",
        "severity": "medium",
        "tags": ["alpha", "beta"],
        "resolution": f"Baseline resolution {suffix}.",
    }
    body.update(overrides)
    resp = client.post("/incidents", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _embedding_bytes(session: Session, incident_id: int) -> bytes | None:
    """Return the raw stored vector for an incident as its text repr (stable compare).

    pgvector's text output is deterministic for a given stored vector, so comparing
    the ``embedding::text`` before/after a metadata-only PUT proves the vector was
    (not) rewritten without needing to parse 384 floats.
    """
    row = session.execute(
        text("SELECT embedding::text FROM incidents WHERE id = :id"),
        {"id": incident_id},
    ).first()
    return None if row is None else row[0]


# --------------------------------------------------------------------------- #
# 1. PUT re-embed → rank shift + cache invalidation
# --------------------------------------------------------------------------- #
def test_put_reembed_shifts_rank_and_invalidates_cache(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """Editing a description toward the query re-embeds the row: the identical repeat
    ``/recommend`` MISSes (``cached is False``) and that incident's ``semantic`` /
    rank shift vs the pre-edit baseline (proving the vector was recomputed)."""
    seeded = _seed_far(db_session, unique)

    # --- Baseline recommend (populates the cache under the current corpus epoch) ---
    baseline = _recommend(client, unique)
    assert baseline["cached"] is False
    base_by_id = _by_id(baseline["suggestions"])

    # Pick a mid-ranked incident that starts FAR from the query, rewrite it near it.
    target = seeded[len(seeded) // 2]
    assert target in base_by_id, "target incident should be retrievable at baseline"
    base_semantic = base_by_id[target]["semantic"]
    base_rank = _rank_of(baseline["suggestions"], target)

    # --- PUT: rewrite ONLY the description to text near the query (triggers re-embed) ---
    put = client.put(
        f"/incidents/{target}",
        json={"description": f"{_NEAR_QUERY_TEXT} correlation-id {unique}"},
    )
    assert put.status_code == 200, put.text
    assert put.json()["id"] == target
    # has_embedding stays True — the row was re-embedded, not left NULL.
    assert put.json()["has_embedding"] is True

    # --- Identical re-recommend → cache MISS (corpus epoch bumped) + re-rank ---
    after = _recommend(client, unique)
    assert after["cached"] is False, (
        "post-edit identical /recommend must MISS the cache (corpus epoch bumped)"
    )
    after_by_id = _by_id(after["suggestions"])
    assert target in after_by_id
    after_semantic = after_by_id[target]["semantic"]
    after_rank = _rank_of(after["suggestions"], target)

    # The vector was recomputed: the semantic score for the SAME query changed, and
    # (because it moved toward the query) rose and climbed the ranking.
    assert after_semantic != pytest.approx(base_semantic), (
        f"semantic for id={target} should change after re-embed: "
        f"{base_semantic} -> {after_semantic}"
    )
    assert after_semantic > base_semantic, (
        f"re-embedded toward the query, semantic should rise: "
        f"{base_semantic} -> {after_semantic}"
    )
    assert after_rank is not None and base_rank is not None
    assert after_rank < base_rank, (
        f"re-embedded incident should climb the ranking: rank {base_rank} -> {after_rank}"
    )


# --------------------------------------------------------------------------- #
# 2. PUT metadata-only → skips re-embed, still invalidates
# --------------------------------------------------------------------------- #
def test_put_metadata_only_skips_reembed_but_still_invalidates(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A PUT changing only ``severity`` leaves the stored vector byte-identical (so the
    ``semantic`` term for a fixed query is unchanged) yet still bumps the corpus epoch,
    so an identical repeat ``/recommend`` is still ``cached is False``."""
    seeded = _seed_far(db_session, unique)
    target = seeded[len(seeded) // 2]

    # Baseline recommend + capture the target's semantic and its raw stored vector.
    baseline = _recommend(client, unique)
    assert baseline["cached"] is False
    base_semantic = _by_id(baseline["suggestions"])[target]["semantic"]
    vec_before = _embedding_bytes(db_session, target)
    assert vec_before is not None

    # --- PUT: change ONLY severity (a non-text field) → no re-embed ---
    put = client.put(f"/incidents/{target}", json={"severity": "critical"})
    assert put.status_code == 200, put.text
    assert put.json()["severity"] == "critical"

    # The stored vector is byte-identical (metadata-only edit skipped the re-embed).
    vec_after = _embedding_bytes(db_session, target)
    assert vec_after == vec_before, "metadata-only PUT must not rewrite the embedding"

    # --- Identical re-recommend → still a cache MISS (epoch bumped anyway) ---
    after = _recommend(client, unique)
    assert after["cached"] is False, (
        "metadata-only PUT must still invalidate the cache (corpus epoch bumped)"
    )
    # ...and because the vector is unchanged, the semantic term for the SAME query is
    # identical to baseline (the recompute produced the same number).
    after_semantic = _by_id(after["suggestions"])[target]["semantic"]
    assert after_semantic == pytest.approx(base_semantic), (
        f"semantic must be unchanged when the vector is unchanged: "
        f"{base_semantic} -> {after_semantic}"
    )


# --------------------------------------------------------------------------- #
# 3. PUT error / partial-update semantics
# --------------------------------------------------------------------------- #
def test_put_unknown_field_returns_422(client: TestClient, unique: str) -> None:
    """A typo'd / unknown field is rejected at the schema boundary (extra='forbid')."""
    created = _create_incident(client, unique)
    resp = client.put(f"/incidents/{created['id']}", json={"titel": "typo"})
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "bad_body",
    [
        {"title": ""},
        {"title": "   "},
        {"description": ""},
        {"resolution": "  "},
        {"service": ""},
        {"severity": "urgent"},
        {"severity": "SEV1"},
    ],
)
def test_put_invalid_field_returns_422(
    client: TestClient, unique: str, bad_body: dict
) -> None:
    """A supplied blank free-text field or an out-of-set severity → 422."""
    created = _create_incident(client, unique)
    resp = client.put(f"/incidents/{created['id']}", json=bad_body)
    assert resp.status_code == 422, resp.text


def test_put_missing_incident_returns_404(client: TestClient) -> None:
    """A PUT to an unknown id → 404 (even with a valid body)."""
    resp = client.put("/incidents/999999999", json={"severity": "low"})
    assert resp.status_code == 404, resp.text


def test_put_omitted_fields_unchanged_and_partial_apply(
    client: TestClient, unique: str
) -> None:
    """Only supplied fields change; omitted fields keep their prior values."""
    created = _create_incident(
        client,
        unique,
        title=f"Original title {unique}",
        service=f"orig-svc-{unique}",
        severity="low",
        tags=["keep", "these"],
        resolution=f"Original resolution {unique}",
    )
    iid = created["id"]

    # Change only the severity.
    resp = client.put(f"/incidents/{iid}", json={"severity": "high"})
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["severity"] == "high"  # changed
    assert body["title"] == f"Original title {unique}"  # unchanged
    assert body["service"] == f"orig-svc-{unique}"  # unchanged
    assert body["tags"] == ["keep", "these"]  # unchanged
    assert body["resolution"] == f"Original resolution {unique}"  # unchanged


def test_put_explicit_empty_tags_clears_them(
    client: TestClient, unique: str
) -> None:
    """An explicit ``tags: []`` clears the incident's tags (vs an omitted tags)."""
    created = _create_incident(client, unique, tags=["one", "two", "three"])
    iid = created["id"]
    assert created["tags"] == ["one", "two", "three"]

    resp = client.put(f"/incidents/{iid}", json={"tags": []})
    assert resp.status_code == 200, resp.text
    assert resp.json()["tags"] == []

    # And the cleared tags are durable (GET reflects it).
    got = client.get(f"/incidents/{iid}")
    assert got.status_code == 200, got.text
    assert got.json()["tags"] == []


def test_put_tags_are_trimmed_and_deduped(client: TestClient, unique: str) -> None:
    """Supplied tags go through the same clean/dedupe as create."""
    created = _create_incident(client, unique)
    resp = client.put(
        f"/incidents/{created['id']}",
        json={"tags": [" db ", "db", "timeout", "", "timeout"]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["tags"] == ["db", "timeout"]


# --------------------------------------------------------------------------- #
# 4. DELETE → 204, cascade cleanup of Feedback + SuggestionScore, re-delete 404
# --------------------------------------------------------------------------- #
def test_delete_removes_incident_feedback_and_suggestion_scores(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """Create an incident + a real recommendation + a real feedback vote on it (so a
    ``Feedback`` and a ``SuggestionScore`` row exist), then DELETE → 204; GET → 404;
    it is gone from ``/recommend``; the dependent rows are gone; re-DELETE → 404."""
    # Seed the corpus, then find which incident actually gets recommended for the
    # query so we can vote on (and later delete) a real suggestion.
    _seed_far(db_session, unique)
    rec = _recommend(client, unique)
    assert rec["cached"] is False
    assert rec["count"] > 0, "need at least one suggestion to vote on"
    rec_id = rec["recommendation_id"]
    target = rec["suggestions"][0]["incident_id"]

    # Cast a real vote on that suggestion → creates a Feedback row AND upserts a
    # SuggestionScore row for (pattern, target).
    vote = client.post(
        "/feedback",
        json={"recommendation_id": rec_id, "incident_id": target, "helpful": True},
    )
    assert vote.status_code == 201, vote.text

    pattern = query_pattern(_QUERY_SERVICE, _QUERY_SEVERITY, _QUERY_TAGS)

    # Pre-condition: both dependent rows exist for the target incident.
    fb_before = (
        db_session.query(models.Feedback)
        .filter(models.Feedback.incident_id == target)
        .count()
    )
    ss_before = repo.get_suggestion_score(db_session, pattern, target)
    assert fb_before >= 1, "a Feedback row should exist for the voted incident"
    assert ss_before is not None, "a SuggestionScore row should exist for the pair"

    # --- DELETE → 204 (no body) ---
    resp = client.delete(f"/incidents/{target}")
    assert resp.status_code == 204, resp.text
    assert resp.content in (b"", None), "204 must carry no body"

    # GET the deleted incident → 404.
    got = client.get(f"/incidents/{target}")
    assert got.status_code == 404, got.text

    # The dependent rows are GONE (queried directly against both tables).
    db_session.expire_all()  # drop any identity-map cache so we re-read the DB
    fb_after = (
        db_session.query(models.Feedback)
        .filter(models.Feedback.incident_id == target)
        .count()
    )
    ss_after = repo.get_suggestion_score(db_session, pattern, target)
    assert fb_after == 0, "Feedback rows for the deleted incident must be removed"
    assert ss_after is None, "SuggestionScore row for the deleted incident must be removed"

    # The incident no longer appears in a fresh identical recommendation.
    after = _recommend(client, unique)
    assert after["cached"] is False, "delete must invalidate the cache"
    assert target not in _by_id(after["suggestions"]), (
        "deleted incident must not appear in /recommend"
    )

    # A second delete of the same id is a 404.
    again = client.delete(f"/incidents/{target}")
    assert again.status_code == 404, again.text


def test_delete_missing_incident_returns_404(client: TestClient) -> None:
    """DELETE of an unknown id → 404."""
    resp = client.delete("/incidents/999999999")
    assert resp.status_code == 404, resp.text


# --------------------------------------------------------------------------- #
# 5. POST invalidation + the no-mutation cached:true control
# --------------------------------------------------------------------------- #
def test_no_mutation_control_second_recommend_is_cached(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """CONTROL: two identical ``/recommend`` with NO corpus mutation between them →
    the second is served from cache (``cached is True``)."""
    _seed_far(db_session, unique)

    first = _recommend(client, unique)
    assert first["cached"] is False, "first recommend computes fresh (populates cache)"

    second = _recommend(client, unique)
    assert second["cached"] is True, (
        "identical /recommend with no mutation between must hit the cache"
    )


def test_post_incident_invalidates_recommend_cache(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A ``POST /incidents`` between two identical ``/recommend`` bumps the corpus epoch,
    so the second is a cache MISS (``cached is False``) — contrasting the control."""
    _seed_far(db_session, unique)

    first = _recommend(client, unique)
    assert first["cached"] is False

    # Add a new incident (bumps the corpus epoch).
    _create_incident(client, unique, service=f"newsvc-{unique}")

    second = _recommend(client, unique)
    assert second["cached"] is False, (
        "a POST /incidents between identical recommends must invalidate the cache"
    )
