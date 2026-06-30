"""Integration tests for the C6 semantic retrieval layer.

These exercise :mod:`src.retrieval` (``retrieve_candidates`` / ``semantic_search``)
and the underlying :func:`src.db.repository.knn_by_embedding` against the **real**
Postgres + pgvector service — the migrated schema, the ``vector(384)`` column and
the HNSW ``vector_cosine_ops`` index. A tiny, semantically-distinct corpus is
seeded **inside each test** with genuine MiniLM vectors
(``embeddings.embed_incident``), so real cosine distances flow through pgvector.

Each test runs inside the rolled-back ``db_session`` transaction (see
``conftest.py``), so the seeded corpus never leaks. ``service`` values are
namespaced with a per-test unique suffix so filter/count assertions are exact and
result sets can be scoped to this test's rows.

Coverage (C6):
  * near-duplicate query ranks the paraphrased incident #1 with the top ``semantic``;
  * scores are monotonically non-increasing and lie in a sane ``[0, 1]``-ish band;
  * ``service`` / ``severities`` pre-filters narrow the result set;
  * NULL-embedding incidents are never returned, even with a large ``k``;
  * both entry points (``retrieve_candidates`` with a vector, ``semantic_search``
    with raw fields) are exercised.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src import embeddings
from src.db import repository as repo
from src.db.models import Incident
from src.retrieval import Candidate, retrieve_candidates, semantic_search


def _full_k(session: Session) -> int:
    """A ``k`` guaranteed to exceed the whole embedded corpus.

    The integration DB uses a persistent volume that may already hold committed
    incidents from other suites/commits. Any assertion that expects *all* of this
    test's seeded rows to come back must use a ``k`` larger than the total number
    of embedded rows, otherwise a farther-away seeded row can be crowded out of the
    ``LIMIT k`` window by unrelated rows. Sizing ``k`` off the live count keeps the
    completeness/count assertions deterministic regardless of volume state.
    """
    embedded = session.scalar(
        select(func.count()).select_from(Incident).where(Incident.embedding.is_not(None))
    )
    return int(embedded or 0) + 10


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded services/ids are isolated."""
    return uuid.uuid4().hex[:12]


# Six clearly-distinct incident families. Each is keyed so a test can reference the
# one it paraphrases. Text is deliberately different across families so MiniLM puts
# real distance between them.
def _corpus_specs() -> dict[str, dict]:
    return {
        "db_timeout": {
            "title": "Database connection pool exhausted causing request timeouts",
            "description": (
                "Under peak load the Postgres connection pool was exhausted; "
                "checkout requests queued and then timed out with 500 errors."
            ),
            "severity": "high",
            "tags": ["database", "timeout", "pool", "postgres"],
            "resolution": "Raised the pool ceiling and added a circuit breaker.",
        },
        "oom": {
            "title": "Service killed by OOM killer after memory leak",
            "description": (
                "The worker's resident memory grew unbounded and the Linux OOM "
                "killer terminated the container repeatedly."
            ),
            "severity": "critical",
            "tags": ["memory", "oom", "leak", "crash"],
            "resolution": "Fixed the leaked buffer and set a memory limit.",
        },
        "tls_cert": {
            "title": "Expired TLS certificate breaking HTTPS handshakes",
            "description": (
                "Clients could not establish HTTPS connections because the "
                "certificate for the API gateway had expired."
            ),
            "severity": "high",
            "tags": ["tls", "certificate", "https", "expiry"],
            "resolution": "Renewed the certificate and automated rotation.",
        },
        "disk_full": {
            "title": "Disk full on log volume halting writes",
            "description": (
                "The log partition reached 100% utilisation; the application "
                "could no longer write and began dropping events."
            ),
            "severity": "medium",
            "tags": ["disk", "storage", "logs", "capacity"],
            "resolution": "Rotated logs and expanded the volume.",
        },
        "auth": {
            "title": "Authentication failures after OAuth token misconfiguration",
            "description": (
                "Users were rejected at login because the OAuth client secret "
                "was rotated but not propagated to the auth service."
            ),
            "severity": "high",
            "tags": ["auth", "oauth", "login", "token"],
            "resolution": "Propagated the new secret and reloaded config.",
        },
        "latency": {
            "title": "Elevated p99 latency from slow downstream dependency",
            "description": (
                "The recommendations endpoint saw p99 latency spike because a "
                "downstream ranking service degraded under load."
            ),
            "severity": "low",
            "tags": ["latency", "p99", "performance", "downstream"],
            "resolution": "Added a timeout and cached downstream responses.",
        },
    }


def _seed_corpus(session: Session, suffix: str) -> dict[str, int]:
    """Seed the six-family corpus with real MiniLM vectors; return key -> incident id.

    Every row's ``service`` is namespaced with ``suffix`` (``<family>_<suffix>``) so
    the whole corpus is uniquely scopable, and each incident is embedded through the
    real model so genuine cosine distances land in Postgres.
    """
    ids: dict[str, int] = {}
    for key, spec in _corpus_specs().items():
        vec = embeddings.embed_incident(
            spec["title"], spec["description"], spec["tags"]
        )
        incident = repo.add_incident(
            session,
            title=spec["title"],
            description=spec["description"],
            service=f"{key}_{suffix}",
            severity=spec["severity"],
            tags=spec["tags"],
            resolution=spec["resolution"],
            embedding=vec,
            commit=True,
        )
        ids[key] = incident.id
    return ids


def _restrict(candidates: list[Candidate], ids: set[int]) -> list[Candidate]:
    """Keep only candidates whose incident_id is in this test's seeded set."""
    return [c for c in candidates if c.incident_id in ids]


# --------------------------------------------------------------------------- #
# 1. Near-duplicate ranks #1  (semantic_search entry point)
# --------------------------------------------------------------------------- #
def test_near_duplicate_ranks_first(db_session: Session, unique: str) -> None:
    """A paraphrase of the DB-timeout incident must come back rank #1 with the
    maximum ``semantic`` score, retrieved via the raw-fields ``semantic_search``."""
    ids = _seed_corpus(db_session, unique)
    seeded = set(ids.values())

    # A near-duplicate / paraphrase of the DB-timeout family (different wording,
    # same meaning) — no verbatim overlap with the seeded title/description.
    candidates = semantic_search(
        db_session,
        title="Postgres pool ran out of connections so queries timed out",
        description=(
            "During a traffic spike the database ran out of pooled connections "
            "and incoming queries hung until they timed out."
        ),
        tags=["postgres", "connections", "timeout"],
        # Exhaustive k so the runner-up seeded row is also in-window (see _full_k).
        k=_full_k(db_session),
    )
    scoped = _restrict(candidates, seeded)

    assert scoped, "expected the seeded corpus to be retrieved"
    assert scoped[0].incident_id == ids["db_timeout"], (
        "the DB-timeout paraphrase should rank #1; "
        f"got id={scoped[0].incident_id} (db_timeout id={ids['db_timeout']})"
    )
    # Its semantic score is the strict maximum over the seeded corpus.
    top = scoped[0].semantic
    runner_up = scoped[1].semantic
    assert top == max(c.semantic for c in scoped)
    assert top > runner_up, (
        f"rank-1 semantic {top:.4f} should beat runner-up {runner_up:.4f}"
    )
    # A genuine near-duplicate should score high.
    assert top > 0.7, f"near-duplicate semantic {top:.4f} unexpectedly low"


# --------------------------------------------------------------------------- #
# 2. Descending order + sane score band  (retrieve_candidates + precomputed vec)
# --------------------------------------------------------------------------- #
def test_scores_descending_and_sane(db_session: Session, unique: str) -> None:
    """``semantic`` is monotonically non-increasing and values sit roughly in
    ``[0, 1]``; the near-duplicate is high and an unrelated family is clearly lower.
    Uses ``retrieve_candidates`` with a precomputed ``embed_query`` vector."""
    ids = _seed_corpus(db_session, unique)
    seeded = set(ids.values())

    query_vec = embeddings.embed_query(
        "Postgres pool ran out of connections so queries timed out",
        "The database ran out of pooled connections and queries hung until timeout.",
        ["postgres", "connections", "timeout"],
    )
    # Exhaustive k so every seeded row is inside the LIMIT window (the DB volume may
    # already hold other embedded incidents; see _full_k).
    candidates = retrieve_candidates(db_session, query_vec, k=_full_k(db_session))
    scoped = _restrict(candidates, seeded)

    assert len(scoped) == len(ids), "all embedded incidents should be retrievable"

    scores = [c.semantic for c in scoped]
    # Monotonically non-increasing (best-match-first). Restricting to the seeded
    # rows preserves relative order, so the check holds on the scoped slice too.
    assert all(a >= b - 1e-9 for a, b in zip(scores, scores[1:])), (
        f"scores not non-increasing: {scores}"
    )
    # Sane cosine-similarity band for MiniLM unit vectors.
    for s in scores:
        assert -0.05 <= s <= 1.0001, f"semantic {s} outside sane [0,1] band"

    # Near-duplicate (rank 1) is high; the least-similar family is clearly lower.
    assert scores[0] > 0.7, f"near-duplicate top {scores[0]:.4f} too low"
    assert scores[-1] < scores[0], "least-similar should score below the near-dup"

    by_id = {c.incident_id: c.semantic for c in scoped}
    assert by_id[ids["db_timeout"]] == scores[0], "db_timeout should be the top score"


# --------------------------------------------------------------------------- #
# 3. Filters narrow the result set  (service, then severities)
# --------------------------------------------------------------------------- #
def test_service_filter_narrows(db_session: Session, unique: str) -> None:
    """A ``service`` pre-filter returns only that service and never more rows than
    the unfiltered search."""
    ids = _seed_corpus(db_session, unique)
    seeded = set(ids.values())

    query_vec = embeddings.embed_query(
        "database connections timing out",
        "the pool was exhausted and requests timed out",
        ["database"],
    )

    unfiltered = _restrict(
        retrieve_candidates(db_session, query_vec, k=_full_k(db_session)), seeded
    )
    target_service = f"db_timeout_{unique}"
    filtered = retrieve_candidates(
        db_session, query_vec, k=50, service=target_service
    )

    assert filtered, "service filter should still return the matching row"
    assert all(c.service == target_service for c in filtered), (
        "every filtered candidate must belong to the requested service"
    )
    assert {c.incident_id for c in filtered} == {ids["db_timeout"]}
    assert len(filtered) <= len(unfiltered)
    assert len(filtered) < len(unfiltered), "filtering should strictly narrow here"


def test_severity_filter_narrows(db_session: Session, unique: str) -> None:
    """A ``severities`` pre-filter returns only those severities and no more rows
    than the unfiltered search."""
    ids = _seed_corpus(db_session, unique)
    seeded = set(ids.values())

    query_vec = embeddings.embed_query(
        "service crashed under load",
        "the process was terminated during a spike",
        ["crash"],
    )
    # Exhaustive k so no seeded row is truncated by LIMIT (see _full_k).
    full_k = _full_k(db_session)

    unfiltered = _restrict(
        retrieve_candidates(db_session, query_vec, k=full_k), seeded
    )

    # "critical" was used by exactly one seeded family (oom).
    crit = _restrict(
        retrieve_candidates(db_session, query_vec, k=full_k, severities=["critical"]),
        seeded,
    )
    assert all(c.severity == "critical" for c in crit)
    assert {c.incident_id for c in crit} == {ids["oom"]}
    assert len(crit) <= len(unfiltered)
    assert len(crit) < len(unfiltered)

    # A two-severity filter narrows to exactly the "high" + "critical" families.
    high_ids = {ids["db_timeout"], ids["tls_cert"], ids["auth"]}
    hi_crit = _restrict(
        retrieve_candidates(
            db_session, query_vec, k=full_k, severities=["high", "critical"]
        ),
        seeded,
    )
    assert all(c.severity in {"high", "critical"} for c in hi_crit)
    assert {c.incident_id for c in hi_crit} == high_ids | {ids["oom"]}
    assert len(hi_crit) <= len(unfiltered)


# --------------------------------------------------------------------------- #
# 4. NULL-embedding incidents are excluded (even with a huge k)
# --------------------------------------------------------------------------- #
def test_null_embedding_excluded(db_session: Session, unique: str) -> None:
    """An incident inserted with ``embedding=None`` must never appear in results,
    even when ``k`` far exceeds the corpus size."""
    ids = _seed_corpus(db_session, unique)
    seeded = set(ids.values())

    null_incident = repo.add_incident(
        db_session,
        title="Unindexed incident with no embedding yet",
        description="This row has not been embedded by the backfill.",
        service=f"unembedded_{unique}",
        severity="high",
        tags=["pending"],
        resolution="n/a",
        embedding=None,
        commit=True,
    )
    assert null_incident.id is not None
    assert repo.get_incident(db_session, null_incident.id).embedding is None

    # Query text intentionally close to the NULL row's own text, and a large k, so
    # only the NULL filter (not distance/limit) can keep it out.
    query_vec = embeddings.embed_query(
        "Unindexed incident with no embedding yet",
        "This row has not been embedded by the backfill.",
        ["pending"],
    )
    candidates = retrieve_candidates(db_session, query_vec, k=1000)
    returned_ids = {c.incident_id for c in candidates}

    assert null_incident.id not in returned_ids, (
        "a NULL-embedding incident must never be retrieved"
    )
    # The embedded corpus is still fully retrievable.
    assert seeded.issubset(returned_ids)


# --------------------------------------------------------------------------- #
# 5. Both entry points agree on the top match
# --------------------------------------------------------------------------- #
def test_both_entry_points_agree(db_session: Session, unique: str) -> None:
    """``semantic_search`` (raw fields) and ``retrieve_candidates`` (precomputed
    vector) return the same rank-1 incident and equal top scores for one query."""
    ids = _seed_corpus(db_session, unique)
    seeded = set(ids.values())

    title = "TLS certificate expired so HTTPS connections failed"
    description = "The gateway cert lapsed and clients could not complete the handshake."
    tags = ["tls", "certificate"]

    full_k = _full_k(db_session)
    via_search = _restrict(
        semantic_search(
            db_session, title=title, description=description, tags=tags, k=full_k
        ),
        seeded,
    )
    via_vec = _restrict(
        retrieve_candidates(
            db_session, embeddings.embed_query(title, description, tags), k=full_k
        ),
        seeded,
    )

    assert via_search and via_vec
    # Both should surface the TLS family at rank #1.
    assert via_search[0].incident_id == ids["tls_cert"]
    assert via_vec[0].incident_id == ids["tls_cert"]
    # Same embedding path underneath → identical top score.
    assert via_search[0].semantic == pytest.approx(via_vec[0].semantic, abs=1e-6)
    # And identical ordering over the seeded corpus.
    assert [c.incident_id for c in via_search] == [c.incident_id for c in via_vec]
