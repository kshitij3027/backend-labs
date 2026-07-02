"""Integration tests for the runtime-config endpoints ``GET | PUT /config`` (C12).

Run against a REAL Redis (the shared ``runtime_config`` hash + version counter) and
the REAL migrated Postgres+pgvector — the compose ``test`` service provides both.
The ``db_session`` fixture (see ``../conftest.py``) rolls the DB back on teardown and
the ``get_db`` dependency is overridden so the ``TestClient``'s HTTP writes are visible
here and then discarded.

Redis, unlike the DB, is **not** transactional: the ``runtime_config`` overrides hash
and the global config version persist across tests in a run. So these tests assert
*relative* effects (version strictly increases across a PUT; a re-``/recommend`` after a
config bump MISSes the cache) rather than absolute version numbers, and they **reset the
weights explicitly at the start** of the reorder test so a prior test's overrides can't
mask the effect. The reorder corpus is namespaced per-test so its query hash is unique.

Coverage (C12):
  * ``GET /config`` → 200, body ``{version:int, config:{...9 documented keys...}}``;
  * a valid ``PUT`` bumps the version, MISSes the cache on the identical repeat
    (``cached is False``), and reorders the ranking toward the newly-dominant signal;
  * validation: out-of-range value / unknown key / empty body → 422.
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
from src.runtime_config import TUNABLE_KEYS

# The 9 documented tunable keys the effective config must always expose.
_CONFIG_KEYS = set(TUNABLE_KEYS)

# A neutral, known-good weighting we reset to at the start of the reorder test so a
# leftover override from an earlier test can't skew the baseline. (Weights need not
# sum to 1 — validation only requires each ≥ 0.)
_BALANCED = {
    "weight_semantic": 0.6,
    "weight_contextual": 0.4,
    "weight_feedback": 0.2,
    "epsilon_explore": 0.0,  # exploration OFF so the reorder assertion is deterministic.
    "diversity_threshold": 0.9,
}


@pytest.fixture
def unique() -> str:
    """Short unique suffix so each test's seeded rows and query hash are isolated."""
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
# 1. GET /config → 200 with the documented shape
# --------------------------------------------------------------------------- #
def test_get_config_returns_version_and_all_keys(client: TestClient) -> None:
    """``GET /config`` → 200; body is ``{version:int, config:{...9 keys...}}`` and every
    documented tunable key is present with a numeric value."""
    resp = client.get("/config")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert isinstance(body["version"], int)
    config = body["config"]
    assert isinstance(config, dict)
    assert set(config.keys()) == _CONFIG_KEYS, (
        f"config keys {set(config.keys())} != documented {_CONFIG_KEYS}"
    )
    for key, value in config.items():
        assert isinstance(value, (int, float)), f"{key} not numeric: {value!r}"
    # top_k is an int knob.
    assert isinstance(config["top_k"], int)


# --------------------------------------------------------------------------- #
# 2. PUT /config: version bump + cache MISS + ranking reorder
# --------------------------------------------------------------------------- #
def _reorder_specs(suffix: str) -> tuple[dict, dict, dict]:
    """Two incidents that a semantic-heavy vs contextual-heavy blend ranks differently.

    * ``sem_incident`` — text that closely paraphrases the query (high semantic) but a
      deliberately *mismatched* service/severity/tags (low contextual).
    * ``ctx_incident`` — text about an unrelated topic (low semantic) but a service /
      severity / tags that *exactly* match the query facets (high contextual).

    So a semantic-dominant weighting ranks ``sem_incident`` first, while a
    contextual-dominant one ranks ``ctx_incident`` first — the reorder the PUT causes.
    Returns ``(sem_spec, ctx_spec, query)``.
    """
    sem_spec = {
        "title": "Database connection pool exhausted causing request timeouts",
        "description": (
            "Under peak load the Postgres connection pool was fully checked out; new "
            "queries queued waiting for a connection and then timed out with 500s."
        ),
        "service": f"sem-svc-{suffix}",  # does NOT match the query service.
        "severity": "low",  # far from the query severity "critical".
        "tags": ["unrelated", "misc"],  # no overlap with query tags.
        "resolution": "Raised the max pool size and added a statement timeout.",
    }
    ctx_spec = {
        "title": "Office coffee machine descaling schedule",
        "description": (
            "The break-room espresso machine needs periodic descaling; this note "
            "tracks the maintenance cadence and has nothing to do with databases."
        ),
        "service": f"match-svc-{suffix}",  # EXACTLY the query service.
        "severity": "critical",  # EXACTLY the query severity.
        "tags": ["database", "pool", "timeout"],  # EXACTLY the query tags.
        "resolution": "Descale the machine and reset the counter.",
    }
    query = {
        "title": f"Requests timing out waiting on a database connection ({suffix})",
        "description": (
            "During a load spike our API returned timeouts; the Postgres connection "
            f"pool ran out of free connections and callers queued. corr {suffix}"
        ),
        "service": f"match-svc-{suffix}",
        "severity": "critical",
        "tags": ["database", "pool", "timeout"],
    }
    return sem_spec, ctx_spec, query


def _seed_one(session: Session, spec: dict) -> int:
    vec = embeddings.embed_incident(spec["title"], spec["description"], spec["tags"])
    inc = repo.add_incident(
        session,
        title=spec["title"],
        description=spec["description"],
        service=spec["service"],
        severity=spec["severity"],
        tags=spec["tags"],
        resolution=spec["resolution"],
        embedding=vec,
        commit=True,
    )
    return inc.id


def test_put_config_bumps_version_misses_cache_and_reorders(
    client: TestClient, db_session: Session, unique: str
) -> None:
    """A valid ``PUT /config`` (a) bumps the version, (b) makes the identical repeat
    ``/recommend`` a cache MISS, and (c) reorders the ranking toward the now-dominant
    signal — with the ``breakdown["weights"]`` echoing the newly-applied weights."""
    sem_id = _seed_one(db_session, _reorder_specs(unique)[0])
    ctx_id = _seed_one(db_session, _reorder_specs(unique)[1])
    query = _reorder_specs(unique)[2]

    # --- Reset to a known baseline, then pin a CONTEXTUAL-dominant weighting. ---
    assert client.put("/config", json=_BALANCED).status_code == 200
    ctx_heavy = {"weight_semantic": 0.05, "weight_contextual": 0.95, "weight_feedback": 0.0}
    r_ctx = client.put("/config", json=ctx_heavy)
    assert r_ctx.status_code == 200, r_ctx.text
    version_ctx = r_ctx.json()["version"]

    first = client.post("/recommend", json=query).json()
    assert first["cached"] is False
    order_ctx = [s["incident_id"] for s in first["suggestions"]]
    # Contextual dominates → the facet-matching (but semantically-unrelated) incident wins.
    assert order_ctx[0] == ctx_id, (
        f"contextual-dominant blend should rank the facet-match {ctx_id} first; "
        f"got {order_ctx}"
    )
    # The served weights are echoed in every breakdown.
    assert first["suggestions"][0]["breakdown"]["weights"] == {
        "semantic": 0.05,
        "contextual": 0.95,
        "feedback": 0.0,
    }

    # --- Flip to a SEMANTIC-dominant weighting. ---
    sem_heavy = {"weight_semantic": 0.95, "weight_contextual": 0.05, "weight_feedback": 0.0}
    r_sem = client.put("/config", json=sem_heavy)
    assert r_sem.status_code == 200, r_sem.text
    version_sem = r_sem.json()["version"]
    # The version strictly increased across the PUT (fleet-wide cache invalidation).
    assert version_sem > version_ctx

    second = client.post("/recommend", json=query).json()
    # Identical query, but the config-version bump forces a fresh cache key → MISS.
    assert second["cached"] is False, (
        "identical /recommend after PUT /config must MISS the cache (version bumped)"
    )
    order_sem = [s["incident_id"] for s in second["suggestions"]]
    # Semantic now dominates → the semantically-close (but facet-mismatched) incident wins.
    assert order_sem[0] == sem_id, (
        f"semantic-dominant blend should rank the semantic match {sem_id} first; "
        f"got {order_sem}"
    )
    # The order genuinely changed vs the contextual-dominant run.
    assert order_sem[0] != order_ctx[0], "expected a reorder when the weights flipped"
    # And the new weights are reflected in the breakdown the client sees.
    assert second["suggestions"][0]["breakdown"]["weights"] == {
        "semantic": 0.95,
        "contextual": 0.05,
        "feedback": 0.0,
    }


# --------------------------------------------------------------------------- #
# 3. Validation: out-of-range / unknown key / empty body → 422
# --------------------------------------------------------------------------- #
def test_put_config_epsilon_out_of_range_returns_422(client: TestClient) -> None:
    resp = client.put("/config", json={"epsilon_explore": 1.5})
    assert resp.status_code == 422, resp.text


def test_put_config_unknown_key_returns_422(client: TestClient) -> None:
    resp = client.put("/config", json={"bogus": 1})
    assert resp.status_code == 422, resp.text


def test_put_config_empty_body_returns_422(client: TestClient) -> None:
    resp = client.put("/config", json={})
    assert resp.status_code == 422, resp.text


def test_put_config_top_k_zero_returns_422(client: TestClient) -> None:
    resp = client.put("/config", json={"top_k": 0})
    assert resp.status_code == 422, resp.text
