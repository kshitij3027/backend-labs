"""Unit tests for GET /api/v1/debug/ground-truth (C8) — the E2E verification aid.

The endpoint reads ``runtime.generator.journeys`` directly, so these tests
drive a real seeded :class:`~src.generators.LogGenerator` for the happy path
and append hand-built :class:`~src.models.JourneyRecord` rows for the edge
cases (age filtering, the [1, 600] ``max_age`` clamp, the 500-row cap,
missing generator).
"""

from __future__ import annotations

import random
import time

from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings
from src.generators import LogGenerator
from src.main import Runtime
from src.models import JourneyRecord

PATH = "/api/v1/debug/ground-truth"

#: The exact per-journey field set the E2E verifier consumes.
EXPECTED_FIELDS = {
    "correlation_id",
    "user_id",
    "sources",
    "started_at",
    "completed_at",
    "abandoned",
}


def make_client(seed: int = 42) -> tuple[TestClient, Runtime]:
    """An injected-runtime app whose generator is seeded (deterministic spawns)."""
    settings = Settings(_env_file=None, events_per_second=120)  # hermetic
    runtime = Runtime.build(settings)
    runtime.generator = LogGenerator(settings, rng=random.Random(seed))
    return TestClient(create_app(runtime=runtime)), runtime


def journey(cid: str, completed_at: float | None) -> JourneyRecord:
    """A minimal hand-built ground-truth record completing at ``completed_at``."""
    anchor = completed_at if completed_at is not None else time.time()
    return JourneyRecord(
        correlation_id=cid,
        user_id="user_1",
        sources=["web", "payment"],
        started_at=anchor - 3.0,
        completed_at=completed_at,
    )


def fetch_ids(client: TestClient, **params) -> list[str]:
    resp = client.get(PATH, params=params)
    assert resp.status_code == 200
    return [j["correlation_id"] for j in resp.json()["journeys"]]


def test_returns_generated_journeys_with_exact_fields():
    client, runtime = make_client()
    # Simulated ticks 30s in the past: every spawned journey completes within
    # ~5s of its tick, so all of them sit comfortably inside the default window.
    base = time.time() - 30.0
    for i in range(3):
        runtime.generator.generate(base + i)
    assert runtime.generator.journeys, "seeded generator must spawn journeys"

    resp = client.get(PATH)
    assert resp.status_code == 200
    journeys = resp.json()["journeys"]
    assert len(journeys) == len(runtime.generator.journeys)
    for record in journeys:
        assert set(record) == EXPECTED_FIELDS
        assert record["correlation_id"].startswith("corr_")
        assert record["user_id"].startswith("user_")
        assert record["sources"], "hop sources must be reported"
        assert record["completed_at"] >= record["started_at"]

    # Newest completion first.
    completions = [record["completed_at"] for record in journeys]
    assert completions == sorted(completions, reverse=True)


def test_incomplete_journeys_are_excluded():
    client, runtime = make_client()
    runtime.generator.journeys.append(journey("corr_pending1", None))
    runtime.generator.journeys.append(journey("corr_done0001", time.time() - 5.0))
    ids = fetch_ids(client)
    assert "corr_done0001" in ids
    assert "corr_pending1" not in ids


def test_max_age_filters_out_old_journeys():
    client, runtime = make_client()
    now = time.time()
    runtime.generator.journeys.append(journey("corr_ancient1", now - 4000.0))
    runtime.generator.journeys.append(journey("corr_fresh001", now - 5.0))
    ids = fetch_ids(client, max_age=120)
    assert "corr_fresh001" in ids
    assert "corr_ancient1" not in ids


def test_max_age_zero_is_clamped_up_to_one_second():
    client, runtime = make_client()
    now = time.time()
    runtime.generator.journeys.append(journey("corr_justnow1", now))
    runtime.generator.journeys.append(journey("corr_stale001", now - 30.0))
    ids = fetch_ids(client, max_age=0)
    # 0 clamps to 1s: the just-completed journey survives, the 30s-old one is out.
    assert "corr_justnow1" in ids
    assert "corr_stale001" not in ids


def test_max_age_is_clamped_down_to_600_seconds():
    client, runtime = make_client()
    now = time.time()
    runtime.generator.journeys.append(journey("corr_incap001", now - 500.0))
    runtime.generator.journeys.append(journey("corr_beyond01", now - 700.0))
    ids = fetch_ids(client, max_age=10000)
    # 10000 clamps to 600s: 500s-old survives, 700s-old is out.
    assert "corr_incap001" in ids
    assert "corr_beyond01" not in ids


def test_response_is_capped_at_500_newest_journeys():
    client, runtime = make_client()
    now = time.time()
    # 600 completed journeys, index 0 the newest; all within the default window.
    for i in range(600):
        runtime.generator.journeys.append(journey(f"corr_{i:08d}", now - 0.01 * i))
    ids = fetch_ids(client)
    assert len(ids) == 500
    assert ids[0] == "corr_00000000"  # newest first...
    assert f"corr_{599:08d}" not in ids  # ...and the 100 oldest were dropped


def test_runtime_without_generator_returns_empty_feed():
    settings = Settings(_env_file=None)
    runtime = Runtime(settings=settings, started_at=time.monotonic())  # nothing wired
    client = TestClient(create_app(runtime=runtime))
    resp = client.get(PATH)
    assert resp.status_code == 200
    assert resp.json() == {"journeys": []}
