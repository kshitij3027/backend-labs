"""Unit tests for GET /api/v1/logs/recent — count clamping, ordering, and shape.

The app gets a hand-wired Runtime whose collector was pre-ticked with a seeded
generator (pipeline off, no Redis store), so the endpoint reads a deterministic
in-memory buffer and never touches the network.
"""

import random
import time

import pytest
from fastapi.testclient import TestClient

from src.aggregation import MetricAggregator
from src.api import create_app
from src.collector import LogCollector
from src.config import Settings
from src.generators import LogGenerator
from src.main import Runtime

EPOCH = 1000.0
#: ~120 eps x 12 simulated seconds -> comfortably more than 500 buffered events,
#: so the count=9999 clamp case can assert an exact 500.
TICKS = 12


@pytest.fixture(scope="module")
def ticked_client() -> TestClient:
    """A TestClient over a Runtime whose collector was pre-filled by manual ticks."""
    settings = Settings(_env_file=None, events_per_second=120)
    generator = LogGenerator(settings, rng=random.Random(42))
    collector = LogCollector(settings, generator, MetricAggregator(), store=None)
    for i in range(TICKS):
        collector.tick(EPOCH + i)
    runtime = Runtime(
        settings=settings,
        started_at=time.monotonic(),
        generator=generator,
        aggregator=collector.aggregator,
        collector=collector,
    )
    return TestClient(create_app(runtime=runtime))


def test_default_count_is_50(ticked_client):
    body = ticked_client.get("/api/v1/logs/recent").json()
    assert set(body) == {"events"}  # the response key is exactly "events"
    assert len(body["events"]) == 50


def test_explicit_count(ticked_client):
    resp = ticked_client.get("/api/v1/logs/recent", params={"count": 5})
    assert resp.status_code == 200
    assert len(resp.json()["events"]) == 5


def test_count_zero_clamps_to_one(ticked_client):
    resp = ticked_client.get("/api/v1/logs/recent", params={"count": 0})
    assert resp.status_code == 200  # silent clamp — never a 422
    assert len(resp.json()["events"]) == 1


def test_count_overshoot_clamps_to_500(ticked_client):
    resp = ticked_client.get("/api/v1/logs/recent", params={"count": 9999})
    assert resp.status_code == 200
    assert len(resp.json()["events"]) == 500


def test_events_are_newest_first_logevent_dicts(ticked_client):
    events = ticked_client.get("/api/v1/logs/recent").json()["events"]
    stamps = [ev["timestamp"] for ev in events]
    assert stamps == sorted(stamps, reverse=True)  # newest first, non-increasing
    for key in ("id", "timestamp", "source", "service", "level", "message", "raw"):
        assert key in events[0]


def test_missing_collector_degrades_to_empty_list():
    # A bare Runtime (no pipeline wired) must yield an empty feed, not a 500.
    bare = Runtime(settings=Settings(_env_file=None), started_at=time.monotonic())
    client = TestClient(create_app(runtime=bare))
    resp = client.get("/api/v1/logs/recent")
    assert resp.status_code == 200
    assert resp.json() == {"events": []}
