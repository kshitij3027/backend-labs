"""HTTP route tests for ``src.http_server.create_app``.

We use FastAPI's :class:`~fastapi.testclient.TestClient` so the app's
lifespan (which spawns the HealthMonitor + WS broadcaster background
tasks) is exercised end-to-end. That gives us realistic coverage of:

* The static dashboard HTML at ``GET /``.
* The JSON endpoints (``/api/health``, ``/api/status``, ``/api/logs``).
* The fan-out side-effect (a single ``POST /api/logs`` should land the
  same entry in every secondary region's ``log_store``) — this is
  technically integration-level but the cost is negligible because
  TestClient drives the whole app in-process.

Each test builds a fresh app via :func:`_build_app` so state from one
test never leaks into another.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config import AppConfig
from src.http_server import create_app


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _build_app() -> tuple[FastAPI, TestClient]:
    """Build a fresh app + TestClient with the canonical 3-region config."""
    config = AppConfig.from_env(
        env={
            "REGIONS": "us-east,europe,asia",
            "PRIMARY_PREFERENCE": "us-east,europe,asia",
        }
    )
    app = create_app(config)
    return app, TestClient(app)


@pytest.fixture
def app_and_client() -> tuple[FastAPI, TestClient]:
    """Yield the app + a TestClient inside a ``with`` to fire lifespan events."""
    app, c = _build_app()
    with c as opened:
        yield app, opened


@pytest.fixture
def client(app_and_client: tuple[FastAPI, TestClient]) -> TestClient:
    """Convenience fixture for tests that don't need direct app access."""
    return app_and_client[1]


# ---------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------


def test_get_index_returns_html(client: TestClient) -> None:
    """``GET /`` returns the raw Vue dashboard HTML."""
    res = client.get("/")
    assert res.status_code == 200
    # FastAPI's HTMLResponse sets the right content-type by default.
    assert res.headers["content-type"].startswith("text/html")
    # The Vue mount point is the canonical "did the right file load" check.
    assert '<div id="app"' in res.text


# ---------------------------------------------------------------------
# GET /api/health and /api/status
# ---------------------------------------------------------------------


def test_get_health_shape(client: TestClient) -> None:
    """``GET /api/health`` returns a HealthSnapshot-shaped dict."""
    res = client.get("/api/health")
    assert res.status_code == 200
    body = res.json()

    # Top-level keys.
    assert "overall_status" in body
    assert "regions" in body
    assert "taken_at" in body
    assert "current_primary" in body

    # Three regions, current primary is the first preference entry.
    assert isinstance(body["regions"], list)
    assert len(body["regions"]) == 3
    assert body["current_primary"] == "us-east"


def test_get_status_shape(client: TestClient) -> None:
    """``GET /api/status`` is a strict alias of ``/api/health``."""
    res = client.get("/api/status")
    assert res.status_code == 200
    body = res.json()
    assert "overall_status" in body
    assert isinstance(body["regions"], list)
    assert len(body["regions"]) == 3
    assert body["current_primary"] == "us-east"


# ---------------------------------------------------------------------
# POST /api/logs
# ---------------------------------------------------------------------


def test_post_log_returns_log_id_and_vc(client: TestClient) -> None:
    """A first write returns a log_id and ``vector_clock={us-east:1}``."""
    res = client.post(
        "/api/logs",
        json={"message": "x", "level": "info", "service": "t"},
    )
    assert res.status_code == 200
    body = res.json()

    assert isinstance(body["log_id"], str) and body["log_id"]
    # The first write at us-east advances its slot to 1; the secondary
    # slots are unobserved at this point so they don't appear yet.
    assert body["vector_clock"] == {"us-east": 1}
    assert body["logical_ts"] == 1
    assert body["region"] == "us-east"


def test_post_log_then_get_logs_returns_entry(client: TestClient) -> None:
    """After a write, ``GET /api/logs`` lists the entry from the primary."""
    write_res = client.post(
        "/api/logs",
        json={"message": "hello", "level": "info", "service": "test"},
    )
    log_id = write_res.json()["log_id"]

    res = client.get("/api/logs")
    assert res.status_code == 200
    entries = res.json()
    assert isinstance(entries, list)
    assert len(entries) == 1
    assert entries[0]["log_id"] == log_id
    # The entry payload round-trips intact.
    assert entries[0]["data"] == {"message": "hello", "level": "info", "service": "test"}


def test_get_logs_respects_limit(client: TestClient) -> None:
    """``?limit=2`` caps the returned list to two entries."""
    for i in range(5):
        client.post(
            "/api/logs",
            json={"message": f"m{i}", "level": "info", "service": "test"},
        )

    res = client.get("/api/logs?limit=2")
    assert res.status_code == 200
    entries = res.json()
    assert len(entries) == 2


# ---------------------------------------------------------------------
# Replication side-effect — integration-level but cheap
# ---------------------------------------------------------------------


def test_post_log_replicates_to_secondaries(
    app_and_client: tuple[FastAPI, TestClient],
) -> None:
    """A single write must land in every secondary's log_store too.

    TestClient drives the lifespan, so the controller fans out via the
    same code path as production. We assert the entry appears in
    europe + asia by reaching into ``app.state.regions`` directly.
    """
    app, client = app_and_client
    res = client.post(
        "/api/logs",
        json={"message": "fan-out", "level": "info", "service": "test"},
    )
    assert res.status_code == 200
    log_id = res.json()["log_id"]

    regions = app.state.regions
    assert log_id in regions["us-east"].log_store
    assert log_id in regions["europe"].log_store
    assert log_id in regions["asia"].log_store
