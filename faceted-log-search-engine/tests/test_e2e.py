"""End-to-end integration tests against the running ``app`` container.

Unlike the other test modules — which exercise the FastAPI app in
process via ``httpx.ASGITransport`` — this file drives **real HTTP**
against the dockerized ``app`` service. It is invoked by
``make e2e``, which:

  1. ``./start.sh`` — bring up ``app`` + ``redis`` via docker-compose.
  2. ``docker compose --profile test run --rm test pytest -v tests/test_e2e.py``
  3. ``./stop.sh`` — tear the stack back down.

Inside the ``test`` container the app is reachable as ``http://app:8000``
(docker-compose DNS). Locally, override with ``APP_URL=http://localhost:8000``.

The module-scoped ``_seed_once`` fixture generates 500 synthetic logs
with a fixed seed before any test runs. Every test builds its own
short-lived ``httpx.AsyncClient`` — we intentionally do NOT reuse
the ``async_client`` fixture from ``conftest.py``, because that one
wires up an in-process ASGI transport against a tmp SQLite file,
which is the opposite of what we want here.
"""

from __future__ import annotations

import asyncio
import os
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio


APP_URL = os.getenv("APP_URL", "http://app:8000")
REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


@pytest_asyncio.fixture
async def http() -> AsyncIterator[httpx.AsyncClient]:
    """Per-test httpx client pointed at the live app container."""
    async with httpx.AsyncClient(
        base_url=APP_URL, timeout=REQUEST_TIMEOUT
    ) as client:
        yield client


@pytest_asyncio.fixture(scope="module", autouse=True)
async def _seed_once() -> None:
    """Seed 500 deterministic logs once before any e2e test runs.

    Runs at module scope + autouse so every test sees at least 500
    rows regardless of whether previous test runs left data in the
    persistent volume. Uses a fixed seed for determinism.
    """
    async with httpx.AsyncClient(
        base_url=APP_URL, timeout=REQUEST_TIMEOUT
    ) as client:
        # Wait for app health before hammering it with data.
        deadline = 15.0
        sleep = 0.5
        elapsed = 0.0
        while elapsed < deadline:
            try:
                resp = await client.get("/health")
                if resp.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(sleep)
            elapsed += sleep
        else:
            raise RuntimeError(f"app did not become healthy within {deadline}s")

        resp = await client.post("/api/logs/generate?count=500&seed=99")
        assert resp.status_code == 201, resp.text


@pytest.mark.asyncio
async def test_e2e_health_ok(http: httpx.AsyncClient) -> None:
    """``/health`` returns 200 with db connected."""
    resp = await http.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("status") == "ok"
    assert body.get("db") == "connected"


@pytest.mark.asyncio
async def test_e2e_generate_and_search(http: httpx.AsyncClient) -> None:
    """After generate, a compound search returns all five facet dims quickly."""
    payload = {
        "filters": {"service": ["payments"], "level": ["ERROR"]},
        "limit": 5,
    }
    resp = await http.post("/api/search", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Response envelope keys present.
    assert "logs" in body
    assert "facets" in body
    assert "query_time_ms" in body
    assert "applied_filters" in body

    # All five facet dimensions must be present in the response.
    facet_names = {f["name"] for f in body["facets"]}
    assert facet_names == {
        "service",
        "level",
        "region",
        "latency_bucket",
        "hour_bucket",
    }, f"facet names mismatch: {facet_names}"
    assert len(body["facets"]) == 5

    # Generous cap; Docker adds RTT on top of actual compute.
    assert body["query_time_ms"] < 150, (
        f"query_time_ms too high: {body['query_time_ms']}"
    )


@pytest.mark.asyncio
async def test_e2e_excluded_self_holds_over_http(http: httpx.AsyncClient) -> None:
    """The service facet must still show sibling services (excluded-self)."""
    payload = {
        "filters": {"service": ["payments"], "level": ["ERROR"]},
        "limit": 5,
    }
    resp = await http.post("/api/search", json=payload)
    assert resp.status_code == 200

    facets = {f["name"]: f for f in resp.json()["facets"]}
    service_facet = facets["service"]

    # There must be at least 2 non-selected services with count > 0 — i.e.
    # the "excluded-self" rule is still holding end-to-end.
    siblings = [
        v
        for v in service_facet["values"]
        if v["value"] != "payments" and v["count"] > 0
    ]
    assert len(siblings) >= 2, (
        f"expected >= 2 non-selected services with count > 0, got: "
        f"{service_facet['values']}"
    )


@pytest.mark.asyncio
async def test_e2e_stats_reflects_seeded_rows(http: httpx.AsyncClient) -> None:
    """``/api/stats`` totals + cardinality reflect the seeded dataset."""
    resp = await http.get("/api/stats")
    assert resp.status_code == 200
    body = resp.json()

    assert body["total_logs"] >= 500, body
    card = body["facet_cardinality"]
    # service: generator draws from 5; 500 rows easily covers 4-5 of them.
    assert card["service"] in (4, 5), card
    # Every other dimension should have at least 1 distinct value.
    for dim in ("level", "region", "latency_bucket", "hour_bucket"):
        assert card[dim] > 0, card


@pytest.mark.asyncio
async def test_e2e_keyset_pagination_works(http: httpx.AsyncClient) -> None:
    """First page advertises has_more+cursor; second call returns new rows."""
    first = await http.post("/api/search", json={"limit": 10})
    assert first.status_code == 200
    page1 = first.json()
    assert page1["has_more"] is True
    assert page1["next_cursor"] is not None
    ids_page1 = {row["id"] for row in page1["logs"]}
    assert len(ids_page1) == 10

    second = await http.post(
        "/api/search",
        json={"limit": 10, "cursor": page1["next_cursor"]},
    )
    assert second.status_code == 200
    page2 = second.json()
    ids_page2 = {row["id"] for row in page2["logs"]}
    # Cursor advances us — the two pages must not share rows.
    assert ids_page1.isdisjoint(ids_page2), (
        "pages overlap; keyset pagination is broken"
    )


@pytest.mark.asyncio
async def test_e2e_dashboard_serves_html(http: httpx.AsyncClient) -> None:
    """``GET /`` returns the dashboard HTML shell."""
    resp = await http.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "Faceted Log Search" in resp.text


@pytest.mark.asyncio
async def test_e2e_cache_hit_on_repeat(http: httpx.AsyncClient) -> None:
    """Second identical search is served from the Redis cache."""
    payload = {
        "filters": {"service": ["auth"], "level": ["INFO"]},
        "limit": 3,
    }
    first = await http.post("/api/search", json=payload)
    assert first.status_code == 200

    second = await http.post("/api/search", json=payload)
    assert second.status_code == 200
    assert second.json().get("cached") is True, (
        "expected the repeat call to be a cache hit; "
        "is Redis reachable from the app container?"
    )
