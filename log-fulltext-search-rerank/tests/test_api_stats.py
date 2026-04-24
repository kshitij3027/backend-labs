"""HTTP integration tests for ``GET /api/search/stats``.

Verifies the stats endpoint's contract in three scenarios:
1. Fresh app (all zeros).
2. After a bulk ingest (total_docs + unique_tokens + index_version).
3. After running searches (cache hit ratio climbs appropriately).
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from src.main import app, reset_app_state


@pytest_asyncio.fixture(autouse=True)
async def _fresh_state():
    """Rebuild every app-state component before each test."""
    reset_app_state(app)
    yield


# ---------------------------------------------------------------------------
# Fresh state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_on_fresh_app_returns_zeros(async_client) -> None:
    """Every counter starts at 0 on a brand-new app."""
    resp = await async_client.get("/api/search/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_docs"] == 0
    assert body["unique_tokens"] == 0
    assert body["index_version"] == 0
    assert body["idf_version"] == 0
    assert body["cache_hit_ratio"] == 0.0
    assert body["p95_latency_ms"] == 0.0


# ---------------------------------------------------------------------------
# After ingest
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_after_bulk_ingest(async_client) -> None:
    """A single bulk ingest bumps the version once and updates counts."""
    bulk = {
        "entries": [
            {"message": "authentication error occurred", "timestamp": 1.0},
            {"message": "database timeout detected", "timestamp": 2.0},
            {"message": "payment succeeded for user", "timestamp": 3.0},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=bulk)
    assert resp.status_code == 202

    resp = await async_client.get("/api/search/stats")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_docs"] == 3
    assert body["unique_tokens"] > 0
    # Bulk ingest bumps the version exactly once per batch — see
    # ``InvertedIndex.add_bulk``.
    assert body["index_version"] == 1


# ---------------------------------------------------------------------------
# Cache hit ratio evolution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_hit_ratio_climbs_on_repeat_query(async_client) -> None:
    """First search is a miss (ratio 0.0); repeat hits drive ratio up."""
    bulk = {
        "entries": [
            {"message": "authentication error occurred", "timestamp": 1.0, "level": "ERROR"},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=bulk)
    assert resp.status_code == 202

    # First search = miss.
    resp = await async_client.post(
        "/api/search", json={"query": "authentication error", "limit": 5}
    )
    assert resp.status_code == 200

    stats1 = (await async_client.get("/api/search/stats")).json()
    # One miss, zero hits -> ratio 0.0.
    assert stats1["cache_hit_ratio"] == 0.0

    # Second identical search = hit.
    resp = await async_client.post(
        "/api/search", json={"query": "authentication error", "limit": 5}
    )
    assert resp.status_code == 200

    stats2 = (await async_client.get("/api/search/stats")).json()
    # Now 1 hit / 2 total -> 0.5.
    assert stats2["cache_hit_ratio"] == 0.5
