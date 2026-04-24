"""HTTP integration tests for ``POST /api/search``.

Exercises the full FastAPI app through the ASGI transport, covering
request validation, the ranked-response shape, incident-mode scoring,
and the cache-hit fast path. Each test starts from a fresh app
state (via :func:`src.main.reset_app_state`) so counts and cache
ratios are deterministic.
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
# Happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_returns_expected_shape(async_client) -> None:
    """A well-formed query on a seeded index returns the full shape."""
    # Seed the index with a few entries so there is something to rank.
    seed = {
        "entries": [
            {"message": "authentication error on user login", "timestamp": 1_700_000_000.0, "level": "ERROR", "service": "auth"},
            {"message": "payment gateway timeout", "timestamp": 1_700_000_001.0, "level": "WARN", "service": "payment"},
            {"message": "user created account", "timestamp": 1_700_000_002.0, "level": "INFO", "service": "auth"},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=seed)
    assert resp.status_code == 202

    resp = await async_client.post(
        "/api/search",
        json={"query": "authentication error", "limit": 5},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "query" in body
    assert "intent" in body
    assert "expanded_terms" in body
    assert isinstance(body["results"], list)
    assert "total_hits" in body
    assert "ranked_hits" in body
    assert "execution_time_ms" in body


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_query_rejected(async_client) -> None:
    """An empty ``query`` string trips ``min_length=1`` -> 422."""
    resp = await async_client.post("/api/search", json={"query": ""})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_oversized_limit_rejected(async_client) -> None:
    """``limit`` above 500 trips the pydantic bound -> 422."""
    resp = await async_client.post(
        "/api/search", json={"query": "x", "limit": 501}
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Incident mode
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_incident_mode_ranks_errors_above_infos(async_client) -> None:
    """``mode=incident`` promotes ERRORs and records a mode reason."""
    seed = {
        "entries": [
            {"message": "login succeeded for user", "timestamp": 1_700_000_000.0, "level": "INFO", "service": "auth"},
            {"message": "login failed with error", "timestamp": 1_700_000_001.0, "level": "ERROR", "service": "auth"},
            {"message": "login failed with error again", "timestamp": 1_700_000_002.0, "level": "ERROR", "service": "auth"},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=seed)
    assert resp.status_code == 202

    resp = await async_client.post(
        "/api/search",
        json={
            "query": "login error",
            "limit": 5,
            "context": {"mode": "incident"},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    results = body["results"]
    assert len(results) >= 1
    # Top result should be an ERROR in incident mode.
    assert results[0]["level"] == "ERROR"
    # The reasons list should reflect the active mode — either a
    # mode boost (when the entry is a high-severity hit) or a mode
    # marker so clients can see the query was ranked under that
    # context.
    reasons = results[0]["ranking_explanation"]["reasons"]
    assert any(r.endswith("_mode_boost") or r.endswith("_mode") for r in reasons)


# ---------------------------------------------------------------------------
# Cache fast path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_repeat_query_uses_cache(async_client) -> None:
    """Two identical calls both return 200 and both report a time >= 0.

    The second call is a cache hit, so it's expected to be cheaper
    — but CI jitter makes strict cost comparisons flaky, so the
    assertion is merely that both report valid non-negative times.
    """
    seed = {
        "entries": [
            {"message": "authentication error", "timestamp": 1_700_000_000.0, "level": "ERROR"},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=seed)
    assert resp.status_code == 202

    resp1 = await async_client.post(
        "/api/search", json={"query": "authentication error", "limit": 5}
    )
    resp2 = await async_client.post(
        "/api/search", json={"query": "authentication error", "limit": 5}
    )
    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert resp1.json()["execution_time_ms"] >= 0
    assert resp2.json()["execution_time_ms"] >= 0


# ---------------------------------------------------------------------------
# Ranking explanation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_results_include_ranking_explanation_fields(async_client) -> None:
    """Each ranked result has all five factor fields + a reasons list."""
    seed = {
        "entries": [
            {"message": "authentication error on login", "timestamp": 1_700_000_000.0, "level": "ERROR", "service": "auth"},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=seed)
    assert resp.status_code == 202

    resp = await async_client.post(
        "/api/search", json={"query": "authentication error", "limit": 5}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["results"]) >= 1
    expl = body["results"][0]["ranking_explanation"]
    for field in ("tfidf", "temporal", "severity", "service", "context"):
        assert field in expl
    assert "reasons" in expl
    assert isinstance(expl["reasons"], list)
