"""HTTP-level tests for the faceted search endpoints (C3).

Exercises ``POST /api/search`` and ``GET /api/facets`` through the
httpx ``async_client`` fixture (which drives the full FastAPI lifespan,
so ``app.state.db`` is a real aiosqlite handle against an isolated
tmp SQLite file). Data is seeded with the synthetic generator for
coverage-style tests where exact counts don't matter; correctness of
counts is already asserted at the unit layer in ``test_search.py``.

Key behaviours verified here:

* Full response shape on the happy path.
* Excluded-self rule survives the HTTP boundary.
* Pydantic guards: ``limit`` range + ``extra='forbid'`` on SearchRequest.
* The 10+-simultaneous-filters success criterion succeeds.
* ``/api/facets`` returns facets-only (no ``logs`` field).
* ``/api/facets`` accepts comma-separated filter query parameters.
"""

from __future__ import annotations

from httpx import AsyncClient

from src.search.query_builder import FACET_DIMS


# ---------------------------------------------------------------------------
# POST /api/search
# ---------------------------------------------------------------------------

async def test_post_search_happy_path(async_client: AsyncClient):
    """POST /api/search {} -> 200, response body has all required keys."""
    gen = await async_client.post("/api/logs/generate?count=500&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.post("/api/search", json={})
    assert resp.status_code == 200, resp.text
    body = resp.json()

    expected_keys = {
        "logs",
        "total_count",
        "has_more",
        "next_cursor",
        "facets",
        "query_time_ms",
        "applied_filters",
    }
    assert expected_keys.issubset(body.keys()), (
        f"missing keys: {expected_keys - set(body.keys())}"
    )
    # Default limit is 10; 500 rows ingested -> has_more must be True.
    assert body["has_more"] is True
    assert isinstance(body["logs"], list)
    assert isinstance(body["facets"], list)
    # 5 facet dims in canonical order.
    assert [f["name"] for f in body["facets"]] == list(FACET_DIMS)


async def test_post_search_with_filters(async_client: AsyncClient):
    """Applying a filter -> 5 facets, excluded-self still shows siblings > 0."""
    gen = await async_client.post("/api/logs/generate?count=1000&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.post(
        "/api/search", json={"filters": {"service": ["payments"]}}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # 5 facet dims regardless of filter.
    assert len(body["facets"]) == 5
    svc_facet = next(f for f in body["facets"] if f["name"] == "service")

    # The selected value must be present and flagged selected=True.
    payments_val = next(v for v in svc_facet["values"] if v["value"] == "payments")
    assert payments_val["selected"] is True

    # Excluded-self at the HTTP boundary: at least one OTHER service
    # value must be present with count > 0. Otherwise the sidebar
    # collapses to just the selected value and users can't broaden.
    others = [v for v in svc_facet["values"] if v["value"] != "payments"]
    assert any(v["count"] > 0 for v in others), (
        f"excluded-self broken at HTTP boundary: service facet has no "
        f"non-payments values with count>0; full values={svc_facet['values']}"
    )

    # applied_filters echoes input.
    assert body["applied_filters"] == {"service": ["payments"]}


async def test_post_search_limit_out_of_range(async_client: AsyncClient):
    """SearchRequest.limit has ge=1, le=200; 0 and 999 must 422."""
    r_low = await async_client.post("/api/search", json={"limit": 0})
    assert r_low.status_code == 422, r_low.text

    r_high = await async_client.post("/api/search", json={"limit": 999})
    assert r_high.status_code == 422, r_high.text


async def test_post_search_forbid_extra_fields(async_client: AsyncClient):
    """SearchRequest has extra='forbid'; unknown top-level field must 422."""
    resp = await async_client.post("/api/search", json={"weird_field": "x"})
    assert resp.status_code == 422, resp.text


async def test_post_search_10_plus_filter_dims(async_client: AsyncClient):
    """10+ total filter values across all 5 dims must return 200, not 400/5xx."""
    gen = await async_client.post("/api/logs/generate?count=500&seed=42")
    assert gen.status_code == 201, gen.text

    # Spans all 5 dims with multiple values each -> 13 total filter values.
    body = {
        "filters": {
            "service": ["payments", "auth", "api-gateway"],
            "level": ["INFO", "WARN", "ERROR"],
            "region": ["us-east-1", "us-west-2"],
            "latency_bucket": ["0-100ms", "100-500ms"],
            "hour_bucket": [0, 12, 23],
        },
        "limit": 5,
    }
    resp = await async_client.post("/api/search", json=body)
    assert resp.status_code == 200, resp.text

    rbody = resp.json()
    assert [f["name"] for f in rbody["facets"]] == list(FACET_DIMS)
    # Basic sanity: the applied filters round-trip.
    assert set(rbody["applied_filters"].keys()) == {
        "service",
        "level",
        "region",
        "latency_bucket",
        "hour_bucket",
    }


async def test_post_search_free_text_substring(async_client: AsyncClient):
    """All returned logs for a free-text query must contain the substring."""
    gen = await async_client.post("/api/logs/generate?count=500&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.post(
        "/api/search", json={"query": "timeout", "limit": 50}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # The seeded ERROR templates include "timeout" so we'd expect some
    # hits, but the assertion stays permissive: whatever rows come
    # back MUST all contain the substring.
    for row in body["logs"]:
        assert "timeout" in row["message"], (
            f"row {row.get('id')} matched the query but message lacks 'timeout': {row['message']!r}"
        )


# ---------------------------------------------------------------------------
# GET /api/facets
# ---------------------------------------------------------------------------

async def test_get_facets_has_no_logs(async_client: AsyncClient):
    """GET /api/facets returns facets + timing ONLY -- no logs field."""
    gen = await async_client.post("/api/logs/generate?count=200&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.get("/api/facets")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert "facets" in body
    assert "query_time_ms" in body
    assert "logs" not in body, (
        "GET /api/facets must not leak a 'logs' field (it's facets-only)"
    )


async def test_get_facets_with_comma_filters(async_client: AsyncClient):
    """GET /api/facets?service=a,b&level=ERROR -> 200 with 5 facets."""
    gen = await async_client.post("/api/logs/generate?count=500&seed=42")
    assert gen.status_code == 201, gen.text

    resp = await async_client.get(
        "/api/facets", params={"service": "payments,auth", "level": "ERROR"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert len(body["facets"]) == 5
    assert [f["name"] for f in body["facets"]] == list(FACET_DIMS)
    # applied_filters should reflect the comma-split values.
    applied = body["applied_filters"]
    assert applied["service"] == ["payments", "auth"]
    assert applied["level"] == ["ERROR"]
