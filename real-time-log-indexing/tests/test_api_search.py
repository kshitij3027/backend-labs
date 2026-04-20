"""Tests for ``GET /api/search``.

Every test seeds documents directly via ``app.state.index`` rather
than through Redis — that keeps the test hermetic (no stream timing
dependency) and still exercises the exact indexing code path the
real consumer goes through.

Coverage
--------

* input validation (missing / empty ``q``; ``limit`` bounds) → 422
* empty-index case returns the full response shape with zero hits
* single-term search finds seeded docs and each result carries the
  required :class:`SearchResult` fields
* post-filters (``service`` / ``level``) narrow the result set
* ``limit`` is honoured
* highlighting wraps matched terms in ``<mark>``
* tokenised ``terms`` are surfaced on the response so the UI can
  render highlights that match the server
* multi-term queries apply AND semantics
* response-shape parity with the :class:`SearchResponse` pydantic
  model so future schema drift is caught loudly
"""

from __future__ import annotations

import time as _time

from fastapi import FastAPI
from httpx import AsyncClient

from src.models import LogEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _seed(index, n: int = 10, prefix: str = "hello world") -> None:
    """Add ``n`` LogEntries with a known ``prefix`` plus a unique suffix.

    Alternating ``service`` and rotating ``level`` give us enough
    variety to exercise the service/level filters without needing a
    second helper.
    """
    for i in range(n):
        await index.add_document(
            LogEntry(
                doc_id=0,
                message=f"{prefix} doc{i}",
                timestamp=_time.time(),
                service="svc-a" if i % 2 == 0 else "svc-b",
                level="INFO" if i % 3 != 0 else "ERROR",
            )
        )


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

async def test_search_requires_q(async_client: AsyncClient) -> None:
    """Missing ``q`` must yield a 422 from FastAPI's Query validator."""
    r = await async_client.get("/api/search")
    assert r.status_code == 422


async def test_search_empty_q_rejected(async_client: AsyncClient) -> None:
    """Empty ``q`` (zero-length) must fail ``min_length=1`` → 422."""
    r = await async_client.get("/api/search?q=")
    assert r.status_code == 422


async def test_search_limit_bounds(async_client: AsyncClient) -> None:
    """``limit`` below 1 or above 500 must be rejected as 422."""
    r1 = await async_client.get("/api/search?q=x&limit=0")
    assert r1.status_code == 422

    r2 = await async_client.get("/api/search?q=x&limit=1000")
    assert r2.status_code == 422


# ---------------------------------------------------------------------------
# Empty-index + response shape
# ---------------------------------------------------------------------------

async def test_search_returns_empty_when_no_data(
    async_client: AsyncClient,
) -> None:
    """With no docs indexed, we still get a well-formed envelope."""
    r = await async_client.get("/api/search?q=anything")
    assert r.status_code == 200

    body = r.json()
    assert body["results"] == []
    assert body["total"] == 0
    assert body["query"] == "anything"


async def test_search_response_shape(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """The response keys must match ``SearchResponse`` exactly."""
    await _seed(app_instance.state.index, n=1)

    r = await async_client.get("/api/search?q=hello")
    assert r.status_code == 200

    body = r.json()
    required = {"results", "total", "took_ms", "query", "terms"}
    assert set(body.keys()) == required


# ---------------------------------------------------------------------------
# Happy-path search
# ---------------------------------------------------------------------------

async def test_search_finds_indexed_docs(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """Single-term search returns seeded docs with the full result shape."""
    await _seed(app_instance.state.index, n=5, prefix="payment failed")

    r = await async_client.get("/api/search?q=payment")
    assert r.status_code == 200

    body = r.json()
    assert body["total"] >= 5
    for res in body["results"]:
        assert "doc_id" in res
        assert "message" in res
        assert "highlighted_message" in res
        assert "timestamp" in res
        assert "service" in res
        assert "level" in res

    # took_ms is populated and sane.
    assert isinstance(body["took_ms"], (int, float))
    assert body["took_ms"] >= 0


async def test_search_limit_applied(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """Explicit ``limit`` caps the returned result list."""
    await _seed(app_instance.state.index, n=20, prefix="bulk token")

    r = await async_client.get("/api/search?q=bulk&limit=5")
    assert r.status_code == 200

    body = r.json()
    assert len(body["results"]) == 5


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

async def test_search_service_filter(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """``service`` narrows results to exact matches only."""
    await _seed(app_instance.state.index, n=10, prefix="query match")

    r = await async_client.get("/api/search?q=query&service=svc-a")
    assert r.status_code == 200
    for res in r.json()["results"]:
        assert res["service"] == "svc-a"


async def test_search_level_filter(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """``level`` narrows results to exact matches only."""
    await _seed(app_instance.state.index, n=9, prefix="level test")

    r = await async_client.get("/api/search?q=level&level=ERROR")
    assert r.status_code == 200
    for res in r.json()["results"]:
        assert res["level"] == "ERROR"


# ---------------------------------------------------------------------------
# Highlighting + tokenised terms surface
# ---------------------------------------------------------------------------

async def test_search_highlights_term(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """``highlighted_message`` wraps matches in ``<mark>`` tags."""
    await _seed(app_instance.state.index, n=3, prefix="auth failure")

    r = await async_client.get("/api/search?q=auth")
    assert r.status_code == 200

    body = r.json()
    assert body["total"] > 0
    assert any(
        "<mark>" in res["highlighted_message"].lower() for res in body["results"]
    )


async def test_search_terms_in_response(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """The response echoes back the tokenised query terms."""
    await _seed(app_instance.state.index, n=2)

    r = await async_client.get("/api/search?q=hello%20world")
    assert r.status_code == 200

    body = r.json()
    assert "hello" in body["terms"]
    assert "world" in body["terms"]


# ---------------------------------------------------------------------------
# AND semantics across terms
# ---------------------------------------------------------------------------

async def test_search_multi_term_and(
    async_client: AsyncClient, app_instance: FastAPI
) -> None:
    """Multi-term queries intersect — only docs containing every term match."""
    index = app_instance.state.index

    await index.add_document(
        LogEntry(
            doc_id=0,
            message="auth failed user",
            timestamp=_time.time(),
            service="a",
            level="INFO",
        )
    )
    await index.add_document(
        LogEntry(
            doc_id=0,
            message="db failed connection",
            timestamp=_time.time(),
            service="b",
            level="INFO",
        )
    )

    r = await async_client.get("/api/search?q=auth%20failed")
    assert r.status_code == 200

    body = r.json()
    assert body["total"] == 1
    assert "auth failed user" in body["results"][0]["message"]
