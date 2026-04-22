"""HTTP integration tests for the ingest routers.

These exercise the FastAPI app end-to-end through the ASGI transport
(no real network), verifying that request/response shapes match the
documented schema, that pydantic's validation catches the empty-
payload cases with a 422, and that the ``index_version`` echoed back
advances monotonically as expected.

Each test starts from a fresh :class:`~src.index.inverted_index.InvertedIndex`
so counts and versions are deterministic — the autouse fixture swaps
a brand-new index onto ``app.state`` before every test runs.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from src.config import get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.main import app


@pytest_asyncio.fixture(autouse=True)
async def _fresh_index():
    """Rebuild the index on ``app.state`` before each test.

    The module-level ``app`` is shared across all tests in the
    suite, so without this reset a test that ingests five docs
    would leak those docs into the next test. Overwriting the
    attribute is the simplest way to guarantee isolation — and it's
    safe because nothing else holds a reference to the old index by
    the time ``yield`` fires.
    """
    settings = get_settings()
    tokenizer = LogTokenizer(settings)
    app.state.index = InvertedIndex(settings, tokenizer)
    app.state.tokenizer = tokenizer
    yield
    # No teardown — the next test's fixture overwrites the state.


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_post_logs_single_returns_202_and_expected_shape(
    async_client,
) -> None:
    """``POST /api/logs`` returns 202 with the documented body shape."""
    resp = await async_client.post(
        "/api/logs", json={"message": "hello", "timestamp": 1.0}
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 1
    assert body["first_doc_id"] == 0
    assert body["last_doc_id"] == 0
    assert body["index_version"] == 1


@pytest.mark.asyncio
async def test_post_logs_bulk_three_entries(async_client) -> None:
    """``POST /api/logs/bulk`` returns the doc-id range and accepted count."""
    resp = await async_client.post(
        "/api/logs/bulk",
        json={
            "entries": [
                {"message": "a", "timestamp": 1.0},
                {"message": "b", "timestamp": 2.0},
                {"message": "c", "timestamp": 3.0},
            ]
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 3
    assert body["first_doc_id"] == 0
    assert body["last_doc_id"] == 2
    # Bulk bumps the version exactly once per batch — see
    # ``InvertedIndex.add_bulk``.
    assert body["index_version"] == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_post_logs_bulk_empty_list_rejected(async_client) -> None:
    """An empty ``entries`` array fails pydantic validation (422)."""
    resp = await async_client.post("/api/logs/bulk", json={"entries": []})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_post_logs_empty_message_rejected(async_client) -> None:
    """Messages must be non-empty — ``""`` trips ``min_length=1``."""
    resp = await async_client.post(
        "/api/logs", json={"message": "", "timestamp": 1.0}
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Version monotonicity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_index_version_monotonically_increases(async_client) -> None:
    """Each successful write advances ``index_version`` strictly.

    Three sequential singletons should produce 1, 2, 3 — the cache
    layer in commit 09 relies on this strict-monotonic property to
    invalidate stale entries without an explicit bust.
    """
    previous = 0
    for i in range(3):
        resp = await async_client.post(
            "/api/logs", json={"message": f"msg {i}", "timestamp": float(i)}
        )
        assert resp.status_code == 202
        version = resp.json()["index_version"]
        assert version > previous
        previous = version
    assert previous == 3
