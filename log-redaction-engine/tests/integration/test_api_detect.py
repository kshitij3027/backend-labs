"""Integration tests for ``POST /v1/detect`` (dry-run detection).

Coverage:

* PHI fixture → returns MRN + SSN detections, response carries no
  ``processed_entries`` key (detect must not redact).
* Empty body → 200 with ``detections: []``.
* Plaintext NEVER in response: raw SSN substring is absent everywhere.
* ``value_preview`` matches the documented mask shape (first 2 + ***
  + last 2).
* Audit trail: one ``detect`` event lands in the ring buffer per call.
* Detection item schema: every item has all six required fields.

We share the same client fixture pattern as test_api_redact — driving
through ``LifespanManager`` so ``app.state`` is fully wired.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import AsyncIterator

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from src.main import app


# ---------------------------------------------------------------------------
# Shared paths + fixtures
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent.parent
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Yield an :class:`httpx.AsyncClient` with the FastAPI lifespan running."""
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phi_fixture_returns_mrn_and_ssn_detections(
    client: AsyncClient,
) -> None:
    """PHI fixture has both MRN-123456 and SSN 123-45-6789; detect surfaces both."""
    fixture = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
    resp = await client.post(
        "/v1/detect", json={"log_entries": [fixture]}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Detect endpoint NEVER returns processed_entries (would imply redaction).
    assert "processed_entries" not in body
    # Both an SSN and an MRN should be detected.
    patterns = {d["pattern"] for d in body["detections"]}
    assert "ssn" in patterns
    assert "mrn" in patterns


@pytest.mark.asyncio
async def test_empty_log_entries_returns_empty_detections(
    client: AsyncClient,
) -> None:
    """Empty input batch → 200 with an empty detection list."""
    resp = await client.post("/v1/detect", json={"log_entries": []})
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"detections": []}


@pytest.mark.asyncio
async def test_plaintext_never_in_response(client: AsyncClient) -> None:
    """The raw SSN string ``123-45-6789`` must NOT appear in the response."""
    fixture = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
    resp = await client.post(
        "/v1/detect", json={"log_entries": [fixture]}
    )
    assert resp.status_code == 200, resp.text
    # The literal raw SSN must not appear anywhere in the serialized body.
    assert "123-45-6789" not in resp.text
    # Same check for MRN.
    assert "MRN-123456" not in resp.text


@pytest.mark.asyncio
async def test_value_preview_masked_shape(client: AsyncClient) -> None:
    """For an SSN, preview matches ``12***89``.

    Rule (see :func:`src.api.routes._value_preview`):
    ``value[:2] + "***" + value[-2:]`` when ``len(value) >= 5``.
    SSN "123-45-6789" → "12***89".
    """
    sample = {
        "log_entries": [
            {
                "message": "user SSN 123-45-6789 here",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    resp = await client.post("/v1/detect", json=sample)
    assert resp.status_code == 200, resp.text
    detections = resp.json()["detections"]
    ssn_items = [d for d in detections if d["pattern"] == "ssn"]
    assert len(ssn_items) >= 1
    # The full SSN is "123-45-6789" → preview "12***89".
    assert ssn_items[0]["value_preview"] == "12***89"


@pytest.mark.asyncio
async def test_audit_log_records_detect_event(client: AsyncClient) -> None:
    """One detect call appends at least one ``detect`` event to the ring buffer."""
    fixture = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
    # Snapshot the audit channel BEFORE the call so we can compare deltas.
    pre = list(app.state.ring_buffer.snapshot())
    pre_detect_count = sum(1 for e in pre if e.event_type == "detect")

    resp = await client.post(
        "/v1/detect", json={"log_entries": [fixture]}
    )
    assert resp.status_code == 200, resp.text

    post = list(app.state.ring_buffer.snapshot())
    post_detect_count = sum(1 for e in post if e.event_type == "detect")
    # At least one new detect event was recorded.
    assert post_detect_count > pre_detect_count


@pytest.mark.asyncio
async def test_detection_item_schema_complete(client: AsyncClient) -> None:
    """Each DetectionItem in the response has all six required fields."""
    sample = {
        "log_entries": [
            {
                "message": "user 234-56-7890 SSN here",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    resp = await client.post("/v1/detect", json=sample)
    assert resp.status_code == 200, resp.text
    detections = resp.json()["detections"]
    assert len(detections) >= 1
    required = {"entry_index", "pattern", "value_preview", "start", "end", "confidence"}
    for item in detections:
        assert set(item.keys()) == required


@pytest.mark.asyncio
async def test_entry_index_maps_correctly(client: AsyncClient) -> None:
    """``entry_index`` correctly identifies which input entry produced the hit."""
    sample = {
        "log_entries": [
            # entry 0: no PII
            {
                "message": "system startup",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            },
            # entry 1: SSN
            {
                "message": "SSN 345-67-8910 here",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            },
        ]
    }
    resp = await client.post("/v1/detect", json=sample)
    assert resp.status_code == 200, resp.text
    detections = resp.json()["detections"]
    # All SSN detections should map to entry_index 1 (the second entry).
    ssn_indices = [d["entry_index"] for d in detections if d["pattern"] == "ssn"]
    assert ssn_indices == [1]
