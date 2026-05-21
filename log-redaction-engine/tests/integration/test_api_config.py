"""Integration tests for ``GET/POST /api/config`` and ``GET /api/stats``.

Coverage:

* GET returns the current config dict (includes ``version`` key).
* POST a valid healthcare preset → 200 and subsequent /api/redact uses
  the new strategies (proof of atomic hot-reload).
* POST with an invalid pattern_name → 422, old config still active
  (rollback proof).
* POST with malformed JSON → 422.
* POST emits a ``config_reload`` audit event.
* /api/stats returns all five documented fields.
* POST rejects unknown top-level fields → 422 (extra="forbid").

Each test is independent — the FastAPI lifespan rebuilds the config
manager seeded with ``REDACTION_PRESET=general`` (the conftest's
effective default) on every fixture invocation, so cross-test state
contamination is bounded by the fixture lifecycle.
"""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import AsyncIterator

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from src.config.loader import load_preset
from src.main import app


# ---------------------------------------------------------------------------
# Shared paths + fixtures
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Yield an :class:`httpx.AsyncClient` with the FastAPI lifespan running."""
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


@pytest.fixture
def healthcare_config_dict() -> dict:
    """Return the healthcare preset (partial SSN + partial MRN) as a dict."""
    return load_preset("healthcare", CONFIG_DIR).model_dump()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_config_returns_current_config(client: AsyncClient) -> None:
    """GET /api/config returns a dict with the documented top-level fields."""
    resp = await client.get("/api/config")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # ``version`` is mandatory in RedactionConfig.
    assert "version" in body
    # Top-level structural keys are all present.
    for key in ("version", "fields_to_redact", "rules", "audit_all_redactions"):
        assert key in body


@pytest.mark.asyncio
async def test_post_valid_healthcare_preset_hot_reloads(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """Posting healthcare → 200, and subsequent /api/redact uses partial SSN."""
    resp = await client.post("/api/config", json=healthcare_config_dict)
    assert resp.status_code == 200, resp.text
    # Response echoes the new config.
    assert resp.json()["rules"]["ssn"]["strategy"] == "partial"

    # Drive a redaction and verify the new SSN strategy is active.
    redact_resp = await client.post(
        "/api/redact",
        json={
            "log_entries": [
                {
                    "message": "SSN 123-45-6789",
                    "timestamp": "2026-05-19T10:00:00Z",
                    "level": "INFO",
                }
            ]
        },
    )
    assert redact_resp.status_code == 200, redact_resp.text
    redacted = redact_resp.json()["processed_entries"][0]["message"]
    # Healthcare → partial → keep last 4 → "***-**-6789".
    assert "***-**-6789" in redacted


@pytest.mark.asyncio
async def test_post_invalid_pattern_name_rejected_old_config_active(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """Invalid pattern_name → 422; subsequent /api/redact uses the OLD strategies."""
    # First lock in healthcare so SSN = partial.
    resp = await client.post("/api/config", json=healthcare_config_dict)
    assert resp.status_code == 200

    # Now post a structurally invalid update with a bogus pattern name.
    bad = deepcopy(healthcare_config_dict)
    bad["rules"]["NOT_A_PATTERN"] = {
        "pattern_name": "NOT_A_PATTERN",
        "strategy": "mask",
        "confidence_min": 0.9,
        "compliance_tags": [],
    }
    bad_resp = await client.post("/api/config", json=bad)
    assert bad_resp.status_code == 422

    # Old (healthcare) config is still active — proves the rollback.
    redact_resp = await client.post(
        "/api/redact",
        json={
            "log_entries": [
                {
                    "message": "SSN 123-45-6789",
                    "timestamp": "2026-05-19T10:00:00Z",
                    "level": "INFO",
                }
            ]
        },
    )
    assert redact_resp.status_code == 200, redact_resp.text
    redacted = redact_resp.json()["processed_entries"][0]["message"]
    # Healthcare's partial strategy still active → "***-**-6789".
    assert "***-**-6789" in redacted


@pytest.mark.asyncio
async def test_post_malformed_json_returns_422(client: AsyncClient) -> None:
    """A body that isn't valid JSON → 422."""
    resp = await client.post(
        "/api/config",
        content=b"{not valid json",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_post_config_records_audit_event(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """A successful /api/config POST writes a ``config_reload`` event."""
    # Snapshot the ring buffer before.
    pre = list(app.state.ring_buffer.snapshot())
    pre_count = sum(1 for e in pre if e.event_type == "config_reload")

    resp = await client.post("/api/config", json=healthcare_config_dict)
    assert resp.status_code == 200, resp.text

    post = list(app.state.ring_buffer.snapshot())
    post_count = sum(1 for e in post if e.event_type == "config_reload")
    assert post_count > pre_count


@pytest.mark.asyncio
async def test_stats_endpoint_returns_all_five_fields(
    client: AsyncClient,
) -> None:
    """/api/stats response has all five documented top-level fields."""
    resp = await client.get("/api/stats")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    required = {
        "logs_processed",
        "ops_per_second",
        "avg_latency_ms",
        "p95_latency_ms",
        "pattern_hits",
    }
    assert set(body.keys()) == required
    # Type sanity.
    assert isinstance(body["logs_processed"], int)
    assert isinstance(body["ops_per_second"], (int, float))
    assert isinstance(body["avg_latency_ms"], (int, float))
    assert isinstance(body["p95_latency_ms"], (int, float))
    assert isinstance(body["pattern_hits"], dict)


@pytest.mark.asyncio
async def test_post_unknown_top_level_field_rejected(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """Extra top-level field → 422 (RedactionConfig has ``extra="forbid"``)."""
    bad = deepcopy(healthcare_config_dict)
    bad["some_new_field"] = "definitely not allowed"
    resp = await client.post("/api/config", json=bad)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_config_after_post_reflects_change(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """GET after POST returns the newly-loaded config."""
    post_resp = await client.post("/api/config", json=healthcare_config_dict)
    assert post_resp.status_code == 200, post_resp.text

    get_resp = await client.get("/api/config")
    assert get_resp.status_code == 200, get_resp.text
    body = get_resp.json()
    # Healthcare's SSN strategy is partial (vs general's mask).
    assert body["rules"]["ssn"]["strategy"] == "partial"
    # Healthcare's compliance set is HIPAA-only.
    assert body["active_compliance_sets"] == ["HIPAA"]
