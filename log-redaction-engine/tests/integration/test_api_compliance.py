"""Integration tests for ``GET /api/compliance/{rule_set}`` (C8).

Coverage:

* After a redaction batch containing MRN + SSN (HIPAA-tagged in the
  default preset), the HIPAA report exposes ``breakdown.mrn >= 1``.
* ``GET /api/compliance/INVALID`` → 422 from the closed Literal.
* ``GET /api/compliance/GDPR`` returns valid JSON with a non-negative
  total even when no GDPR-tagged event has fired yet.
* A freshly-started app (no redactions) returns 200 with
  ``total_redactions == 0`` and well-formed empty dicts.

We drive the FastAPI app through ``ASGITransport`` + ``LifespanManager``
so the audit ring buffer is wired correctly and routes can resolve
``request.app.state.ring_buffer`` — same pattern as test_api_redact.
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

# tests/integration/test_api_compliance.py → repo root is three levels up.
PROJECT_ROOT = Path(__file__).parent.parent.parent
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Yield an :class:`httpx.AsyncClient` driving the real FastAPI app.

    Wraps the app in ``LifespanManager`` so the startup builds every
    singleton (processor, config manager, audit logger, ring buffer,
    stats). The transport is ASGI-in-process — no actual TCP socket is
    opened; requests flow straight through the ASGI callable.
    """
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hipaa_report_after_phi_redaction_includes_mrn(
    client: AsyncClient,
) -> None:
    """POST PHI fixture → GET /api/compliance/HIPAA shows breakdown.mrn >= 1.

    The default preset tags ``mrn`` and ``ssn`` with ``HIPAA``, so a
    single PHI-fixture redaction emits two HIPAA-tagged audit events
    (one per pattern). The HIPAA compliance report must surface both.
    """
    # Drive at least one redaction so the ring buffer carries HIPAA
    # events. PHI fixture has both MRN and SSN strings.
    fixture = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
    redact_resp = await client.post("/api/redact", json={"log_entries": [fixture]})
    assert redact_resp.status_code == 200, redact_resp.text

    resp = await client.get("/api/compliance/HIPAA")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    # Shape checks — every required field on the response model is present.
    assert body["rule_set"] == "HIPAA"
    assert "generated_at" in body
    assert "report_window_start" in body
    assert "report_window_end" in body
    assert "total_redactions" in body
    assert "breakdown" in body
    assert "strategies_used" in body
    assert "report_generation_time_ms" in body

    # Spec acceptance criterion: HIPAA report after MRN redaction has
    # breakdown.mrn >= 1.
    assert body["breakdown"].get("mrn", 0) >= 1
    # SSN is also HIPAA-tagged in the default preset; the PHI fixture
    # contains an SSN so it should also appear.
    assert body["breakdown"].get("ssn", 0) >= 1
    # Total reflects the per-pattern entries.
    assert body["total_redactions"] >= 2


@pytest.mark.asyncio
async def test_invalid_rule_set_returns_422(client: AsyncClient) -> None:
    """``GET /api/compliance/INVALID`` → 422 from the Literal validator.

    FastAPI sees the path parameter is typed with a closed Literal and
    surfaces a structured 422 without ever entering the handler.
    """
    resp = await client.get("/api/compliance/INVALID")
    assert resp.status_code == 422
    # Pydantic-style error detail list.
    detail = resp.json().get("detail")
    assert isinstance(detail, list)
    # The error should call out the offending path parameter.
    assert any("rule_set" in str(err) for err in detail)


@pytest.mark.asyncio
async def test_gdpr_report_returns_valid_json(client: AsyncClient) -> None:
    """``GET /api/compliance/GDPR`` → 200 with a well-formed report body.

    The GDPR report may have zero matches depending on what has been
    redacted (the default preset tags email / phone / person / org as
    GDPR but the PHI fixture doesn't trigger them). The endpoint must
    still return a 200 with ``total_redactions >= 0``.
    """
    resp = await client.get("/api/compliance/GDPR")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["rule_set"] == "GDPR"
    assert isinstance(body["total_redactions"], int)
    assert body["total_redactions"] >= 0
    # Empty when no GDPR-tagged event has fired; non-empty otherwise.
    assert isinstance(body["breakdown"], dict)
    assert isinstance(body["strategies_used"], dict)


@pytest.mark.asyncio
async def test_empty_audit_log_returns_zero_total(client: AsyncClient) -> None:
    """Brand-new lifespan with no redactions yet → ``total_redactions == 0``.

    The ``client`` fixture brings up a fresh ``LifespanManager`` per
    test, so the ring buffer is empty at the start of THIS test
    (no other test in this module ran against the same fixture
    instance). Querying any rule_set must return a well-formed zero.
    """
    resp = await client.get("/api/compliance/HIPAA")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["total_redactions"] == 0
    assert body["breakdown"] == {}
    assert body["strategies_used"] == {}
    # Window bounds present and well-ordered even on an empty buffer.
    assert "report_window_start" in body
    assert "report_window_end" in body
