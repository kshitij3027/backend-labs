"""Integration tests for ``POST /api/redact``.

Coverage:

* Spec-verification: the sample request from ``project_requirements.md``
  §8 (User SSN 123-45-6789 logged in) returns ``"***-**-6789"`` once the
  healthcare preset (partial SSN) is hot-loaded.
* Batch of 10 entries returns 10 results in order.
* Malformed input (missing ``log_entries``) → 422.
* Empty batch returns 200 + empty list.
* Default ``general`` preset masks SSN to all asterisks.
* PHI fixture (healthcare preset): MRN partial + SSN partial.
* Response shape includes ``message``/``timestamp``/``level``/``redactions``.
* Custom extra fields round-trip via ``extra="allow"``.
* ``/api/stats`` reflects the redactions afterwards.
* ``/metrics`` endpoint reachable and contains ``redactions_total``.

Why we drive the app via ``ASGITransport`` + ``LifespanManager``
---------------------------------------------------------------
The lifespan handler in ``src.main`` builds every singleton at startup
(processor, config manager, audit logger, ...). A plain
``httpx.AsyncClient`` against an in-process app bypasses lifespan
entirely, so ``app.state.processor`` would be unset and the route
would AttributeError. ``LifespanManager`` runs the startup + shutdown
phases like uvicorn would.
"""
from __future__ import annotations

import json
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

# tests/integration/test_api_redact.py → root is three levels up.
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """Yield an :class:`httpx.AsyncClient` driving the real FastAPI app.

    Wraps the app in ``LifespanManager`` so the C7 startup builds every
    singleton (processor, config manager, audit logger, stats). The
    transport is ASGI-in-process so no actual TCP socket is opened —
    requests flow straight through the ASGI callable.
    """
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


@pytest.fixture
def healthcare_config_dict() -> dict:
    """Return the healthcare preset as a plain dict for POST /api/config."""
    cfg = load_preset("healthcare", CONFIG_DIR)
    return cfg.model_dump()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spec_sample_partial_ssn_after_loading_healthcare(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """Spec sample (§8): SSN partially redacted to ``***-**-6789``.

    The default preset is ``general`` which masks SSN to all asterisks.
    The spec's expected output requires the ``partial`` strategy — we
    hot-load the healthcare preset via POST /api/config first, then
    post the sample log line and assert the partial output.
    """
    # Load healthcare preset (SSN = partial) so we get ***-**-6789.
    cfg_resp = await client.post("/api/config", json=healthcare_config_dict)
    assert cfg_resp.status_code == 200, cfg_resp.text

    sample = {
        "log_entries": [
            {
                "message": "User SSN 123-45-6789 logged in",
                "timestamp": "2025-05-01T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    resp = await client.post("/api/redact", json=sample)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "processed_entries" in body
    assert len(body["processed_entries"]) == 1
    entry = body["processed_entries"][0]
    # Spec-defined output substring.
    assert "***-**-6789" in entry["message"]
    # Plaintext must NOT appear in the response.
    assert "123-45-6789" not in entry["message"]


@pytest.mark.asyncio
async def test_batch_of_ten_returns_ten_results(client: AsyncClient) -> None:
    """A batch of 10 distinct entries returns 10 processed entries in order."""
    entries = [
        {
            "message": f"entry number {i} with no PII",
            "timestamp": "2026-05-19T10:00:00Z",
            "level": "INFO",
        }
        for i in range(10)
    ]
    resp = await client.post("/api/redact", json={"log_entries": entries})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["processed_entries"]) == 10
    # Order preserved.
    for i, processed in enumerate(body["processed_entries"]):
        assert f"entry number {i}" in processed["message"]


@pytest.mark.asyncio
async def test_missing_log_entries_returns_422(client: AsyncClient) -> None:
    """Body without the required ``log_entries`` field → 422."""
    resp = await client.post("/api/redact", json={"wrong_key": []})
    assert resp.status_code == 422
    # Pydantic detail list is a list of dicts with type info.
    detail = resp.json().get("detail")
    assert isinstance(detail, list)
    assert any("log_entries" in str(err) for err in detail)


@pytest.mark.asyncio
async def test_empty_log_entries_returns_200_and_empty(
    client: AsyncClient,
) -> None:
    """An empty batch returns 200 with empty ``processed_entries`` list."""
    resp = await client.post("/api/redact", json={"log_entries": []})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"processed_entries": []}


@pytest.mark.asyncio
async def test_default_general_preset_masks_ssn_with_asterisks(
    client: AsyncClient,
) -> None:
    """Under the default preset (general), SSN strategy=mask → all stars.

    The general preset maps SSN to ``mask`` (full asterisk replacement)
    rather than ``partial``. The expected output is 11 stars (matching
    the SSN length including hyphens).
    """
    sample = {
        "log_entries": [
            {
                "message": "see SSN 123-45-6789 here",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    resp = await client.post("/api/redact", json=sample)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    redacted_msg = body["processed_entries"][0]["message"]
    # SSN literally "123-45-6789" is 11 chars total; mask -> 11 stars.
    assert "***********" in redacted_msg
    assert "123-45-6789" not in redacted_msg


@pytest.mark.asyncio
async def test_phi_fixture_under_healthcare_preset(
    client: AsyncClient, healthcare_config_dict: dict
) -> None:
    """PHI fixture under healthcare: MRN partial + SSN partial.

    Healthcare preset uses ``partial`` for both ssn and mrn. We assert
    the spec-required transforms appear in the output and the originals
    do NOT.
    """
    # Reload healthcare preset for this test (the test before may have
    # already loaded it; reload is idempotent).
    cfg_resp = await client.post("/api/config", json=healthcare_config_dict)
    assert cfg_resp.status_code == 200, cfg_resp.text

    fixture = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
    resp = await client.post(
        "/api/redact", json={"log_entries": [fixture]}
    )
    assert resp.status_code == 200, resp.text
    redacted = resp.json()["processed_entries"][0]["message"]

    # SSN partial → ***-**-6789
    assert "***-**-6789" in redacted
    # MRN partial: keep last 3 digits of the 6-digit suffix → MRN-***456
    assert "MRN-***456" in redacted
    # Originals stripped.
    assert "123-45-6789" not in redacted
    assert "MRN-123456" not in redacted


@pytest.mark.asyncio
async def test_response_entry_shape_has_required_fields(
    client: AsyncClient,
) -> None:
    """Each processed entry exposes message / timestamp / level / redactions."""
    sample = {
        "log_entries": [
            {
                "message": "user SSN 123-45-6789",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    resp = await client.post("/api/redact", json=sample)
    assert resp.status_code == 200, resp.text
    entry = resp.json()["processed_entries"][0]
    assert "message" in entry
    assert "timestamp" in entry
    assert "level" in entry
    assert "redactions" in entry
    # ``redactions`` is a list of metadata dicts; one for the SSN hit.
    assert isinstance(entry["redactions"], list)
    assert len(entry["redactions"]) >= 1
    first = entry["redactions"][0]
    # Schema fields from RedactionMetadata.
    assert set(first.keys()) == {"pattern", "strategy", "start", "end"}


@pytest.mark.asyncio
async def test_extra_caller_fields_round_trip(client: AsyncClient) -> None:
    """A caller-supplied ``request_id`` field appears unchanged in the response."""
    sample = {
        "log_entries": [
            {
                "message": "no PII here",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
                # Extra field — LogEntry has extra="allow" so it survives.
                "request_id": "abc-123",
            }
        ]
    }
    resp = await client.post("/api/redact", json=sample)
    assert resp.status_code == 200, resp.text
    entry = resp.json()["processed_entries"][0]
    assert entry.get("request_id") == "abc-123"


@pytest.mark.asyncio
async def test_stats_endpoint_reflects_redactions(client: AsyncClient) -> None:
    """After at least one redaction, /api/stats shows the activity."""
    # Drive at least one redaction so the counters bump above zero.
    sample = {
        "log_entries": [
            {
                "message": "SSN 234-56-7890 in log",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    redact_resp = await client.post("/api/redact", json=sample)
    assert redact_resp.status_code == 200, redact_resp.text

    stats_resp = await client.get("/api/stats")
    assert stats_resp.status_code == 200, stats_resp.text
    stats = stats_resp.json()
    # At least one log processed since the lifespan started.
    assert stats["logs_processed"] >= 1
    # Pattern hits include ssn (we just redacted one).
    assert "ssn" in stats["pattern_hits"]
    assert stats["pattern_hits"]["ssn"] >= 1


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_prometheus_format(
    client: AsyncClient,
) -> None:
    """GET /metrics returns Prometheus text format including our counter."""
    # Drive one redaction first so ``redactions_total`` has a non-zero
    # series — Prometheus only emits counters that have been observed.
    sample = {
        "log_entries": [
            {
                "message": "SSN 345-67-8910",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    await client.post("/api/redact", json=sample)

    resp = await client.get("/metrics")
    assert resp.status_code == 200, resp.text
    body = resp.text
    # Our custom counter name appears in the scrape output.
    assert "redactions_total" in body


@pytest.mark.asyncio
async def test_redactions_metadata_has_no_plaintext(client: AsyncClient) -> None:
    """The per-redaction metadata MUST NOT echo the matched value.

    Asserts the no-plaintext invariant of :class:`RedactionMetadata` at
    the wire level — the API response should never carry a 'value' or
    'plaintext' field even if a future schema change accidentally added
    one.
    """
    sample = {
        "log_entries": [
            {
                "message": "user SSN 456-78-9012 login",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        ]
    }
    resp = await client.post("/api/redact", json=sample)
    assert resp.status_code == 200, resp.text
    redactions = resp.json()["processed_entries"][0]["redactions"]
    assert len(redactions) >= 1
    for meta in redactions:
        # No plaintext-leaking keys.
        assert "value" not in meta
        assert "plaintext" not in meta
        assert "redacted_value" not in meta
        # The plaintext SSN must not appear ANYWHERE in this metadata.
        assert "456-78-9012" not in json.dumps(meta)
