"""Integration tests for ``GET /v1/reports/{framework}``.

Runs the full FastAPI app under ``LifespanManager`` so the lifespan
fires (DB init, scheduler start, policy load) and ``app.state`` is
fully populated before the route handler executes. Mirrors the
``test_lifespan_wiring`` and ``test_ingest_and_jobs`` fixture pattern
verbatim — fresh per-test temp DB + storage root, env-driven settings,
and a re-imported ``src.main`` so each test sees its own app object.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest_asyncio.fixture
async def app_under_test(tmp_path, monkeypatch) -> AsyncIterator[tuple]:
    """Fresh app instance with per-test DB and storage root.

    Wraps the app in ``LifespanManager`` so the lifespan fires (DB init,
    scheduler start, policy load). ``ASGITransport`` alone doesn't drive
    lifespan events — without the manager ``app.state.session_factory``
    would still be unset when the route handler tried to read it.
    """
    db_path = tmp_path / "test.db"
    storage_root = tmp_path / "tiers"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv(
        "POLICY_CONFIG_PATH", str(PROJECT_ROOT / "config" / "retention_config.yaml")
    )
    # Long intervals so the background scheduler doesn't fire during the
    # test window — we exercise the report route synchronously.
    monkeypatch.setenv("SCAN_INTERVAL_SEC", "3600")
    monkeypatch.setenv("APPLY_INTERVAL_SEC", "3600")
    monkeypatch.setenv("SWEEP_INTERVAL_SEC", "3600")

    from src.settings import get_settings

    get_settings.cache_clear()
    for mod_name in list(sys.modules.keys()):
        if mod_name == "src.main":
            del sys.modules[mod_name]

    import src.main as main_module  # noqa: E402

    importlib.reload(main_module)
    app = main_module.app

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client


@pytest.mark.asyncio
async def test_get_report_unknown_framework_returns_400(app_under_test):
    """An unsupported framework slug returns HTTP 400 with a helpful detail.

    The detail message must enumerate all five supported framework slugs
    (``gdpr``, ``sox``, ``hipaa``, ``pci_dss``, ``soc2``) so an operator
    can self-serve from the error alone — no docs lookup required.
    """
    _app, client = app_under_test
    r = await client.get("/v1/reports/voightkampff")
    assert r.status_code == 400, r.text
    body = r.json()
    assert "detail" in body
    assert "voightkampff" in body["detail"]
    # Detail should hint at the supported set so an operator can self-serve.
    for fw in ("gdpr", "sox", "hipaa", "pci_dss", "soc2"):
        assert fw in body["detail"], (fw, body["detail"])


@pytest.mark.asyncio
async def test_get_report_gdpr_returns_200_with_bundle_shape(app_under_test):
    """The GDPR report returns 200 with all top-level bundle keys present."""
    _app, client = app_under_test
    r = await client.get("/v1/reports/gdpr")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["framework"] == "gdpr"
    assert "compliance_score" in body
    assert "violations" in body
    assert "policies_in_scope" in body
    assert "files_in_scope" in body
    assert "audit_in_range" in body
    assert "time_range" in body
    assert "generated_at" in body
    assert "extras" in body
    # Score must be a float between 0 and 100 inclusive.
    assert isinstance(body["compliance_score"], (int, float))
    assert 0.0 <= float(body["compliance_score"]) <= 100.0


@pytest.mark.asyncio
async def test_get_report_sox_returns_200(app_under_test):
    """The SOX report renders successfully against the default policy YAML."""
    _app, client = app_under_test
    r = await client.get("/v1/reports/sox")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["framework"] == "sox"
    # The default config includes one SOX policy (payment_logs_sox).
    assert len(body["policies_in_scope"]) >= 1
    assert all(
        p["compliance_tag"] == "sox" for p in body["policies_in_scope"]
    )


@pytest.mark.asyncio
async def test_get_report_hipaa_returns_200(app_under_test):
    """The HIPAA report renders successfully against the default policy YAML."""
    _app, client = app_under_test
    r = await client.get("/v1/reports/hipaa")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["framework"] == "hipaa"
    # The default config includes one HIPAA policy (health_records_hipaa).
    assert len(body["policies_in_scope"]) >= 1
    assert all(
        p["compliance_tag"] == "hipaa" for p in body["policies_in_scope"]
    )


@pytest.mark.asyncio
async def test_get_report_pci_returns_200(app_under_test):
    """The PCI DSS report renders successfully against the default policy YAML."""
    _app, client = app_under_test
    r = await client.get("/v1/reports/pci_dss")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["framework"] == "pci_dss"
    # The default config includes one PCI policy (card_data_pci).
    assert len(body["policies_in_scope"]) >= 1
    assert all(
        p["compliance_tag"] == "pci_dss" for p in body["policies_in_scope"]
    )
    # PCI renderer exposes ``cardholder_data_segments`` in extras even
    # when no files have been ingested yet (0 is a valid value).
    assert "cardholder_data_segments" in body["extras"]


@pytest.mark.asyncio
async def test_get_report_soc2_returns_200(app_under_test):
    """The SOC 2 report renders successfully and reports chain integrity status."""
    _app, client = app_under_test
    r = await client.get("/v1/reports/soc2")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["framework"] == "soc2"
    # The default config includes one SOC2 policy (ops_audit_soc2).
    assert len(body["policies_in_scope"]) >= 1
    assert all(
        p["compliance_tag"] == "soc2" for p in body["policies_in_scope"]
    )
    # SOC2 renderer always reports chain integrity status — fresh DB has
    # only the genesis row, which verifies cleanly.
    assert body["extras"]["chain_integrity_status"] == "VALID"
    assert body["violations"] == []


@pytest.mark.asyncio
async def test_get_report_with_time_range_query(app_under_test):
    """Query params ``from`` and ``to`` round-trip into the bundle's time_range."""
    _app, client = app_under_test
    r = await client.get(
        "/v1/reports/gdpr",
        params={
            "from": "2026-01-01T00:00:00",
            "to": "2026-06-30T23:59:59",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["time_range"]["from"] == "2026-01-01T00:00:00"
    assert body["time_range"]["to"] == "2026-06-30T23:59:59"
