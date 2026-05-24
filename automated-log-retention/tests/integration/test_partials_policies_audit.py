"""Integration tests for the C18 HTMX partials.

Covers two routes:

  * ``GET /partials/policies`` — fragment rendered into ``#policies-card``.
    Lists every loaded retention policy and the chain-integrity badge.
  * ``GET /partials/audit`` — fragment rendered into ``#audit-card``.
    Lists the most recent 30 audit-chain entries (newest first).

The fixture mirrors ``test_partials_tiers_stats.py``: a per-test SQLite
+ storage_root + long scheduler intervals + ``LifespanManager`` so the
full ``src.main:app`` lifespan fires (policy YAML loaded, genesis
audit row inserted by ``init_db`` → ``ensure_genesis``, ``ChainVerifier``
attached to ``app.state``). We re-import ``src.main`` per test for the
same reason — settings get pinned at app-construct time.
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
    """Spin up a fresh ``src.main:app`` against a per-test SQLite DB.

    Long scheduler intervals so nothing fires during the test window;
    we exercise the partials by hitting the routes directly via the
    in-process httpx client.

    The C18 routes also need the chain-integrity cache cleared between
    tests so one test's verify result doesn't bleed into another. We
    reset ``_INTEGRITY_CACHE`` at the start of the fixture.
    """
    db_path = tmp_path / "test.db"
    storage_root = tmp_path / "tiers"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv(
        "POLICY_CONFIG_PATH", str(PROJECT_ROOT / "config" / "retention_config.yaml")
    )
    monkeypatch.setenv("SCAN_INTERVAL_SEC", "3600")
    monkeypatch.setenv("APPLY_INTERVAL_SEC", "3600")
    monkeypatch.setenv("SWEEP_INTERVAL_SEC", "3600")

    from src.settings import get_settings

    get_settings.cache_clear()
    for mod_name in list(sys.modules.keys()):
        if mod_name == "src.main" or mod_name.startswith("src.main."):
            del sys.modules[mod_name]

    import src.main as main_module  # noqa: E402

    importlib.reload(main_module)
    app = main_module.app

    # Reset the chain-integrity cache so the verify result from the
    # previous test doesn't bleed into this one (the cache is a module-
    # level dict on ``src.api.routes``).
    from src.api import routes as routes_module

    routes_module._INTEGRITY_CACHE["at"] = 0.0
    routes_module._INTEGRITY_CACHE["result"] = None

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client


# --- /partials/policies ---------------------------------------------------


@pytest.mark.asyncio
async def test_partial_policies_returns_200_and_lists_demo_policies(app_under_test):
    """All 6 demo policies from ``config/retention_config.yaml`` appear."""
    _app, client = app_under_test
    r = await client.get("/partials/policies")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    for name in (
        "user_activity_gdpr",
        "payment_logs_sox",
        "health_records_hipaa",
        "card_data_pci",
        "ops_audit_soc2",
        "debug_logs",
    ):
        assert name in body, f"missing policy name in /partials/policies: {name}"


@pytest.mark.asyncio
async def test_partial_policies_shows_chain_valid_on_clean_db(app_under_test):
    """A fresh DB has only the genesis entry, so the chain must be VALID."""
    _app, client = app_under_test
    r = await client.get("/partials/policies")
    assert r.status_code == 200
    assert "chain: VALID" in r.text


# --- /partials/audit ------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_audit_returns_200(app_under_test):
    """Audit card renders with the canonical header even on an empty chain tail."""
    _app, client = app_under_test
    r = await client.get("/partials/audit")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/html")
    assert "Audit chain" in r.text


@pytest.mark.asyncio
async def test_partial_audit_shows_genesis_entry(app_under_test):
    """On a fresh DB the audit card must show at least the genesis row.

    ``ensure_genesis`` (invoked from ``init_db``) inserts seq=0 with
    ``actor='system'``, ``action='genesis'``, ``resource='audit_chain'``.
    All three markers should land in the rendered HTML.
    """
    _app, client = app_under_test
    r = await client.get("/partials/audit")
    assert r.status_code == 200
    body = r.text
    assert "system" in body
    assert "genesis" in body
    assert "audit_chain" in body


# --- chain-integrity cache ------------------------------------------------


@pytest.mark.asyncio
async def test_chain_integrity_cache_short_circuits(app_under_test):
    """Two back-to-back /partials/policies hits return the same status.

    The chain-integrity cache (`_INTEGRITY_CACHE`) coalesces verifier
    calls within `_INTEGRITY_TTL_SECONDS`. We can't easily assert the
    cache hit without instrumentation; this is a light-weight assertion
    that both calls succeed and report the same chain status — which is
    what the cache contract guarantees within the TTL window.
    """
    _app, client = app_under_test
    r1 = await client.get("/partials/policies")
    r2 = await client.get("/partials/policies")
    assert r1.status_code == 200
    assert r2.status_code == 200
    # Both responses must report the same chain-status string. The
    # phrase appears inside the policies-card header.
    assert "chain: VALID" in r1.text
    assert "chain: VALID" in r2.text
