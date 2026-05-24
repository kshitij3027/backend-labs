"""Integration tests for the C16 dashboard skeleton.

These cover three things:

  1. ``GET /`` renders the HTML dashboard shell with the four placeholder
     card ``<section>`` ids the HTMX polling targets.
  2. The ``/static`` mount serves both ``dashboard.css`` and
     ``htmx.min.js`` (asset bytes were copied verbatim from the reference
     project in C16).
  3. The dashboard footer carries the five compliance-report links the
     plan promised — those URLs already resolve as JSON in C14/C15; C18
     will optionally wrap them in HTML.

The fixture mirrors ``test_lifespan_wiring.py`` exactly: a per-test
SQLite + storage_root + long scheduler intervals + ``LifespanManager``
so the full app lifespan fires. We re-import ``src.main`` per test for
the same reason the lifespan tests do — settings get pinned at
app-construct time.
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

    Matches the pattern in ``test_lifespan_wiring.py`` so all integration
    tests bootstrap the app identically (long scheduler intervals so
    nothing fires during the test window).
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

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client


@pytest.mark.asyncio
async def test_dashboard_root_returns_html(app_under_test):
    """GET / returns 200 + text/html with the four card ``<section>`` ids."""
    _app, client = app_under_test
    r = await client.get("/")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    # Each HTMX card placeholder must be present so C17/C18's partials
    # have a swap target.
    for section_id in (
        'id="stats-card"',
        'id="tiers-card"',
        'id="policies-card"',
        'id="audit-card"',
    ):
        assert section_id in body, f"missing section id in dashboard: {section_id}"
    # HTMX wiring sanity — at least one hx-get and the refresh interval
    # placeholder must have resolved to a number.
    assert "hx-get=" in body
    assert "every " in body and "ms" in body


@pytest.mark.asyncio
async def test_static_css_served(app_under_test):
    """The /static mount delivers dashboard.css."""
    _app, client = app_under_test
    r = await client.get("/static/dashboard.css")
    assert r.status_code == 200
    # CSS body sanity — the reference stylesheet defines a ``.grid`` rule
    # and a ``--accent`` CSS custom property; both must survive the copy.
    assert ".grid" in r.text
    assert "--accent" in r.text


@pytest.mark.asyncio
async def test_static_htmx_served(app_under_test):
    """The /static mount delivers the htmx bundle."""
    _app, client = app_under_test
    r = await client.get("/static/htmx.min.js")
    assert r.status_code == 200
    # The shipped bundle always carries the ``htmx`` identifier somewhere
    # in its source — cheap sanity check that we didn't end up with an
    # empty or wrong file.
    assert "htmx" in r.text.lower()


@pytest.mark.asyncio
async def test_dashboard_has_compliance_footer_links(app_under_test):
    """Footer links every framework (gdpr/sox/hipaa/pci_dss/soc2)."""
    _app, client = app_under_test
    r = await client.get("/")
    assert r.status_code == 200
    body = r.text
    for fw in ("gdpr", "sox", "hipaa", "pci_dss", "soc2"):
        assert (
            f"/v1/reports/{fw}" in body
        ), f"missing compliance footer link for framework {fw}"
