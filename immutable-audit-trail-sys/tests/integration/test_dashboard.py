"""Integration tests for the dashboard + HTMX partials."""
import base64
import importlib
import os

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def env_setup(monkeypatch, tmp_path):
    monkeypatch.setenv("SIGNING_KEY_B64", base64.b64encode(os.urandom(32)).decode())
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'audit.db'}")
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "dashboard-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    from src.stats.counters import reset_counters_for_tests
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()
    yield
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()


@pytest.fixture
async def app_and_client(env_setup):
    from src import main as main_module
    importlib.reload(main_module)
    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield main_module.app, client


def _append_body(**overrides) -> dict:
    base = dict(
        action="read", resource="X", success=True,
        args_digest="0" * 64, result_digest="0" * 64, processing_ms=1.0,
    )
    base.update(overrides)
    return base


# --- Dashboard page -----------------------------------------------------

@pytest.mark.asyncio
async def test_dashboard_renders_200_html(app_and_client):
    _app, client = app_and_client
    r = await client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    body = r.text
    # Card containers and HTMX wiring are present.
    assert 'id="stats-card"' in body
    assert 'id="records-table"' in body
    assert 'id="integrity-card"' in body
    assert 'id="alerts-card"' in body
    assert "hx-get" in body  # HTMX attribute present
    assert "10000" in body  # default refresh_ms baked into template


# --- Static files mount ---------------------------------------------------

@pytest.mark.asyncio
async def test_static_css_served(app_and_client):
    _app, client = app_and_client
    r = await client.get("/static/dashboard.css")
    assert r.status_code == 200
    assert "text/css" in r.headers["content-type"]
    # Smell-test some content we know is in the CSS.
    assert "--bg" in r.text


@pytest.mark.asyncio
async def test_static_htmx_served(app_and_client):
    _app, client = app_and_client
    r = await client.get("/static/htmx.min.js")
    assert r.status_code == 200
    assert len(r.content) > 10_000  # HTMX min is ~47KB


# --- Partials ------------------------------------------------------------

@pytest.mark.asyncio
async def test_partial_stats_returns_html(app_and_client):
    _app, client = app_and_client
    await client.post("/v1/audit/append", json=_append_body())
    r = await client.get("/partials/stats")
    assert r.status_code == 200
    assert "<h2>Activity counters</h2>" in r.text
    assert "Records appended" in r.text
    # Should report at least 1 (the genesis is counted only at init; appends increment).
    # Just assert the value renders (digit somewhere).
    assert any(c.isdigit() for c in r.text)


@pytest.mark.asyncio
async def test_partial_records_includes_genesis(app_and_client):
    _app, client = app_and_client
    r = await client.get("/partials/records")
    assert r.status_code == 200
    body = r.text
    assert "<h2>Latest records (newest first)</h2>" in body
    assert "<table" in body
    # Genesis row contains "system" and "genesis".
    assert "system" in body
    assert "genesis" in body


@pytest.mark.asyncio
async def test_partial_integrity_returns_valid(app_and_client):
    _app, client = app_and_client
    r = await client.get("/partials/integrity")
    assert r.status_code == 200
    body = r.text
    assert "Chain integrity" in body
    assert "VALID" in body
    # Head seq 0 means just genesis.
    assert "0" in body


@pytest.mark.asyncio
async def test_partial_alerts_placeholder(app_and_client):
    _app, client = app_and_client
    r = await client.get("/partials/alerts")
    assert r.status_code == 200
    assert "Recent alerts" in r.text


# --- Integrity cache (5s TTL) -------------------------------------------

@pytest.mark.asyncio
async def test_integrity_cache_returns_consistent_within_ttl(app_and_client):
    """Two back-to-back hits should return identical bodies (cache).

    After a new append, the cache should NOT immediately reflect it —
    because verify is cached for 5s.
    """
    _app, client = app_and_client
    r1 = (await client.get("/partials/integrity")).text
    await client.post("/v1/audit/append", json=_append_body())
    r2 = (await client.get("/partials/integrity")).text
    # The records-table sees the new row immediately, but integrity stays cached.
    # We're testing that the cached value (head_seq=0 from genesis) was reused;
    # if the cache TTL were 0, r2 would show head_seq=1.
    assert "head_seq" in r1.lower() or "Head seq" in r1
    # Identical render confirms cache hit.
    assert r1 == r2


# --- Records table includes recent appends ------------------------------

@pytest.mark.asyncio
async def test_records_table_shows_new_appends(app_and_client):
    _app, client = app_and_client
    for i in range(3):
        await client.post("/v1/audit/append", json=_append_body(actor=f"user_{i}"))
    r = await client.get("/partials/records")
    body = r.text
    assert "user_0" in body
    assert "user_1" in body
    assert "user_2" in body
