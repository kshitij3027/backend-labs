"""Integration test for /api/stats + /metrics."""
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
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "metrics-test")
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


@pytest.mark.asyncio
async def test_stats_endpoint_reflects_activity(app_and_client):
    _app, client = app_and_client
    # Baseline.
    snap = (await client.get("/api/stats")).json()
    base_appended = snap["records_appended"]
    base_verifs = snap["verifications_run"]

    # 3 appends + 1 verify.
    for _ in range(3):
        r = await client.post("/v1/audit/append", json=_append_body())
        assert r.status_code == 201
    await client.get("/v1/verify")

    snap = (await client.get("/api/stats")).json()
    assert snap["records_appended"] == base_appended + 3
    assert snap["verifications_run"] == base_verifs + 1
    assert snap["integrity_breaks_detected"] == 0  # clean chain


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_custom_metrics(app_and_client):
    _app, client = app_and_client
    # Trigger one of each so the metric appears with a value > 0.
    await client.post("/v1/audit/append", json=_append_body())
    await client.get("/v1/verify")

    r = await client.get("/metrics")
    assert r.status_code == 200
    text = r.text
    # Our five custom counter + one histogram should appear.
    for metric_name in [
        "audit_records_appended_total",
        "audit_verifications_total",
        "audit_verify_breaks_total",
        "audit_decorator_invocations_total",
        "audit_decorator_failures_total",
        "audit_decorator_overhead_ms",
    ]:
        assert metric_name in text, f"missing metric: {metric_name}"


@pytest.mark.asyncio
async def test_integrity_break_increments_break_counter(app_and_client, tmp_path):
    """Tamper a row, run verify, confirm integrity_breaks_detected increases."""
    _app, client = app_and_client
    for _ in range(3):
        await client.post("/v1/audit/append", json=_append_body())

    # Direct tamper via sqlite3.
    import sqlite3
    # Find the db path from settings.
    from src.settings import get_settings
    db_path = get_settings().database_url.replace("sqlite+aiosqlite:///", "")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DROP TRIGGER IF EXISTS audit_records_no_update")
        conn.execute("UPDATE audit_records SET actor='evil' WHERE seq=2")
        conn.commit()
    finally:
        conn.close()

    snap_before = (await client.get("/api/stats")).json()
    base_breaks = snap_before["integrity_breaks_detected"]

    r = await client.get("/v1/verify")
    body = r.json()
    assert body["ok"] is False

    snap_after = (await client.get("/api/stats")).json()
    assert snap_after["integrity_breaks_detected"] == base_breaks + 1
