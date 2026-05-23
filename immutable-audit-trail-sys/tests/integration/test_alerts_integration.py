"""Integration test: tampering the DB triggers an integrity-break alert
that shows up on /partials/alerts."""
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
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "alerts-integration-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    from src.stats.counters import reset_counters_for_tests
    from src.anomaly.alerts import reset_sink_for_tests
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()
    reset_sink_for_tests()
    yield tmp_path
    get_settings.cache_clear()
    clear_appender()
    reset_counters_for_tests()
    reset_sink_for_tests()


@pytest.fixture
async def app_and_client(env_setup):
    from src import main as main_module
    importlib.reload(main_module)
    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield main_module.app, client, env_setup


@pytest.mark.asyncio
async def test_tamper_fires_integrity_break_alert(app_and_client):
    _app, client, tmp_path = app_and_client
    # Seed records.
    for _ in range(3):
        await client.post("/v1/audit/append", json={
            "action": "read", "resource": "X", "success": True,
            "args_digest": "0"*64, "result_digest": "0"*64, "processing_ms": 1.0,
        })
    # Tamper seq=2.
    import sqlite3
    db_path = tmp_path / "audit.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TRIGGER IF EXISTS audit_records_no_update")
        conn.execute("UPDATE audit_records SET actor='evil' WHERE seq=2")
        conn.commit()
    finally:
        conn.close()
    # Run verify — this should emit an integrity_break alert.
    r = await client.get("/v1/verify")
    assert r.json()["ok"] is False

    # Alerts partial should now include the integrity_break alert.
    r = await client.get("/partials/alerts")
    body = r.text
    assert "integrity_break" in body or "INTEGRITY_BREAK" in body.upper()
    assert "seq=2" in body or "seq=2," in body or " 2 " in body
