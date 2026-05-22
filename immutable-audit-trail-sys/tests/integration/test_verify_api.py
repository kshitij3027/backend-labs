"""Integration tests for /v1/verify (full + range)."""
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
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "verify-api-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    get_settings.cache_clear()
    clear_appender()
    yield tmp_path
    get_settings.cache_clear()
    clear_appender()


@pytest.fixture
async def app_and_client(env_setup):
    from src import main as main_module
    importlib.reload(main_module)
    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield main_module.app, client, env_setup  # tmp_path passed through


def _append_body(**overrides) -> dict:
    base = dict(
        action="read",
        resource="X",
        success=True,
        args_digest="0" * 64,
        result_digest="0" * 64,
        processing_ms=1.0,
    )
    base.update(overrides)
    return base


# --- Clean chain ----------------------------------------------------------

@pytest.mark.asyncio
async def test_verify_full_clean_chain(app_and_client):
    _app, client, _tmp = app_and_client
    for _ in range(10):
        await client.post("/v1/audit/append", json=_append_body())
    r = await client.get("/v1/verify")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["integrity_status"] == "VALID"
    assert body["total_records"] == 11  # genesis + 10
    assert body["head_seq"] == 10
    assert body["first_break_seq"] is None
    assert body["signature_failures"] == []
    assert body["seq_gaps"] == []


@pytest.mark.asyncio
async def test_verify_just_genesis(app_and_client):
    _app, client, _tmp = app_and_client
    r = await client.get("/v1/verify")
    body = r.json()
    assert body["ok"] is True
    assert body["total_records"] == 1
    assert body["head_seq"] == 0


# --- Range mode -----------------------------------------------------------

@pytest.mark.asyncio
async def test_verify_range_scoped(app_and_client):
    _app, client, _tmp = app_and_client
    for _ in range(10):
        await client.post("/v1/audit/append", json=_append_body())
    r = await client.get("/v1/verify?from=3&to=7")
    body = r.json()
    assert r.status_code == 200
    assert body["ok"] is True
    assert body["total_records"] == 5  # 3,4,5,6,7 inclusive
    # head_seq still reports the chain head, not the range end
    assert body["head_seq"] == 10


@pytest.mark.asyncio
async def test_verify_422_when_only_from_given(app_and_client):
    _app, client, _tmp = app_and_client
    r = await client.get("/v1/verify?from=3")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_verify_422_when_only_to_given(app_and_client):
    _app, client, _tmp = app_and_client
    r = await client.get("/v1/verify?to=5")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_verify_422_when_to_less_than_from(app_and_client):
    _app, client, _tmp = app_and_client
    r = await client.get("/v1/verify?from=5&to=3")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_verify_422_negative_from(app_and_client):
    _app, client, _tmp = app_and_client
    # FastAPI Query(ge=0) returns 422 for negative.
    r = await client.get("/v1/verify?from=-1&to=5")
    assert r.status_code == 422


# --- Tampered chain ------------------------------------------------------

@pytest.mark.asyncio
async def test_verify_detects_tamper(app_and_client, tmp_path):
    """Direct SQL bypass (dropping triggers) lets us simulate tampering."""
    _app, client, tp = app_and_client
    for _ in range(5):
        await client.post("/v1/audit/append", json=_append_body())
    # Tamper seq=3 via raw sqlite.
    db_path = tp / "audit.db"
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("DROP TRIGGER IF EXISTS audit_records_no_update")
        conn.execute("DROP TRIGGER IF EXISTS audit_records_no_delete")
        conn.execute("UPDATE audit_records SET actor='evil' WHERE seq=3")
        conn.commit()
    finally:
        conn.close()
    r = await client.get("/v1/verify")
    body = r.json()
    assert body["ok"] is False
    assert body["integrity_status"] == "BROKEN"
    assert body["first_break_seq"] == 3
