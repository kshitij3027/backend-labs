"""Integration smoke: spin up the FastAPI app via lifespan, hit /api/health,
verify the SQLite file gets created with the genesis row."""
import base64
import os
from pathlib import Path

import pytest
import sqlalchemy as sa
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture
def env_setup(monkeypatch, tmp_path):
    """Point the app at a tmp SQLite file and a throwaway signing key."""
    db_path = tmp_path / "audit.db"
    monkeypatch.setenv("SIGNING_KEY_B64", base64.b64encode(os.urandom(32)).decode())
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "integration-test")
    # Clear settings cache so the override env vars are picked up.
    from src.settings import get_settings
    get_settings.cache_clear()
    yield db_path
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_app_boots_and_inserts_genesis(env_setup):
    db_path = env_setup
    # Force re-import to get a fresh app with the new env-bound lifespan.
    import importlib
    from src import main as main_module
    importlib.reload(main_module)

    async with LifespanManager(main_module.app):
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "healthy"
        assert isinstance(body["timestamp"], int)

    # After lifespan exits, the SQLite file should exist with the genesis row.
    assert db_path.exists(), "lifespan should have created the DB file"

    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql(
                "SELECT seq, actor, action, resource FROM audit_records ORDER BY seq"
            )
            rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0] == (0, "system", "genesis", "integration-test")
    finally:
        await engine.dispose()
