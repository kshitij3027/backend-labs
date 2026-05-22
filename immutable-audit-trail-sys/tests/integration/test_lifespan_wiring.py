"""Confirms FastAPI lifespan wires every component into app.state and
populates the decorator registry.

This is the gatekeeper for C10+ — every endpoint added later relies on
something being on app.state. If lifespan wiring breaks, this test will
break first, with a clear failure message."""
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
    monkeypatch.setenv("CHAIN_GENESIS_NOTE", "lifespan-wiring-test")
    from src.settings import get_settings
    from src.interceptor.decorator import clear_appender
    get_settings.cache_clear()
    clear_appender()
    yield
    get_settings.cache_clear()
    clear_appender()


@pytest.mark.asyncio
async def test_lifespan_populates_app_state_and_registry(env_setup):
    """All chain components on app.state, decorator registry has appender."""
    from src import main as main_module
    importlib.reload(main_module)
    from src.interceptor.decorator import get_appender

    async with LifespanManager(main_module.app):
        # During startup, all state attrs are populated.
        assert main_module.app.state.settings is not None
        assert main_module.app.state.signer is not None
        assert main_module.app.state.engine is not None
        assert main_module.app.state.session_factory is not None
        assert main_module.app.state.appender is not None
        assert main_module.app.state.chain_verifier is not None

        # The decorator's process-wide registry should be populated.
        assert get_appender() is main_module.app.state.appender

        # Health endpoint still 200 through the new router.
        transport = ASGITransport(app=main_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/api/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_lifespan_chain_verifier_works_against_genesis(env_setup):
    """verify_full() on the freshly-init'd chain returns VALID."""
    from src import main as main_module
    importlib.reload(main_module)

    async with LifespanManager(main_module.app):
        result = await main_module.app.state.chain_verifier.verify_full()
        assert result.ok is True
        assert result.total_records == 1  # just genesis
        assert result.head_seq == 0
