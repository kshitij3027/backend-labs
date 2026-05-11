import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from src.api.app import create_app


@pytest.mark.asyncio
async def test_admin_config_updates_ewma_alpha_and_fuser_picks_it_up():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/v1/admin/config", json={"ewma_alpha": 0.7})
            assert r.status_code == 200
            body = r.json()
            assert "ewma_alpha" in body["updated_fields"]
            assert body["current"]["ewma_alpha"] == 0.7
            c = app.state.components
            assert c.fuser.alpha == 0.7


@pytest.mark.asyncio
async def test_admin_config_updates_thresholds_and_state_machine_reads_live():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post(
                "/api/v1/admin/config",
                json={"up_normal_to_pressure": 0.5, "down_pressure_to_normal": 0.3},
            )
            assert r.status_code == 200
            c = app.state.components
            assert c.settings.up_normal_to_pressure == 0.5
            assert c.settings.down_pressure_to_normal == 0.3
            import time
            c.manager._entered_at = time.monotonic() - c.settings.min_dwell_seconds - 1.0
            level = c.manager.tick(0.55)
            from src.state import PressureLevel
            assert level == PressureLevel.PRESSURE


@pytest.mark.asyncio
async def test_admin_config_no_fields_supplied_returns_empty_updated_list():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/v1/admin/config", json={})
            assert r.status_code == 200
            assert r.json()["updated_fields"] == []


@pytest.mark.asyncio
async def test_admin_config_rollback_restores_prev_value():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/v1/admin/config", json={"ewma_alpha": 0.9})
            assert r.json()["current"]["ewma_alpha"] == 0.9
            r2 = await client.post("/api/v1/admin/config", json={"ewma_alpha": 0.3})
            assert r2.json()["current"]["ewma_alpha"] == 0.3
            c = app.state.components
            assert c.fuser.alpha == 0.3
