import asyncio

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from src.api.app import create_app


async def _client():
    app = create_app()
    return app


@pytest.mark.asyncio
async def test_health_returns_ok():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/system/health")
            assert r.status_code == 200
            assert r.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_system_status_shape_matches_spec():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/api/v1/system/status")
            assert r.status_code == 200
            body = r.json()
            assert "backpressure" in body and "processor" in body and "circuit_breaker" in body
            bp = body["backpressure"]
            assert {"pressure_level", "throttle_rate", "queue_size", "pressure_score"} <= set(bp.keys())
            pr = body["processor"]
            assert {"processed_count", "dropped_count", "error_count"} <= set(pr.keys())
            cb = body["circuit_breaker"]
            assert {"state", "failure_count"} <= set(cb.keys())


@pytest.mark.asyncio
async def test_ingest_returns_202_in_normal_state():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/v1/ingest", json={"message": "hello", "priority": "normal"})
            assert r.status_code == 202
            assert r.json()["accepted"] is True


@pytest.mark.asyncio
async def test_prometheus_metrics_endpoint():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/metrics")
            assert r.status_code == 200
            body = r.text
            assert "abpm_pressure_score" in body
            assert "abpm_queue_size" in body


@pytest.mark.asyncio
async def test_metrics_json_endpoint_includes_priorities():
    app = create_app()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get("/api/v1/metrics/json")
            assert r.status_code == 200
            body = r.json()
            assert "queue_sizes" in body
            assert set(body["queue_sizes"].keys()) == {"critical", "high", "normal", "low"}
