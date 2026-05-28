from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_metrics_snapshot_returns_structure() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/metrics/snapshot?window_sec=10")
            assert r.status_code == 200
            data = r.json()
            assert "samples" in data
            assert "dropped" in data
            assert "window_sec" in data


@pytest.mark.asyncio
async def test_bottlenecks_endpoint_404_on_missing() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/runs/missing/bottlenecks")
            assert r.status_code == 404


@pytest.mark.asyncio
async def test_recommendations_endpoint_404_on_missing() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/runs/missing/recommendations")
            assert r.status_code == 404


@pytest.mark.asyncio
async def test_bottlenecks_and_recs_lists_after_run() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            post = await ac.post("/api/runs", json={"log_count": 30, "concurrency": 1, "seed": 1})
            run_id = post.json()["run_id"]
            for _ in range(40):
                await asyncio.sleep(0.1)
                r = await ac.get(f"/api/runs/{run_id}")
                if r.status_code == 200:
                    break
            else:
                pytest.fail("run did not complete")
            b = await ac.get(f"/api/runs/{run_id}/bottlenecks")
            assert b.status_code == 200
            assert isinstance(b.json(), list)
            rec = await ac.get(f"/api/runs/{run_id}/recommendations")
            assert rec.status_code == 200
            assert isinstance(rec.json(), list)
