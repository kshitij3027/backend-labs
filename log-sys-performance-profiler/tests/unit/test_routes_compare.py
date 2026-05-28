from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_compare_404_on_missing() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/compare?a=missing&b=missing2")
            assert r.status_code == 404


@pytest.mark.asyncio
async def test_compare_returns_diff_after_runs() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            p1 = await ac.post("/api/runs", json={"log_count": 30, "concurrency": 1, "seed": 1})
            rid_a = p1.json()["run_id"]
            for _ in range(40):
                await asyncio.sleep(0.1)
                if (await ac.get(f"/api/runs/{rid_a}")).status_code == 200:
                    break
            p2 = await ac.post("/api/runs", json={"log_count": 30, "concurrency": 1, "seed": 2})
            rid_b = p2.json()["run_id"]
            for _ in range(40):
                await asyncio.sleep(0.1)
                if (await ac.get(f"/api/runs/{rid_b}")).status_code == 200:
                    break
            r = await ac.get(f"/api/compare?a={rid_a}&b={rid_b}")
            assert r.status_code == 200
            data = r.json()
            assert "baseline" in data and "optimized" in data and "diff" in data
            assert data["diff"]["verdict"] in {"improved", "regressed", "neutral"}
