from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_post_run_returns_202_with_run_id() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post("/api/runs", json={"log_count": 50, "concurrency": 2, "seed": 1})
            assert r.status_code == 202
            data = r.json()
            assert "run_id" in data
            assert data["mode"] == "baseline"


@pytest.mark.asyncio
async def test_list_runs_initially_empty() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/runs")
            assert r.status_code == 200
            assert isinstance(r.json(), list)


@pytest.mark.asyncio
async def test_get_run_missing_returns_404() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/runs/nonexistent")
            assert r.status_code == 404


@pytest.mark.asyncio
async def test_post_then_get_eventually_returns_summary() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            post = await ac.post("/api/runs", json={"log_count": 30, "concurrency": 1, "seed": 1})
            assert post.status_code == 202
            run_id = post.json()["run_id"]
            # Wait for the background task to complete
            for _ in range(40):
                await asyncio.sleep(0.1)
                r = await ac.get(f"/api/runs/{run_id}")
                if r.status_code == 200:
                    data = r.json()
                    assert data["log_count"] == 30
                    return
            pytest.fail(f"Run {run_id} did not complete within 4s")


@pytest.mark.asyncio
async def test_post_compare_returns_compare_mode() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.post(
                "/api/runs",
                json={
                    "log_count": 30, "concurrency": 1, "seed": 1,
                    "optimization_name": "batch_writer",
                },
            )
            assert r.status_code == 202
            assert r.json()["mode"] == "compare"
