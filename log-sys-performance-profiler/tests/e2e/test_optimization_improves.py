"""§5 success criterion: at least one optimization demonstrates a meaningful
improvement on the synthetic workload — defined here as verdict=='improved'
(throughput >=+10% or p95 <=-10% with no regression metric >+10%).

Tries each registered optimization in order until one passes; fails if none do.
"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


CANDIDATES = ["batch_writer", "object_pool", "precompiled_validator", "fsm_parser"]


@pytest.mark.asyncio
async def test_at_least_one_optimization_improves() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            successes = []
            failures = []
            for opt in CANDIDATES:
                # Trigger a compare run
                r = await ac.post(
                    "/api/runs",
                    json={"log_count": 800, "concurrency": 4, "seed": 42, "optimization_name": opt},
                )
                assert r.status_code == 202

                # Wait for the harness to land both baseline + optimized in the store
                base_id = None
                opt_id = None
                for _ in range(60):
                    await asyncio.sleep(0.2)
                    runs = (await ac.get("/api/runs?limit=20")).json()
                    base = [r for r in runs if r["baseline_or_optimized"] == "baseline" and r["optimization_name"] is None]
                    optimized = [r for r in runs if r["baseline_or_optimized"] == "optimized" and r["optimization_name"] == opt]
                    if base and optimized:
                        base_id = base[-1]["run_id"]
                        opt_id = optimized[-1]["run_id"]
                        break
                if not base_id or not opt_id:
                    failures.append((opt, "timeout waiting for runs"))
                    continue

                diff = await ac.get(f"/api/compare?a={base_id}&b={opt_id}")
                assert diff.status_code == 200
                data = diff.json()["diff"]
                if data["verdict"] == "improved":
                    successes.append((opt, data))
                    break
                failures.append((opt, data))
            assert successes, f"no optimization improved on synthetic workload; tried: {failures}"
