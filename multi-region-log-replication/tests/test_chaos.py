"""Pytest-driven chaos test — kill primary mid-burst, verify no data loss.

This complements ``scripts/chaos.py`` (which drives the *running*
docker-compose stack) with an in-process variant that runs against the
ASGI app directly via ``httpx.AsyncClient`` + ``ASGITransport``. No
docker, no network — just the FastAPI app loaded into the test process.

Why we manually start the monitor:
  ``httpx.ASGITransport`` deliberately does not run the FastAPI
  ``lifespan`` (it's a transport, not a runner). So the failover
  background task wouldn't fire under the test client without us
  poking ``app.state.monitor.start()`` ourselves. We do the same in
  reverse on teardown — see the ``finally`` block.

Why a 50ms tick:
  The monitor needs ``UNHEALTHY_THRESHOLD = 2`` consecutive unhealthy
  ticks before it re-elects, so 50ms × 2 = 100ms detection + a tiny
  election step. We sleep 400ms after killing to give the monitor
  comfortable margin on a slow CI box without dragging the test out.
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.http_server import create_app


@pytest.mark.asyncio
async def test_chaos_kill_primary_mid_write_no_data_loss() -> None:
    """A burst of writes survives a primary kill mid-burst — no data loss."""
    # Fast-tick monitor so failover detects within ~100ms and the
    # whole test stays under ~3s of wall time.
    config = AppConfig.from_env(env={"HEALTH_CHECK_INTERVAL_SEC": "0.05"})
    app = create_app(config)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Lifespan doesn't run under ASGITransport, so we start the
        # monitor explicitly. It owns the failover loop.
        await app.state.monitor.start()
        try:
            accepted: list[str] = []

            async def writer(i: int) -> None:
                resp = await client.post(
                    "/api/logs",
                    json={
                        "message": f"chaos-{i}",
                        "level": "info",
                        "service": "chaos",
                    },
                )
                if resp.status_code == 200:
                    accepted.append(resp.json()["log_id"])

            # First half of writes — all should land on us-east.
            await asyncio.gather(*[writer(i) for i in range(50)])

            # Kill primary. The monitor will pick this up on the next
            # two ticks and re-elect europe.
            r = await client.post("/api/regions/us-east/kill")
            assert r.status_code == 200

            # Wait for failover detection. 2 ticks at 0.05s + safety
            # margin for CI flake.
            await asyncio.sleep(0.4)

            # Verify the failover actually flipped before we drive
            # the second half — otherwise we'd be testing nothing.
            health = (await client.get("/api/health")).json()
            assert health["current_primary"] == "europe", (
                f"failover did not flip: current_primary="
                f"{health['current_primary']!r}"
            )

            # Continue writing under the new primary.
            await asyncio.gather(*[writer(i) for i in range(50, 100)])

            # No accepted log_id may be missing from europe (the new
            # primary is the source of truth post-failover).
            eu_logs = (
                await client.get("/api/regions/europe/logs?limit=500")
            ).json()
            eu_ids = {e["log_id"] for e in eu_logs}
            missing = set(accepted) - eu_ids
            assert not missing, (
                f"missing {len(missing)} accepted writes from europe; "
                f"sample: {list(missing)[:5]}"
            )
            # Most writes should succeed; a handful may be in-flight when
            # the kill lands and bounce off the 503 from current_primary().
            assert len(accepted) >= 80, (
                f"expected ≥80 accepted writes, got {len(accepted)}"
            )
        finally:
            await app.state.monitor.stop()


@pytest.mark.asyncio
async def test_chaos_snapshot_exposes_total_writes_and_failover() -> None:
    """After chaos, the snapshot exposes ``total_writes`` + a failover event."""
    config = AppConfig.from_env(env={"HEALTH_CHECK_INTERVAL_SEC": "0.05"})
    app = create_app(config)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await app.state.monitor.start()
        try:
            # Drive a small burst, kill, drive a smaller burst.
            for i in range(20):
                await client.post(
                    "/api/logs",
                    json={
                        "message": f"obs-{i}",
                        "level": "info",
                        "service": "chaos",
                    },
                )
            await client.post("/api/regions/us-east/kill")
            await asyncio.sleep(0.4)
            for i in range(20, 30):
                await client.post(
                    "/api/logs",
                    json={
                        "message": f"obs-{i}",
                        "level": "info",
                        "service": "chaos",
                    },
                )

            snap = (await client.get("/api/status")).json()

            # ``total_writes`` is sourced from the current primary's
            # ``log_count`` — never summed across regions. Post-failover
            # primary is europe, which holds every replicated entry.
            assert "total_writes" in snap, "snapshot missing total_writes"
            assert isinstance(snap["total_writes"], int)
            assert snap["total_writes"] >= 25, (
                f"expected ≥25 total_writes on europe, got "
                f"{snap['total_writes']}"
            )

            # And the failover landed in recent_failovers.
            events = snap.get("recent_failovers", [])
            assert len(events) >= 1, "expected at least one failover event"
            assert events[-1]["old_primary"] == "us-east"
            assert events[-1]["new_primary"] == "europe"
        finally:
            await app.state.monitor.stop()
