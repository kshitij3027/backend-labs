import asyncio
from datetime import datetime, timezone, timedelta

import pytest

from src.db import get_workers, mark_worker_dead, register_worker, update_heartbeat
from src.coordinator.heartbeat import heartbeat_checker


@pytest.mark.asyncio(loop_scope="session")
class TestWorkerRegistration:
    async def test_register_worker(self, test_client):
        resp = await test_client.post(
            "/workers/register",
            json={"worker_id": "test-worker-001"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["worker_id"] == "test-worker-001"
        assert data["status"] == "ALIVE"

    async def test_register_worker_is_idempotent(self, test_client):
        resp1 = await test_client.post(
            "/workers/register",
            json={"worker_id": "test-worker-idem"},
        )
        assert resp1.status_code == 200

        resp2 = await test_client.post(
            "/workers/register",
            json={"worker_id": "test-worker-idem"},
        )
        assert resp2.status_code == 200
        assert resp2.json()["status"] == "ALIVE"


@pytest.mark.asyncio(loop_scope="session")
class TestHeartbeatEndpoint:
    async def test_heartbeat_updates(self, test_client):
        # Register first
        await test_client.post(
            "/workers/register",
            json={"worker_id": "test-worker-hb"},
        )

        resp = await test_client.post("/workers/test-worker-hb/heartbeat")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    async def test_heartbeat_nonexistent_worker(self, test_client):
        resp = await test_client.post("/workers/nonexistent-worker-xyz/heartbeat")
        assert resp.status_code == 404


@pytest.mark.asyncio(loop_scope="session")
class TestListWorkers:
    async def test_list_workers_returns_registered(self, test_client):
        await test_client.post(
            "/workers/register",
            json={"worker_id": "test-worker-list"},
        )
        resp = await test_client.get("/workers")
        assert resp.status_code == 200
        data = resp.json()
        ids = [w["id"] for w in data]
        assert "test-worker-list" in ids


@pytest.mark.asyncio(loop_scope="session")
class TestHeartbeatChecker:
    async def test_worker_marked_dead_after_timeout(self):
        """Register a worker, set its heartbeat far in the past, run checker."""
        worker_id = "test-worker-timeout"
        await register_worker(worker_id)

        # Manually set heartbeat to the past using db function
        from src.db import pool

        past = datetime.now(timezone.utc) - timedelta(seconds=60)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE workers SET last_heartbeat = $1 WHERE id = $2",
                past,
                worker_id,
            )

        # Run heartbeat checker with a very short timeout
        checker_task = asyncio.create_task(heartbeat_checker(interval=0, timeout=5))
        # Let it run one iteration
        await asyncio.sleep(0.5)
        checker_task.cancel()
        try:
            await checker_task
        except asyncio.CancelledError:
            pass

        # Verify the worker is now DEAD
        workers = await get_workers()
        worker = next(w for w in workers if w["id"] == worker_id)
        assert worker["status"] == "DEAD"
