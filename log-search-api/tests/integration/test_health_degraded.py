from __future__ import annotations

import asyncio
import os
import subprocess

import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.skipif(
    os.getenv("API_URL") is None or os.getenv("DEGRADED_TEST_ENABLED") is None,
    reason="set API_URL and DEGRADED_TEST_ENABLED env vars to enable degraded probe test",
)


async def test_live_health_detailed_reports_degraded_when_redis_down(live_url: str) -> None:
    project = os.getenv("COMPOSE_PROJECT_NAME", "log-search-api")
    try:
        subprocess.run(
            ["docker", "compose", "-p", project, "stop", "redis"],
            check=True,
            capture_output=True,
        )

        last_body: dict | None = None
        last_status: int = 0
        async with AsyncClient(base_url=live_url, timeout=10.0) as client:
            for attempt in range(10):
                await asyncio.sleep(0.5)
                response = await client.get("/api/v1/health/detailed")
                last_status = response.status_code
                last_body = response.json()
                if (
                    last_status == 200
                    and last_body.get("status") == "degraded"
                    and last_body.get("dependencies", {}).get("redis") == "down"
                ):
                    break

        assert last_status == 200, f"unexpected status {last_status}: {last_body}"
        assert last_body is not None
        assert last_body["status"] == "degraded", f"expected degraded, got {last_body}"
        assert last_body["dependencies"]["redis"] == "down", last_body
        assert last_body["dependencies"]["elasticsearch"] == "ok", last_body
    finally:
        subprocess.run(
            ["docker", "compose", "-p", project, "start", "redis"],
            check=True,
            capture_output=True,
        )

        async with AsyncClient(base_url=live_url, timeout=10.0) as client:
            for attempt in range(20):
                await asyncio.sleep(0.5)
                response = await client.get("/api/v1/health/detailed")
                if response.status_code == 200:
                    body = response.json()
                    if body.get("dependencies", {}).get("redis") == "ok":
                        break
