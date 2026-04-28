from __future__ import annotations

import asyncio
import os

import pytest
from httpx import AsyncClient


pytestmark = pytest.mark.skipif(
    os.getenv("API_URL") is None,
    reason="set API_URL env var",
)


async def test_live_health_detailed_all_dependencies_ok(live_url: str) -> None:
    last_body: dict | None = None
    last_status: int = 0
    async with AsyncClient(base_url=live_url, timeout=10.0) as client:
        for attempt in range(5):
            response = await client.get("/api/v1/health/detailed")
            last_status = response.status_code
            last_body = response.json()
            if (
                last_status == 200
                and last_body.get("status") == "ok"
                and last_body.get("dependencies", {}).get("elasticsearch") == "ok"
                and last_body.get("dependencies", {}).get("redis") == "ok"
            ):
                break
            await asyncio.sleep(1.0)

    assert last_status == 200, f"unexpected status {last_status}: {last_body}"
    assert last_body is not None
    assert last_body["status"] == "ok", f"expected status ok, got body {last_body}"
    assert last_body["dependencies"]["elasticsearch"] == "ok", last_body
    assert last_body["dependencies"]["redis"] == "ok", last_body
