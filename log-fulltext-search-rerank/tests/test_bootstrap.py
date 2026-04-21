"""Smoke test for the FastAPI scaffold introduced in commit 01.

Exercises the ASGI app directly via ``httpx.ASGITransport`` so the
test does not depend on a running container — it's a pure in-process
check that the app object is importable and ``/health`` responds as
expected.
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_health_ok() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
