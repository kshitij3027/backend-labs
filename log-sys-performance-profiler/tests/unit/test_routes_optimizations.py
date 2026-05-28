from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_list_optimizations_returns_six_or_more() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/api/optimizations")
            assert r.status_code == 200
            items = r.json()
            names = {it["name"] for it in items}
            assert {
                "batch_writer", "object_pool", "fsm_parser",
                "precompiled_validator", "async_io_variant", "mmap_reader",
            } <= names
