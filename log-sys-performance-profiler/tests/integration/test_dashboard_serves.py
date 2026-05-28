from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_get_root_returns_html() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/")
            assert r.status_code == 200
            assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_get_compare_view_returns_html() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/compare")
            assert r.status_code == 200
            assert "text/html" in r.headers["content-type"]
