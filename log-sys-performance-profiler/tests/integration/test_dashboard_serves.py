from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app


@pytest.mark.asyncio
async def test_get_root_returns_html_with_chart_js() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/")
            assert r.status_code == 200
            assert "text/html" in r.headers["content-type"]
            body = r.text
            assert "chart.min.js" in body
            assert "dashboard.js" in body
            assert "Log Pipeline Performance Profiler" in body


@pytest.mark.asyncio
async def test_get_compare_view_returns_html() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/compare")
            assert r.status_code == 200
            assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_static_chart_js_served() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/static/chart.min.js")
            assert r.status_code == 200
            # Should be the Chart.js UMD bundle
            assert len(r.content) > 50_000


@pytest.mark.asyncio
async def test_static_dashboard_js_served() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/static/dashboard.js")
            assert r.status_code == 200


@pytest.mark.asyncio
async def test_static_dashboard_css_served() -> None:
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as ac:
            r = await ac.get("/static/dashboard.css")
            assert r.status_code == 200
