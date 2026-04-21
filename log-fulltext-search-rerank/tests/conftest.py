"""Shared pytest fixtures for the log-fulltext-search-rerank suite.

Every test gets a fresh view of the settings cache (so env-var
overrides in one test can't bleed into another) plus an async HTTP
client that drives the ASGI app in-process — no real port binding.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.config import Settings, get_settings, reset_settings_cache
from src.main import app


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    """Drop the cached :class:`Settings` before and after each test.

    Pre-clear makes each test see whatever env state the test's own
    ``monkeypatch`` sets up; post-clear prevents a leaked cached
    instance from poisoning the next test.
    """
    reset_settings_cache()
    yield
    reset_settings_cache()


@pytest.fixture
def settings() -> Settings:
    """Return the current :class:`Settings` singleton."""
    return get_settings()


@pytest_asyncio.fixture
async def async_client():
    """Async HTTP client bound to the ASGI app via :class:`ASGITransport`.

    Using the ASGI transport means tests do not need a running uvicorn
    — the client calls into the app object directly so failures point
    at the handler, not at network plumbing.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
