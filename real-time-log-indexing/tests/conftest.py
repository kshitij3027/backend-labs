"""Shared pytest fixtures for the real-time log indexing tests.

The ``async_client`` fixture drives the FastAPI lifespan properly via
``app.router.lifespan_context(app)`` so any lifespan-managed state is
wired up before a request is issued and torn down cleanly
afterwards. The ``tmp_segment_dir`` fixture isolates on-disk segments
per test so parallel runs never step on each other's data.
"""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def tmp_segment_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at an isolated segment directory for one test.

    Mutates both the environment variable and the already-loaded
    ``settings`` singleton — the singleton is read directly inside
    the app, so patching the env alone is not enough; we also reach
    in and rewrite the attribute.
    """
    seg_dir = tmp_path / "segments"
    seg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DISK_SEGMENT_DIR", str(seg_dir))

    # The settings object is cached at module import time; patch the
    # live instance so already-imported code sees the new path.
    from src import config

    monkeypatch.setattr(config.settings, "disk_segment_dir", str(seg_dir))
    return seg_dir


@pytest_asyncio.fixture
async def async_client(tmp_segment_dir: Path) -> AsyncIterator[AsyncClient]:
    """Yield an httpx ``AsyncClient`` bound to the FastAPI app.

    Uses ``ASGITransport`` (no network) so tests are fast, and enters
    ``app.router.lifespan_context`` so startup + shutdown run exactly
    once around the test body.
    """
    from src.main import app

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
