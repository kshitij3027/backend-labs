"""Shared pytest fixtures for C2+ tests.

The async_client fixture drives the FastAPI lifespan properly via
``app.router.lifespan_context(app)`` so ``app.state.db`` is wired up
before any request is issued and torn down after.

The ``tmp_db_path`` fixture monkeypatches ``DB_PATH`` (env) and the
in-memory ``settings`` + the imported module-level reference in
``src.main`` so the app writes to an isolated tmp path for the test.
This lets tests run in parallel without stepping on each other's DB.
"""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def tmp_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at an isolated SQLite file for the duration of a test.

    Mutates both the environment variable and the already-loaded
    ``settings`` singleton — the singleton is read directly by
    ``src.main`` inside the lifespan hook so we have to patch both.
    """
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))

    # The settings object is imported & cached at module load, so
    # patching the env var alone isn't enough; reach into the live
    # instance and rewrite the attribute.
    from src import config, main

    monkeypatch.setattr(config.settings, "db_path", str(db_file))
    monkeypatch.setattr(main.settings, "db_path", str(db_file))
    return db_file


@pytest_asyncio.fixture
async def async_client(tmp_db_path: Path) -> AsyncIterator[AsyncClient]:
    """Yield an httpx AsyncClient bound to the FastAPI app.

    Runs the lifespan manually so ``app.state.db`` is created and
    migrations run before the test makes a request, then torn down
    cleanly. Uses ASGITransport (no network) so tests are fast.
    """
    from src.main import app

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest_asyncio.fixture
async def seeded_db(async_client: AsyncClient) -> AsyncClient:
    """Async client over a DB pre-seeded with 200 generated logs.

    Uses a fixed seed so tests that care about determinism can rely
    on a stable dataset shape across runs.
    """
    resp = await async_client.post("/api/logs/generate?count=200&seed=42")
    assert resp.status_code == 201, resp.text
    return async_client
