"""Shared pytest fixtures for the real-time log indexing tests.

Fixtures here drive the full FastAPI lifespan (via
:func:`src.main.build_app` + ``app.router.lifespan_context``) against
a throwaway on-disk segment directory per test, so test runs never
step on each other's data and the app under test always sees fresh
components (index, consumer, Redis client).

Exposed fixtures
----------------

* ``tmp_segment_dir``   — a ``tmp_path/segments`` directory wired
                          into the settings singleton so non-HTTP
                          tests (segment, persistence, index) keep
                          working unchanged.
* ``app_instance``      — a freshly-built :class:`FastAPI` with its
                          lifespan entered; ``app.state.index`` /
                          ``app.state.consumer`` / ``app.state.redis_client``
                          are populated.
* ``async_client``      — an :mod:`httpx` ``AsyncClient`` bound to
                          ``app_instance`` via ``ASGITransport`` so
                          tests hit the real route handlers without a
                          real network socket.
"""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def tmp_segment_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at an isolated segment directory for one test.

    Mutates both the environment variable (so freshly-constructed
    ``Settings`` instances in subagents pick it up) and the cached
    ``settings`` singleton (so already-imported code sees the new
    path without a reimport).
    """
    seg_dir = tmp_path / "segments"
    seg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DISK_SEGMENT_DIR", str(seg_dir))

    from src import config

    monkeypatch.setattr(config.settings, "disk_segment_dir", str(seg_dir))
    return seg_dir


@pytest_asyncio.fixture
async def app_instance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[FastAPI]:
    """Yield a freshly built :class:`FastAPI` with its lifespan active.

    Each test gets its own segment directory, index, consumer, and
    Redis client. The lifespan runs for real — which means the
    consumer task starts and tries to connect to Redis. In the test
    profile Redis is up (the compose file makes it a dependency), so
    the consumer idles on ``XREADGROUP BLOCK`` until fixture teardown
    signals ``stop``. If Redis is down, the app still comes up in
    degraded mode — that's covered by ``test_health_fields_match_model``.
    """
    seg_dir = tmp_path / "segments"
    seg_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DISK_SEGMENT_DIR", str(seg_dir))

    # Build a Settings with the overridden directory baked in. Using a
    # fresh Settings() reads the patched env so we don't mutate the
    # global singleton.
    from src.config import Settings
    from src.main import build_app

    settings = Settings()
    # Belt-and-braces: force the disk dir onto the instance regardless
    # of env ordering issues with pydantic-settings caches.
    settings = settings.model_copy(update={"disk_segment_dir": str(seg_dir)})

    app = build_app(settings=settings)

    # ``lifespan_context`` returns an async context manager; entering
    # it runs the real startup path (index load, Redis connect, consumer
    # spawn) and exiting it runs the teardown (stop, flush, aclose).
    async with app.router.lifespan_context(app):
        yield app


@pytest_asyncio.fixture
async def async_client(app_instance: FastAPI) -> AsyncIterator[AsyncClient]:
    """Yield an httpx ``AsyncClient`` bound to the lifespan-active app.

    Uses ``ASGITransport`` so requests round-trip through the
    application stack without touching the network. The transport is
    closed automatically when the context exits.
    """
    transport = ASGITransport(app=app_instance)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
