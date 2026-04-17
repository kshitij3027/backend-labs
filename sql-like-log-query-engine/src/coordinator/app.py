"""FastAPI application factory for the coordinator service."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.shared.config import CoordinatorSettings

from .executor import QueryExecutor
from .progress import ProgressRegistry
from .registry import PartitionRegistry
from .routes import router


# ``src/`` — this module lives at ``src/coordinator/app.py`` so its parent's
# parent is the ``src/`` root where ``templates/`` and ``static/`` live.
_SRC_ROOT = Path(__file__).resolve().parent.parent
_TEMPLATES_DIR = _SRC_ROOT / "templates"
_STATIC_DIR = _SRC_ROOT / "static"


def create_coordinator_app(settings: CoordinatorSettings) -> FastAPI:
    """Build and configure the coordinator FastAPI app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(timeout=settings.request_timeout)
        registry = PartitionRegistry(settings.partition_urls_dict())
        executor = QueryExecutor(
            client=client, request_timeout=settings.request_timeout
        )

        app.state.settings = settings
        app.state.client = client
        app.state.registry = registry
        app.state.executor = executor
        app.state.progress = ProgressRegistry()

        # Prime the registry with a single synchronous refresh so that the
        # very first /api/query after startup sees an accurate partition
        # list. Any per-partition exceptions are already swallowed by
        # ``refresh``.
        try:
            await registry.refresh(client)
        except Exception:
            pass

        poll_task = asyncio.create_task(registry.poll_forever(client))
        app.state.poll_task = poll_task

        try:
            yield
        finally:
            poll_task.cancel()
            try:
                await poll_task
            except (asyncio.CancelledError, Exception):
                pass
            await client.aclose()

    app = FastAPI(
        title="SQL-Like Log Query Engine — Coordinator",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount static assets (JS + CSS) and expose the shared Jinja2Templates
    # instance via ``app.state.templates`` so the route handlers can render
    # ``index.html`` without rebuilding the Templates object on every call.
    # We only mount when the directory actually exists, which keeps the app
    # importable in environments where the UI assets weren't copied in.
    if _STATIC_DIR.is_dir():
        app.mount(
            "/static",
            StaticFiles(directory=str(_STATIC_DIR)),
            name="static",
        )
    if _TEMPLATES_DIR.is_dir():
        app.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    else:
        app.state.templates = None

    app.include_router(router)
    return app
