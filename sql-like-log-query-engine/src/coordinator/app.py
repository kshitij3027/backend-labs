"""FastAPI application factory for the coordinator service."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.shared.config import CoordinatorSettings

from .executor import QueryExecutor
from .progress import ProgressRegistry
from .registry import PartitionRegistry
from .routes import router


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

    app.include_router(router)
    return app
