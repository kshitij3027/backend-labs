from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.settings import Settings, get_settings

logger = logging.getLogger("storage_format_optimizer")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the object graph on startup, tear down in reverse on shutdown.

    For now this only stashes Settings on app.state; later commits attach the
    manifest store, engines, migration loop, and WebSocket broadcast loop here.
    """
    settings: Settings = get_settings()
    app.state.settings = settings
    logger.info("storage-format-optimizer starting (data_dir=%s)", settings.data_dir)
    try:
        yield
    finally:
        logger.info("storage-format-optimizer shutting down")


app = FastAPI(title="Adaptive Storage Format Optimizer", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy"}


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
