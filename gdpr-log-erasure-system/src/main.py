"""FastAPI app entry point — C1 ships the bare shell.

This commit deliberately wires NO database, redis, or business logic.
The lifespan only initialises logging and stashes settings on app.state
so subsequent commits (C12 in particular) can layer the DB engine,
redis client, scheduler, and richer ``/health`` payload on top without
restructuring this module.

Routes added later go into ``src/api/*`` and are mounted via
``app.include_router``. C1 exposes ``/health`` inline so the compose
healthcheck has something to hit immediately.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.logging_config import configure_logging, get_logger
from src.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger(__name__)
    log.info("startup", host=settings.api_host, port=settings.api_port)
    app.state.settings = settings
    try:
        yield
    finally:
        log.info("shutdown")


app = FastAPI(title="GDPR Log Erasure System", version="0.1.0", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
