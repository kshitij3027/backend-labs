"""FastAPI app entry point for the automated-log-retention service.

C01 ships a minimal shell: a no-op lifespan that just logs startup and
shutdown, and a single ``GET /api/health`` endpoint. Later commits wire
up the persistence layer (C02), policy/compliance stack (C03–C05),
storage tiers (C06–C08), lifecycle jobs (C09–C11), APScheduler (C12),
audit chain (C13), compliance reports (C14–C15), and the HTMX dashboard
(C16–C18) — all attached via this same ``lifespan``.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.settings import get_settings


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """No-op lifespan for C01 — wiring lands in later commits."""
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())
    logger.info("automated-log-retention: startup")
    try:
        yield
    finally:
        logger.info("automated-log-retention: shutdown")


app = FastAPI(
    title="automated-log-retention",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/api/health")
async def health() -> dict:
    """Liveness probe consumed by the Docker HEALTHCHECK.

    Returns the canonical ``{"status":"healthy","timestamp": <int>}``
    shape. ``timestamp`` is a unix epoch ``int`` (per plan — not a
    datetime/ISO string) so it survives JSON round-trip without any
    timezone ambiguity and is cheap to compare in tests.
    """
    return {"status": "healthy", "timestamp": int(time.time())}
