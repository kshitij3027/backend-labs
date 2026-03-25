"""FastAPI application for Time-Based Windowed Log Analytics."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from fastapi import FastAPI

from src.config import AppConfig


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: load config and configure logging."""
    config = AppConfig.from_env()
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
    )
    log = structlog.get_logger()
    log.info("starting", api_port=config.api_port, redis_host=config.redis_host)
    app.state.config = config
    yield
    log.info("shutting_down")


app = FastAPI(
    title="Time-Based Windowed Log Analytics",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}
