"""FastAPI entrypoint for the Log Pattern Alerting System."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.api.alerts import router as alerts_router
from src.api.health import router as health_router
from src.config import get_settings
from src.models import Base

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage startup and shutdown of database and Redis connections."""
    settings = get_settings()

    # Create async engine and session factory
    engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
    )
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables verified")

    # Connect to Redis
    redis_client = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
    )
    try:
        await redis_client.ping()
        logger.info("Redis connected")
    except Exception as exc:
        logger.warning("Redis connection failed on startup: %s", exc)

    # Store in app state
    app.state.engine = engine
    app.state.async_session = session_factory
    app.state.redis = redis_client
    app.state.settings = settings

    try:
        yield
    finally:
        # Shutdown
        await engine.dispose()
        await redis_client.close()
        logger.info("Connections closed")


app = FastAPI(
    title="Log Pattern Alerting System",
    lifespan=lifespan,
)

# CORS middleware (allow all origins for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(alerts_router, prefix="", tags=["alerts"])
app.include_router(health_router)
