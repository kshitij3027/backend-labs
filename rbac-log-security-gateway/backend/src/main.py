"""FastAPI application factory. Run with: `uvicorn src.main:app`."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.auth import router as auth_router
from src.config import get_settings
from src.middleware.audit import AuditMiddleware
from src.shared import audit_service


def _configure_logging(level: str) -> None:
    """Wire structlog + stdlib logging to emit JSON to stdout."""
    logging.basicConfig(level=level.upper(), format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper(), logging.INFO)),
        cache_logger_on_first_use=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    _configure_logging(settings.app_log_level)
    log = structlog.get_logger("rbac-gateway")
    log.info("startup", host=settings.app_host, port=settings.app_port)
    yield
    log.info("shutdown")


def build_app() -> FastAPI:
    """Construct the FastAPI app. Pure function — call repeatedly in tests."""
    settings = get_settings()

    app = FastAPI(
        title="RBAC Log Security Gateway",
        version="0.1.0",
        description="JWT auth + role-based authorization + audit logging for log queries.",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # AuditMiddleware is added LAST so it wraps the rest of the stack (Starlette adds
    # middleware in LIFO order — last `add_middleware` runs FIRST per request).
    # This ensures every request, including CORS preflights, is recorded.
    app.add_middleware(AuditMiddleware, audit_service=audit_service)

    app.include_router(auth_router)

    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = build_app()
