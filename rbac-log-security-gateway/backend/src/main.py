"""FastAPI application factory. Run with: `uvicorn src.main:app`."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.admin import router as admin_router
from src.api.auth import router as auth_router
from src.api.logs import router as logs_router
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


def _seed_demo_audit() -> None:
    """Pre-populate a few audit entries + one security event so the admin dashboard isn't empty on first load."""
    from datetime import datetime, timezone, timedelta

    from src.audit.models import AuditEntry, SecurityEvent
    from src.shared import audit_service

    now = datetime.now(timezone.utc)
    seeds = [
        AuditEntry(
            timestamp=now - timedelta(minutes=10), user_id="seed", username="alice",
            method="POST", path="/api/auth/login", status=200, duration_ms=12.3,
            source_ip="10.0.0.4", user_agent="seed/1.0", decision="n/a",
        ),
        AuditEntry(
            timestamp=now - timedelta(minutes=8), user_id="seed", username="bob",
            method="GET", path="/api/logs/search", status=200, duration_ms=4.1,
            source_ip="10.0.0.5", user_agent="seed/1.0", decision="allow",
            rule="logs:read:application.*", reason="allow match",
        ),
        AuditEntry(
            timestamp=now - timedelta(minutes=5), user_id="seed", username="bob",
            method="GET", path="/api/logs/search", status=403, duration_ms=2.4,
            source_ip="10.0.0.5", user_agent="seed/1.0", decision="deny",
            rule="!logs:read:business.*", reason="explicit deny",
        ),
    ]
    for entry in seeds:
        audit_service.append(entry)

    audit_service.append_security_event(SecurityEvent(
        timestamp=now - timedelta(minutes=3),
        event_type="auth_failure",
        username=None,
        path="/api/auth/login",
        status=401,
        source_ip="10.0.0.99",
        reason="bad password",
    ))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    _configure_logging(settings.app_log_level)
    _seed_demo_audit()
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
    app.include_router(logs_router)
    app.include_router(admin_router)

    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = build_app()
