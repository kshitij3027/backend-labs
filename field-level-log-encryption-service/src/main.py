"""FastAPI application entry point for the Field-Level Log Encryption Service.

C1 scope: bootstrap only — a single health endpoint so Docker + the test
substrate can be verified. Business logic (detection, crypto, keystore,
audit, dashboard) is layered on in C2+ per `plan.md`.
"""
from __future__ import annotations

import logging

from fastapi import FastAPI

from src.settings import settings


def _configure_logging(level: str) -> None:
    """Configure stdlib logging once at module import.

    Uvicorn also wires its own loggers; this just makes sure any direct
    `logging.getLogger(...)` calls in our code respect the configured level.
    """
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


_configure_logging(settings.log_level)

app = FastAPI(
    title="Field-Level Log Encryption Service",
    description=(
        "Middleware that detects PII in structured log entries and selectively "
        "encrypts the sensitive fields using AES-256-GCM while leaving "
        "operational fields readable."
    ),
    version="0.1.0",
)


@app.get("/api/health", tags=["health"])
async def health() -> dict[str, str]:
    """Liveness probe used by Docker healthcheck and the test suite."""
    return {"status": "healthy", "service": "field-encryption-service"}
