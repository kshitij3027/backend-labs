"""FastAPI application entry point for the Intelligent Log Redaction Engine.

C1 scope: bootstrap-only. This module wires up the bare minimum needed to
prove the Docker substrate works:

* A FastAPI app with project metadata (title / description / version).
* Root-level stdlib logging configured from ``Settings.LOG_LEVEL``.
* A single ``GET /api/health`` endpoint returning the documented JSON body.

Detection, redaction, audit, stats, and the dashboard are all introduced in
later commits (C2+). Keeping this file deliberately empty of business logic
means the C1 smoke test only exercises the health endpoint, which in turn
keeps the build cache for ``Dockerfile`` cheap to invalidate as we layer
new endpoints on top in subsequent commits.
"""
from __future__ import annotations

import logging

from fastapi import FastAPI

from src.settings import get_settings


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
#
# Configure the root logger once at module import. Uvicorn installs its own
# access-log + error-log loggers; this call exists so any direct
# ``logging.getLogger(...)`` calls in our code respect the ``LOG_LEVEL`` env
# var. We resolve settings at module scope (not lazily) because the log level
# is needed before the first request lands.

_settings = get_settings()

logging.basicConfig(
    level=_settings.LOG_LEVEL.upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)
logger.info("startup: log-redaction-engine booting (log_level=%s)", _settings.LOG_LEVEL)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


app = FastAPI(
    title="Log Redaction Engine",
    description=(
        "Real-time log processing service that detects and redacts sensitive "
        "data (PII, PHI, payment info) from log entries using configurable "
        "strategies. Exposes a REST API plus a live dashboard."
    ),
    version="0.1.0",
)


@app.get("/api/health")
async def health() -> dict[str, str]:
    """Liveness / readiness probe used by Docker HEALTHCHECK and orchestrators.

    Returns a deterministic JSON body the C1 smoke test asserts against:

        {"status": "healthy", "service": "log-redaction-engine"}

    No dependencies are checked here — this endpoint must remain dependency-
    free so a transient external outage cannot flip the container unhealthy
    and cause a restart loop. Deeper checks (Redis reachability, NER model
    loaded, etc.) belong on a separate ``/api/ready`` endpoint when wired in.
    """
    return {"status": "healthy", "service": "log-redaction-engine"}
