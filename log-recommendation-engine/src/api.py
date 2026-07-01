"""FastAPI application factory for the Log Recommendation Engine.

This module is intentionally minimal for the C1 skeleton: it wires a single
dependency-free ``GET /health`` route so the container healthcheck and the test
suite have something concrete to assert against. Later commits attach the real
routers (incidents, recommend, feedback, config, system) onto the app produced by
:func:`create_app`, so the structure here is deliberately router-ready.

``GET /health`` is a pure liveness probe in C1 — it must NOT touch Postgres or Redis
(a deep readiness ``/health`` reporting per-subsystem booleans arrives in C13). This
keeps the healthcheck green the instant uvicorn binds.
"""

from __future__ import annotations

from fastapi import FastAPI

from src import observability
from src.routers import incidents as incidents_router
from src.routers import recommend as recommend_router

#: Reported in the /health payload and (later) elsewhere. Bumped per release.
SERVICE_VERSION = "0.1.0"
SERVICE_NAME = "log-recommendation-engine"


def create_app() -> FastAPI:
    """Build and return the FastAPI application.

    Returns:
        A configured :class:`FastAPI` instance exposing ``GET /health``. Future
        commits register additional routers on this same instance and add the
        Prometheus request middleware (C14).
    """
    # Configure structured logging once at startup (best-effort; never crashes).
    observability.configure_logging()

    app = FastAPI(
        title="Log Recommendation Engine",
        version=SERVICE_VERSION,
        description=(
            "Matches a new incident against a historical incident corpus using "
            "semantic + contextual similarity and returns a ranked list of solution "
            "suggestions that improves over time via a feedback loop."
        ),
    )

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        """Liveness probe — dependency-free (does not touch Postgres/Redis in C1)."""
        return {
            "status": "ok",
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION,
        }

    # Incident-corpus routes (C3). Later commits register feedback / config /
    # system routers on this same app.
    app.include_router(incidents_router.router)
    # Recommendation route (C9): POST /recommend — the core deliverable.
    app.include_router(recommend_router.router)

    return app


#: Module-level ASGI app so the container can run `uvicorn src.api:app`.
app = create_app()
