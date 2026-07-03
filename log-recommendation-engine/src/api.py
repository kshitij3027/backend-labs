"""FastAPI application factory for the Log Recommendation Engine.

This module wires the FastAPI app and attaches every router (incidents, recommend,
feedback, config, system). The application factory :func:`create_app` is the single
place routers are registered, so the structure here stays thin and router-ready.

``GET /health`` is a **deep** readiness probe from C13 (defined on the system router):
it reports per-subsystem status (database / redis / embedding_model) and the corpus
size, yet **always returns HTTP 200 while the process is alive** so the container
healthcheck stays green the instant uvicorn binds — a degraded dependency is signalled
in the body, not via a non-2xx status.
"""

from __future__ import annotations

from fastapi import FastAPI

from src import observability
from src.routers import config as config_router
from src.routers import feedback as feedback_router
from src.routers import incidents as incidents_router
from src.routers import recommend as recommend_router
from src.routers import system as system_router

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

    # Incident-corpus routes (C3): search / filter / fetch the corpus.
    app.include_router(incidents_router.router)
    # Recommendation route (C9): POST /recommend — the core deliverable.
    app.include_router(recommend_router.router)
    # Feedback route (C10): POST /feedback — records votes into the learned aggregate.
    app.include_router(feedback_router.router)
    # Runtime config routes (C12): GET/PUT /config — live retuning of ranking knobs.
    app.include_router(config_router.router)
    # System routes (C13): GET /stats (corpus/feedback rollup) + deep GET /health.
    app.include_router(system_router.router)

    return app


#: Module-level ASGI app so the container can run `uvicorn src.api:app`.
app = create_app()
