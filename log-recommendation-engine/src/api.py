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

import os
import threading

from fastapi import FastAPI

from src import embeddings, observability
from src.config import get_settings
from src.routers import config as config_router
from src.routers import feedback as feedback_router
from src.routers import incidents as incidents_router
from src.routers import recommend as recommend_router
from src.routers import system as system_router

logger = observability.get_logger(__name__)

#: Reported in the /health payload and (later) elsewhere. Bumped per release.
SERVICE_VERSION = "0.1.0"
SERVICE_NAME = "log-recommendation-engine"


def _warmup_enabled() -> bool:
    """Whether to warm the embedding model on startup.

    The ``WARMUP_ON_STARTUP`` env var wins when set (so tests can force it off with
    ``WARMUP_ON_STARTUP=false`` for an instant, deterministic startup); otherwise the
    ``settings.warmup_on_startup`` default (True) applies. Accepts the usual truthy /
    falsy spellings for the env override.
    """
    raw = os.environ.get("WARMUP_ON_STARTUP")
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return bool(get_settings().warmup_on_startup)


def _start_model_warmup() -> None:
    """Kick off :func:`src.embeddings.warmup` in a background **daemon** thread.

    Non-blocking on purpose: the ~90 MB model load must not delay uvicorn binding or
    the first ``/health`` (which stays 200 and simply reports
    ``embedding_model: false`` until the load finishes, then flips to true). The
    thread is a daemon so it never blocks interpreter shutdown, and ``warmup()``
    itself swallows all errors, so a failed load leaves the service running (lazy
    load will retry on the first request).
    """
    thread = threading.Thread(
        target=embeddings.warmup,
        name="embedding-warmup",
        daemon=True,
    )
    thread.start()
    logger.info("embedding model warmup started (background daemon thread)")


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

    # Prometheus request middleware (C14): times every request and records the count
    # + latency against the matched route's path *template* (bounded cardinality).
    # The metric singletons live at module scope in ``observability`` so repeated
    # ``create_app()`` calls (e.g. across tests) never re-register a collector.
    app.add_middleware(observability.PrometheusMiddleware)

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

    # Model warmup on startup (C21): load the embedding singleton in a background
    # daemon thread so the heavy load happens once, off the request path, instead of
    # lazily on the first /recommend. Non-blocking, so uvicorn binds immediately and
    # /health answers right away (reporting embedding_model:false until warmup lands,
    # then true). Gated by WARMUP_ON_STARTUP (env overrides the config default) so
    # tests can disable it for an instant, deterministic startup.
    @app.on_event("startup")
    def _warmup_model_on_startup() -> None:  # pragma: no cover - exercised in Docker
        if _warmup_enabled():
            _start_model_warmup()
        else:
            logger.info("embedding model warmup disabled (WARMUP_ON_STARTUP)")

    return app


#: Module-level ASGI app so the container can run `uvicorn src.api:app`.
app = create_app()
