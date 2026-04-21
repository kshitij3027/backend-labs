"""FastAPI application entry point.

The module exports a :func:`build_app` factory and a module-level
``app = build_app()`` so ``uvicorn src.main:app`` still resolves
directly. Later commits will extend the factory with routers,
lifespan wiring, and component construction — this commit keeps the
shape minimal but forward-compatible: settings are read at build
time, structured JSON logging is installed once, and the
:class:`Settings` instance is stashed on ``app.state`` so handlers
can introspect it later without re-reading the env.
"""

from fastapi import FastAPI

from src.config import Settings, get_settings
from src.logging_setup import configure_logging
from src.models import HealthResponse


def build_app(settings: Settings | None = None) -> FastAPI:
    """Construct a fresh FastAPI app, optionally with custom settings.

    ``build_app()`` with no argument behaves identically to the
    module-level ``app = build_app()``. Passing a :class:`Settings`
    instance lets tests isolate configuration without mutating the
    cached singleton or the process environment.

    Logging is configured here — callers that construct multiple apps
    in-process (tests) pay a cheap handler swap each time, which is
    the intended behaviour of :func:`configure_logging`.
    """
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(title="log-fulltext-search-rerank")
    # Stash on state so later-commit routes/middleware can reach the
    # same instance via ``request.app.state.settings`` without importing
    # the cached accessor.
    app.state.settings = settings

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Liveness probe used by Docker and the start.sh wait loop.

        Response shape is locked to ``{"status": "ok"}`` — the compose
        healthcheck and ``tests/test_bootstrap.py`` both assert on it.
        """
        return HealthResponse()

    return app


# Module-level app instance for ``uvicorn src.main:app``. The Dockerfile
# CMD and ``start.sh`` both import this symbol directly.
app = build_app()
