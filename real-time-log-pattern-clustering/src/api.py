"""FastAPI application factory for the Real-Time Log Pattern Clustering engine.

Commit 1 surface (foundation only): this wires configuration into the app and
exposes a minimal HTTP surface so the container is immediately healthy:

* On startup (the FastAPI **lifespan**) the resolved :class:`~src.config.AppConfig`
  is loaded into ``app.state.config``. Warming up the clustering engine (fitting the
  initial model on historical/batch logs) is **not** done here yet — see the TODO in
  :func:`lifespan` — so ``/health`` stays trivial and the container reports healthy
  the moment uvicorn binds.
* Permissive CORS is enabled so the browser dashboard (served from a different
  origin in a later commit) can call the API directly.
* Routes:
  - ``GET /health`` — a dependency-free liveness probe (used by the Docker
    healthcheck). Always reports the three clustering algorithms this engine runs.
  - ``GET /`` — a tiny service banner pointing at the docs.

Everything else (preprocessing, feature extraction, the concurrent K-means / DBSCAN
/ HDBSCAN engine, the streaming API, the metrics WebSocket and the dashboard) arrives
in later commits.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config import load_config
from src.schemas import HealthResponse

#: Service version reported by ``/health``. Bump as the engine evolves.
APP_VERSION = "0.1.0"

#: The clustering algorithms this engine runs concurrently (project requirements §2).
ALGORITHMS = ["kmeans", "dbscan", "hdbscan"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: load config on startup, clean up on shutdown.

    The resolved :class:`~src.config.AppConfig` is stashed on ``app.state.config`` so
    request handlers and (later) the clustering engine can read it without re-parsing
    YAML.
    """
    app.state.config = load_config()

    # TODO(commit: engine warm-up): construct the clustering engine here and fit the
    # initial model on historical/batch logs BEFORE serving, then expose it via
    # app.state.engine so /health can optionally report readiness. Kept out of C1 so
    # the container is healthy immediately (no model training on startup yet).

    yield

    # No resources to release in C1.


def create_app() -> FastAPI:
    """Build and return the FastAPI application.

    Returns:
        A configured :class:`fastapi.FastAPI` instance with permissive CORS and the
        Commit-1 routes (``/health`` and ``/``).
    """
    app = FastAPI(
        title="Real-Time Log Pattern Clustering",
        version=APP_VERSION,
        lifespan=lifespan,
    )

    # Permissive CORS so the dashboard (different origin) can call the API directly.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        """Liveness probe — dependency-free so the container is healthy immediately."""
        return HealthResponse(
            status="ok", version=APP_VERSION, algorithms=ALGORITHMS
        )

    @app.get("/")
    def root() -> dict:
        """Tiny service banner pointing at the interactive docs."""
        return {"service": "real-time-log-pattern-clustering", "docs": "/docs"}

    return app
