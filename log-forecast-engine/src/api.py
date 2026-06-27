"""FastAPI application factory for the Predictive Log Analytics Engine.

This module is intentionally minimal for the C0 skeleton: it wires a single
dependency-free ``GET /health`` route so the container healthcheck and the test
suite have something concrete to assert against. Later commits attach the real
routers (predictions, forecast, metrics, models, retrain) onto the app produced by
:func:`create_app`, so the structure here is deliberately router-ready.
"""

from __future__ import annotations

from fastapi import FastAPI

#: Reported in the /health payload and (later) elsewhere. Bumped per release.
SERVICE_VERSION = "0.1.0"
SERVICE_NAME = "log-forecast-engine"


def create_app() -> FastAPI:
    """Build and return the FastAPI application.

    Returns:
        A configured :class:`FastAPI` instance exposing ``GET /health``. Future
        commits register additional routers on this same instance.
    """
    app = FastAPI(
        title="Predictive Log Analytics Engine",
        version=SERVICE_VERSION,
        description=(
            "Forecasts future system metrics (response times, error rates, "
            "throughput) from log-derived time series using an ensemble of "
            "time-series models."
        ),
    )

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        """Liveness probe.

        Dependency-free in this build, so the service is healthy as soon as uvicorn
        binds. Later commits extend this to report model status, Redis connectivity,
        and performance metrics.
        """
        return {
            "status": "ok",
            "service": SERVICE_NAME,
            "version": SERVICE_VERSION,
        }

    return app


#: Module-level ASGI app so the container can run `uvicorn src.api:app`.
app = create_app()
