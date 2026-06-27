"""FastAPI application factory for the Predictive Log Analytics Engine.

This module is intentionally minimal for the C0 skeleton: it wires a single
dependency-free ``GET /health`` route so the container healthcheck and the test
suite have something concrete to assert against. Later commits attach the real
routers (predictions, forecast, metrics, models, retrain) onto the app produced by
:func:`create_app`, so the structure here is deliberately router-ready.
"""

from __future__ import annotations

import math
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.routers import metrics as metrics_router


def _sanitize(obj: Any) -> Any:
    """Recursively make ``obj`` JSON-serializable.

    FastAPI's default validation handler echoes the rejected ``input`` back into
    the error body. When a client POSTs raw ``NaN`` / ``Infinity`` / ``-Infinity``
    JSON tokens, that input is a non-finite float, and ``json.dumps`` raises
    ``ValueError: Out of range float values are not JSON compliant`` — turning a
    clean 422 into a 500. Replace non-finite floats with their string repr and,
    defensively, any other non-serializable leaf with its ``repr``.
    """
    if isinstance(obj, float):
        if math.isnan(obj):
            return "NaN"
        if math.isinf(obj):
            return "Infinity" if obj > 0 else "-Infinity"
        return obj
    if isinstance(obj, dict):
        return {key: _sanitize(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(item) for item in obj]
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    # Anything else (e.g. exotic objects in the error context) -> safe string.
    return repr(obj)

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

    @app.exception_handler(RequestValidationError)
    async def _validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Return a clean 422 for request validation errors.

        Mirrors FastAPI's default 422 shape (a list of error dicts under
        ``detail``) but sanitizes the echoed ``input`` so non-finite floats
        (``NaN`` / ``Infinity`` / ``-Infinity``) don't crash ``json.dumps`` and
        surface as a 500. Ordinary errors (missing field, wrong type) are
        unchanged.
        """
        sanitized = [_sanitize(error) for error in exc.errors()]
        return JSONResponse(status_code=422, content={"detail": sanitized})

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

    # Metric ingestion (POST /metrics) + read-back (GET /metrics/{metric_name}).
    # Later commits add the predictions / forecast / models / analytics routers.
    app.include_router(metrics_router.router)

    return app


#: Module-level ASGI app so the container can run `uvicorn src.api:app`.
app = create_app()
