"""Unversioned liveness probe — ``GET /health``.

**Why this route is deliberately NOT under ``/api/v1``.** Every data route in this project is
hard-prefixed ``/api/v1`` precisely so a breaking change can arrive as a new namespace instead
of a changelog entry. ``/health`` is the opposite kind of thing: it is a *liveness contract*
consumed by the container HEALTHCHECK, compose's ``condition: service_healthy``, and any
orchestrator probe — none of which should have to be redeployed the day ``/api/v2`` ships. A
probe that moves with the API version is a probe that breaks your rollout. So it stays put,
alongside the other unversioned paths (``/docs``, ``/redoc``, ``/openapi.json``).

It is also dependency-free by design: **no authentication, no role check and no rate limiting**.
A liveness probe that can return 401 or 429 is not a liveness probe. Everything is read
defensively off ``request.app.state.runtime`` via ``getattr``, so a missing or half-wired
runtime degrades to zeroed counters rather than raising — the route must return 200 for as
long as the process is alive, full stop.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    """The ``GET /health`` body. Small, stable, and safe to expose unauthenticated."""

    status: str = Field(description="Always 'healthy' while the process is serving.")
    version: str = Field(description="API version reported by the FastAPI app.")
    uptime_sec: float = Field(description="Seconds since the runtime was constructed.")
    store_entries: int = Field(description="Entries currently resident in the log ring.")


def _store_entries(runtime: object | None) -> int:
    """Best-effort count of entries resident in the log store; 0 when there is no store.

    C4's ``LogStore`` implements ``__len__``, so ``len(store)`` is the forward-compatible
    probe. Any failure at all — no runtime, no store yet (the C1 state), or a store that
    somehow does not support ``len`` — degrades to 0, because this function exists inside a
    route that is not allowed to fail.
    """
    store = getattr(runtime, "store", None)
    if store is None:
        return 0
    try:
        return int(len(store))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description=(
        "Dependency-free liveness check. Public, unversioned, unauthenticated and unmetered; "
        "always returns 200 while the process is alive."
    ),
)
def health(request: Request) -> HealthResponse:
    """Report liveness plus two cheap, non-sensitive vital signs.

    ``version`` is read off ``request.app.version`` rather than imported from ``src.main``:
    ``src.main`` imports this router, so importing back would be a cycle, and the app object
    is the authoritative source of its own version anyway.
    """
    runtime = getattr(request.app.state, "runtime", None)
    uptime_sec = float(getattr(runtime, "uptime_sec", 0.0) or 0.0)
    return HealthResponse(
        status="healthy",
        version=str(getattr(request.app, "version", "")),
        uptime_sec=round(uptime_sec, 3),
        store_entries=_store_entries(runtime),
    )
