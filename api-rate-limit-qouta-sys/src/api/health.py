"""Unversioned liveness probe — ``GET /health``.

**Why this route is deliberately NOT under ``/api/v1``.** Every metered route in this project is
hard-prefixed ``/api/v1`` precisely so a breaking change can arrive as a new namespace instead of
a changelog entry. ``/health`` is the opposite kind of thing: it is a *liveness contract* consumed
by the container HEALTHCHECK, compose's ``condition: service_healthy``, nginx's upstream check
(C12) and any orchestrator probe — none of which should have to be redeployed the day
``/api/v2`` ships.

**Why it is never metered.** The limiter middleware exempts this path, and that is not a
convenience: a liveness probe that can return 429 is not a liveness probe. The container's own
HEALTHCHECK polls every 10 s from the same source address, so metering it would eventually mark a
perfectly healthy replica unhealthy and have compose restart it — the limiter would have taken the
service down by working correctly. Same reasoning for authentication: this route is
dependency-free and unauthenticated, and everything is read defensively off
``request.app.state.runtime`` via ``getattr``, so a missing or half-wired runtime degrades to
zeroed vitals rather than raising.

.. rubric:: The response body

``status`` and ``rate_limiter`` are the spec's two keys, **verbatim** — same names, same values,
top level, not nested. Everything else is additive:

* ``version`` — the running API version, so a rollout can be observed.
* ``uptime_sec`` — how long this process has been up.
* ``served_by`` — this container's hostname. C12 runs two replicas behind a load balancer, and
  this field is how the E2E verifier proves that *both* of them answered: without it, the
  distributed double-spend check would pass trivially against a single replica and the bug the
  whole project exists to catch would go undetected.

``rate_limiter`` is the constant ``"active"`` for now; C8 makes it report ``"degraded"`` (still
with a 200) when Redis is unreachable and the fallback bucket is carrying the load. A silent
fail-open is indistinguishable from having no rate limiter at all, which is why the degraded state
has to be visible on the one endpoint everything already polls.
"""

from __future__ import annotations

import socket

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])

#: The spec's two literal values. Named constants because the E2E verifier asserts them
#: character-for-character, and a "harmless" rewording here would fail a check on the other side
#: of a container boundary.
STATUS_HEALTHY = "healthy"
RATE_LIMITER_ACTIVE = "active"


def _hostname() -> str:
    """This container's hostname, or ``"unknown"`` if the OS declines to say.

    Resolved once at import rather than per request: the hostname cannot change under a running
    process, and ``/health`` is polled every 10 s by the container healthcheck plus once per
    dashboard refresh, so there is no reason to pay a syscall for a constant.
    """
    try:
        return socket.gethostname() or "unknown"
    except OSError:  # pragma: no cover - gethostname does not fail on a supported platform
        return "unknown"


#: Under compose this is the container id, which is exactly what makes two replicas
#: distinguishable to the E2E verifier without any coordination between them.
SERVED_BY: str = _hostname()


class HealthResponse(BaseModel):
    """The ``GET /health`` body. Small, stable, and safe to expose unauthenticated."""

    status: str = Field(description="Always 'healthy' while the process is serving.")
    rate_limiter: str = Field(
        description="'active' while enforcement is backed by Redis; 'degraded' on the C8 "
        "fail-open fallback path. Never absent — a missing field would read as 'no limiter'."
    )
    version: str = Field(description="API version reported by the FastAPI app.")
    uptime_sec: float = Field(description="Seconds since the runtime was constructed.")
    served_by: str = Field(
        description="Hostname of the replica that answered. Proves the load balancer is "
        "fanning out across replicas rather than pinning to one."
    )


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description=(
        "Dependency-free liveness check. Public, unversioned, unauthenticated and never rate "
        "limited; always returns 200 while the process is alive."
    ),
)
def health(request: Request) -> HealthResponse:
    """Report liveness, the limiter's state, and two cheap non-sensitive vital signs.

    ``version`` is read off ``request.app.version`` rather than imported from ``src.main``:
    ``src.main`` imports this router, so importing back would be a cycle, and the app object is
    the authoritative source of its own version anyway.
    """
    runtime = getattr(request.app.state, "runtime", None)
    uptime_sec = float(getattr(runtime, "uptime_sec", 0.0) or 0.0)
    return HealthResponse(
        status=STATUS_HEALTHY,
        rate_limiter=RATE_LIMITER_ACTIVE,
        version=str(getattr(request.app, "version", "")),
        uptime_sec=round(uptime_sec, 3),
        served_by=SERVED_BY,
    )
