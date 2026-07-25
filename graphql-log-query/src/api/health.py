"""Liveness probe — ``GET /health``.

.. rubric:: The body is exactly ``{"status": "healthy"}`` and nothing else

That literal shape is a requirement, not a default: the spec pins it twice — once in §2 ("Server
exposes ``/health`` returning ``{"status": "healthy"}``") and again as the only literal payload in
its §8 Input/Output sample. So this route deliberately does **not** report a version, an uptime, a
row count, or anything else a richer probe might want to expose. Adding a field would be a
one-line change and a contract break, and there is a test asserting the key set is exactly
``{"status"}`` to make that break loud rather than gradual.

.. rubric:: It is dependency-free on purpose — no Postgres, no Redis, no GraphQL

The container ``HEALTHCHECK`` and compose's ``condition: service_healthy`` both target this route,
and the ``e2e`` / ``loadtest`` services wait on it before they start. It therefore answers exactly
one question: **is this process listening?** A probe that also checked Postgres would report the
API unhealthy while its dependency reconnects, and Docker would restart a process that is working
perfectly — turning a transient database blip into a container restart loop, and making the
compose gate that the harnesses depend on flap. Data-layer readiness is a *startup* concern,
handled by C2's ``init_db`` retry loop; liveness is this, and only this.

That also means the handler takes no ``Request`` and touches no ``app.state``: there is nothing
here that a half-wired application could make fail, which is the strongest form of the guarantee
"returns 200 for as long as the process is alive".
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])

#: The one and only value ``status`` ever takes. A constant rather than a literal in the return
#: statement so the test and the handler cannot drift into disagreeing about the spelling.
HEALTHY_STATUS = "healthy"


class HealthResponse(BaseModel):
    """The ``GET /health`` body: one field, fixed by the spec.

    Declared as a model (rather than returning a bare dict) so the shape is published in the
    OpenAPI document and so FastAPI's ``response_model`` filtering enforces it — if a later
    commit returns extra keys from the handler, they are stripped rather than served.
    """

    status: str = Field(description="Always 'healthy' while the process is serving.")


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description=(
        "Dependency-free liveness check: it queries neither Postgres nor Redis, so it returns "
        "200 as soon as uvicorn binds and for as long as the process is alive."
    ),
)
def health() -> HealthResponse:
    """Report liveness. No arguments, no I/O, no failure mode."""
    return HealthResponse(status=HEALTHY_STATUS)
