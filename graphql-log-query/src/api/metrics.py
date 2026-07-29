"""Prometheus scrape endpoint — ``GET /metrics``.

Text exposition (``text/plain; version=0.0.4``) generated from the application's **own**
:class:`~src.metrics.Metrics` registry, which :func:`src.main.lifespan` builds and attaches to
``app.state.metrics``. Reading it off the request rather than from a module global is what lets two
applications in one test process report independently — see the registry note in :mod:`src.metrics`.

.. rubric:: Why this is a separate module from ``/health``

They answer opposite questions and have opposite dependency rules. ``/health`` is the container's
liveness probe: it must touch nothing, so that "is this process listening" cannot be made to fail by
anything else being unwell. ``/metrics`` exists precisely to reach into the process's state — the
broker's subscriber count, the cache's counters, the persisted-query store's counters — and read it
all out. Keeping them in one module would put a route that reads ``app.state`` next to one whose
whole value proposition is that it does not.

.. rubric:: It is registered ONLY when ``METRICS_ENABLED`` is set

``src.main.create_app`` includes this router conditionally, so a disabled deployment answers **404**
rather than an empty 200 — the same shape ``GET /graphql`` takes when ``GRAPHQL_PLAYGROUND_ENABLED``
is false. A 404 is an honest "this server does not serve that", while an empty 200 is a scrape
target that reports zero series forever and looks, on a dashboard, exactly like a service that has
served no traffic.

.. rubric:: A failed scrape is an empty 200, never a 500

The one case where the route is registered and the registry is missing is a
``METRICS_ENABLED=true`` deployment whose ``prometheus_client`` will not import (see the guarded
import in :mod:`src.metrics`). That answers 200 with an empty body and the correct content type,
which Prometheus reads as "this target currently has no series". Answering 500 would page an
operator about an outage that is not happening — the service is serving requests perfectly and has
merely lost its instrumentation.
"""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from src.metrics import PROMETHEUS_CONTENT_TYPE, Metrics

router = APIRouter(tags=["metrics"])


@router.get(
    "/metrics",
    summary="Prometheus text exposition",
    description=(
        "GraphQL operation and per-field timings, the active subscription count, and the broker / "
        "result cache / persisted query counters, in the Prometheus text exposition format. "
        "Registered only when METRICS_ENABLED is set."
    ),
    responses={200: {"content": {"text/plain": {}}, "description": "Prometheus text exposition"}},
    response_class=Response,
)
def metrics(request: Request) -> Response:
    """Render this application's registry.

    Takes the ``Request`` — unlike :func:`src.api.health.health`, deliberately — because the
    registry belongs to the application instance rather than to the module. Synchronous because
    ``generate_latest`` is CPU-bound and short: making it ``async def`` would run it directly on the
    event loop, while a plain ``def`` lets Starlette put it on the threadpool and keeps a scrape from
    sitting in front of an in-flight query.
    """
    instrumentation = getattr(request.app.state, "metrics", None)
    if not isinstance(instrumentation, Metrics):
        return Response(content=b"", media_type=PROMETHEUS_CONTENT_TYPE)

    body, content_type = instrumentation.render()
    return Response(content=body, media_type=content_type)
