"""Prometheus ``/metrics`` endpoint.

Renders the chaos-framework :class:`CollectorRegistry` (defined in
``observability/prom.py``) in the standard Prometheus text exposition
format. Stays a thin wrapper so the renderer can be unit-tested without
spinning up a FastAPI app.
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response

from ..observability.prom import render_latest

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def metrics_endpoint() -> Response:
    """Serve Prometheus-formatted metrics for the framework's own registry."""
    body, content_type = render_latest()
    return Response(content=body, media_type=content_type)
