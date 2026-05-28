from __future__ import annotations

import pytest

from src.main import app


def test_sse_route_is_registered() -> None:
    """ASGITransport does not support true SSE streaming, so verify the route
    is mounted instead of consuming events. Real SSE behavior is exercised
    against the running container during the E2E flow (commit 20).
    """
    paths = [getattr(r, "path", None) for r in app.router.routes]
    assert "/api/metrics/live" in paths
    assert "/api/metrics/snapshot" in paths


def test_runs_subroutes_registered() -> None:
    paths = [getattr(r, "path", None) for r in app.router.routes]
    assert "/api/runs/{run_id}/bottlenecks" in paths
    assert "/api/runs/{run_id}/recommendations" in paths
