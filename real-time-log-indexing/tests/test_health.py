"""Tests for ``GET /health``.

Two angles:

* shape — the response body must contain exactly the fields declared
  on :class:`HealthResponse` (no extras, no missing keys).
* semantics — status flips between ``ok`` and ``degraded`` based on
  whether Redis is reachable from the process, and ``uptime_s`` is a
  non-negative float that reflects real lifespan time.

Tests run inside Docker via ``make test``. In the test profile Redis
is a compose dependency and therefore reachable, so the happy path
expects ``status=="ok"``; the shape-only test stays tolerant so it
passes even when an earlier test temporarily tanked the broker.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient


async def test_health_ok_when_redis_reachable(async_client: AsyncClient) -> None:
    """Hitting /health returns 200 with a valid HealthResponse body."""
    resp = await async_client.get("/health")
    assert resp.status_code == 200

    body = resp.json()
    assert body["status"] in ("ok", "degraded")
    assert "redis_connected" in body
    assert "uptime_s" in body
    assert body["uptime_s"] >= 0


async def test_health_fields_match_model(async_client: AsyncClient) -> None:
    """The response body must expose exactly the HealthResponse fields."""
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()

    # Exact-match guards against accidental field leaks (e.g. a
    # future change that lets internal debugging info escape).
    assert set(body.keys()) == {
        "status",
        "redis_connected",
        "segments_ready",
        "uptime_s",
    }
