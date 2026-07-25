"""Integration tests for ``GET /health`` through the real application construction path.

``real_client`` builds the app with :func:`src.main.create_app` and **no injected settings** —
configuration comes from the environment exactly as it does in the container — and drives it as a
context manager, so Starlette runs the lifespan around every request here. That is the difference
that matters: the unit suite proves the handler returns the right object, and this suite proves
the process that actually ships can start, serve it, and shut down again.

It is also the check that stands between a green build and a container that restart-loops. The
image's ``HEALTHCHECK`` curls this exact URL and compose's ``condition: service_healthy`` gates
``api`` — and the ``e2e``/``loadtest`` services in turn — on the result. A lifespan that raised on
startup would leave every one of those hanging, and this is the cheapest place to catch it.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config import Settings


def test_health_through_the_real_app(real_client: TestClient) -> None:
    """200 with the exact spec body, served by an app that has completed startup."""
    response = real_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}
    assert set(response.json()) == {"status"}


def test_lifespan_startup_publishes_settings_on_app_state(
    real_app: FastAPI, real_client: TestClient
) -> None:
    """Startup completed and left resolved configuration reachable from the request.

    ``app.state`` is where C2 hangs the SQLAlchemy engine and C6 the Redis client and broker, so
    proving the attachment seam works now means those commits are wiring into a path that is
    already covered rather than into one that is not. ``real_client`` is requested (not merely
    for its return value) because entering it is what runs the lifespan — without it this would
    only be asserting on what :func:`~src.main.create_app` set, not on what startup left behind.
    """
    assert real_client.get("/health").status_code == 200
    settings = real_app.state.settings

    assert isinstance(settings, Settings)
    assert settings.default_query_limit <= settings.max_query_limit


def test_health_survives_repeated_probes(real_client: TestClient) -> None:
    """Docker curls this every 10s for the life of the container; it must be stateless.

    Ten identical responses, byte for byte. A probe whose body drifted between calls (a counter,
    an uptime) would break the exact-shape contract on some later call rather than the first.
    """
    bodies = [real_client.get("/health").json() for _ in range(10)]

    assert bodies == [{"status": "healthy"}] * 10


def test_health_response_schema_is_published_with_one_property(real_client: TestClient) -> None:
    """OpenAPI documents the same single-field shape the route serves.

    ``response_model=HealthResponse`` is what strips any extra key a future handler returns, so
    the generated schema having exactly one property is the machine-readable half of the same
    guarantee the unit test asserts on the wire.
    """
    schema = real_client.get("/openapi.json").json()

    assert "/health" in schema["paths"]
    properties = schema["components"]["schemas"]["HealthResponse"]["properties"]
    assert set(properties) == {"status"}
