"""Unit tests for ``GET /health``.

The body shape is a **spec requirement**, not an implementation detail: §2 requires
``{"status": "healthy"}`` and §8 repeats it as the only literal payload in the document. So these
tests assert the exact object and the exact key set — an extra field would still satisfy a
``response.json()["status"] == "healthy"`` check while breaking the contract, and the container
HEALTHCHECK plus compose's ``condition: service_healthy`` both hang off this route.

They also pin the two properties that make it usable as a liveness probe: it is reachable with no
authentication or setup at all, and it takes no dependency that could fail.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from src.api.health import HEALTHY_STATUS, health


def test_health_returns_exactly_the_specified_body(client: TestClient) -> None:
    """The payload is the literal object the spec pins — no more keys, no fewer."""
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_health_body_has_no_extra_keys(client: TestClient) -> None:
    """Guards the shape against additive drift, which an equality check on one key would miss."""
    payload = client.get("/health").json()

    assert set(payload) == {"status"}, (
        f"the /health body must be exactly {{'status': 'healthy'}}; got keys {sorted(payload)}. "
        "Adding a field here is a contract break — the spec pins this payload twice."
    )


def test_health_content_type_is_json(client: TestClient) -> None:
    """A probe that returned ``text/plain`` would still be 200 and still be wrong."""
    response = client.get("/health")
    assert response.headers["content-type"].startswith("application/json")


def test_health_status_constant_is_the_served_value() -> None:
    """The module constant and the wire value are the same string, checked without HTTP."""
    assert HEALTHY_STATUS == "healthy"
    assert health().status == HEALTHY_STATUS


def test_health_handler_takes_no_dependencies() -> None:
    """The handler has no parameters at all — so there is nothing it can fail to resolve.

    This is the structural form of "dependency-free": not "it happens not to query Postgres
    today", but "it accepts no request, no session and no app state, so a later commit cannot
    quietly give it something to depend on without this test noticing".
    """
    assert health.__code__.co_argcount == 0


def test_health_is_registered_at_exactly_slash_health(app: FastAPI) -> None:
    """The probe path is unversioned and exact — the HEALTHCHECK URL is hard-coded to it.

    A probe that moves with an API version is a probe that breaks a rollout, so ``/health`` must
    not acquire a prefix. Asserting on the registered route (rather than on a 404 for
    ``/api/v1/health``) states the requirement positively.
    """
    paths = {route.path for route in app.routes if isinstance(route, APIRoute)}

    assert "/health" in paths
    assert not any(path.endswith("/health") and path != "/health" for path in paths), (
        f"/health must not be nested under a prefix; found {sorted(paths)}"
    )


def test_health_allows_get_only(client: TestClient) -> None:
    """A liveness probe is a read. POSTing to it is a client error, not a 200."""
    assert client.post("/health").status_code == 405
