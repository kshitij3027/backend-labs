"""Unit tests for GET /health — the unversioned, dependency-free liveness contract.

The probe is consumed by the container HEALTHCHECK, by compose's ``condition: service_healthy``,
by nginx's upstream check (C12) and by the E2E verifier, so three properties here are
non-negotiable: it carries the spec's two keys **verbatim**, it returns 200 while the process is
alive whatever the runtime looks like, and it names the replica that answered.

The app-wiring assertions at the bottom (``Runtime``, and the CORS ``expose_headers`` contract)
live here because ``/health`` is the only route mounted at C1 and therefore the only surface those
properties can be observed through.
"""

import dataclasses
import socket

import pytest
from fastapi.testclient import TestClient

from src.api.health import RATE_LIMITER_ACTIVE, SERVED_BY, STATUS_HEALTHY
from src.main import API_VERSION, Runtime, create_app

#: The complete body. Asserted as a set so an accidentally added field is caught here rather than
#: by a dashboard that silently starts rendering something nobody meant to publish.
EXPECTED_KEYS = {"status", "rate_limiter", "version", "uptime_sec", "served_by"}


def test_health_returns_the_spec_contract(client):
    """The spec names two keys and two values; they are asserted character-for-character."""
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()

    # The spec's literal contract: {"status": "healthy", "rate_limiter": "active"}. Top level,
    # not nested, not renamed.
    assert body["status"] == "healthy"
    assert body["rate_limiter"] == "active"

    # And the constants the rest of the project reads must be those same two strings.
    assert STATUS_HEALTHY == "healthy"
    assert RATE_LIMITER_ACTIVE == "active"


def test_health_reports_version_uptime_and_replica(client):
    """The additive fields: what is running, for how long, and which replica answered."""
    body = client.get("/health").json()

    assert body["version"] == API_VERSION
    assert body["uptime_sec"] >= 0.0
    assert body["served_by"]
    assert isinstance(body["served_by"], str)


def test_health_body_has_exactly_the_documented_keys(client):
    assert set(client.get("/health").json()) == EXPECTED_KEYS


def test_served_by_is_this_containers_hostname(client):
    """C12 proves two replicas answered by comparing this field across responses.

    Under compose the hostname is the container id, so two replicas are distinguishable without
    any coordination between them — which is what lets the E2E verifier assert
    ``len({X-Served-By}) >= 2`` and know the burst really was spread across processes.
    """
    assert SERVED_BY == socket.gethostname()
    assert client.get("/health").json()["served_by"] == socket.gethostname()


def test_health_needs_no_authentication(client):
    """A liveness probe that can return 401 or 429 is not a liveness probe.

    The container HEALTHCHECK carries no credentials at all, and a bogus one must not change the
    answer either — the route is exempt from the whole identity/limiter path, not merely lenient
    about it.
    """
    assert client.get("/health").status_code == 200
    assert client.get("/health", headers={"X-API-Key": "totally-bogus"}).status_code == 200
    assert (
        client.get("/health", headers={"Authorization": "Bearer not-a-jwt"}).status_code == 200
    )


def test_health_is_unversioned(client):
    """It must not drift under /api/v1 — a probe that moves with the API version breaks rollouts."""
    assert client.get("/health").status_code == 200
    assert client.get("/api/v1/health").status_code == 404


def test_health_survives_a_missing_runtime(settings):
    """No runtime on app.state must degrade to zeroed vitals — never a 500.

    Simulates a half-wired or pre-startup process. The route reads everything through
    ``getattr(..., default)`` precisely so this case stays a 200: a liveness probe that 500s when
    the app is degraded reports the opposite of the truth to the orchestrator, which then restarts
    a process that was answering fine.
    """
    app = create_app(runtime=Runtime.build(settings))
    del app.state.runtime
    assert getattr(app.state, "runtime", None) is None

    response = TestClient(app).get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["rate_limiter"] == "active"
    assert body["uptime_sec"] == 0.0
    assert body["served_by"]


def test_health_through_the_production_lifespan_path():
    """Drive an app built the way uvicorn builds it — no injected Runtime, lifespan and all.

    Every other test here uses the hermetic ``create_app(runtime=...)`` seam, which skips the
    lifespan entirely. That is the right default, but it would leave the startup path that
    actually ships as the one path nothing exercises: the Runtime construction, the settings read,
    and (from C2) the Redis pool open/close. Entering the TestClient as a context manager is what
    runs startup and shutdown.
    """
    app = create_app()

    with TestClient(app) as lifespan_client:
        response = lifespan_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["rate_limiter"] == "active"
    assert body["version"] == API_VERSION
    # A runtime built by the lifespan, not injected by a fixture.
    assert isinstance(app.state.runtime, Runtime)


def test_rate_limit_headers_are_readable_cross_origin(client):
    """Every header this API uses to report limit state must be exposed to browser JavaScript.

    ``EXPOSE_HEADERS`` is data, not a branch, so coverage says nothing about it — the list counts
    as "covered" the moment the app is constructed. It needs a direct assertion precisely because
    of how it fails: a header dropped from the list is still sent on the wire and still received by
    the browser, and only ``response.headers.get(...)`` in the dashboard's JavaScript comes back
    null. No Python anywhere notices, which is what makes this untestable by every other means.

    The nine names are written out literally rather than imported from ``src.main``: asserting the
    response against the same list that produced it would pass just as happily if a header were
    deleted from both sides at once. This is the contract; ``EXPOSE_HEADERS`` implements it.

    CORS emits ``access-control-expose-headers`` only for a request that carries an ``Origin``, so
    one is sent here rather than assumed.
    """
    response = client.get("/health", headers={"Origin": "http://localhost:5173"})

    assert response.status_code == 200
    exposed = {
        name.strip().lower()
        for name in response.headers["access-control-expose-headers"].split(",")
    }

    assert exposed == {
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "x-ratelimit-reset",
        "x-quota-limit",
        "x-quota-remaining",
        "x-quota-reset",
        "retry-after",
        "x-ratelimit-degraded",
        "x-served-by",
    }


def test_runtime_uptime_is_monotonic_and_non_negative(settings):
    """Uptime is measured from time.monotonic(), so an NTP step cannot make it go backwards."""
    runtime = Runtime.build(settings)

    first = runtime.uptime_sec
    second = runtime.uptime_sec

    assert first >= 0.0
    assert second >= first
    assert runtime.settings is settings


def test_runtime_is_frozen(settings):
    """A request must never be able to rebind which collaborators the next request uses."""
    runtime = Runtime.build(settings)

    with pytest.raises(dataclasses.FrozenInstanceError):
        runtime.settings = settings  # type: ignore[misc]
