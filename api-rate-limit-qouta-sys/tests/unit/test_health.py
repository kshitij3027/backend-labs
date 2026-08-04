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

from src.api.health import (
    RATE_LIMITER_ACTIVE,
    REDIS_OK,
    REDIS_UNREACHABLE,
    SERVED_BY,
    STATUS_HEALTHY,
)
from src.main import API_VERSION, Runtime, create_app
from src.redis_client import BackingStoreUnavailable

#: The complete body. Asserted as a set so an accidentally added field is caught here rather than
#: by a dashboard that silently starts rendering something nobody meant to publish.
EXPECTED_KEYS = {
    "status",
    "rate_limiter",
    "version",
    "uptime_sec",
    "served_by",
    "redis",
    "config_version",
}


class StubGateway:
    """A gateway stand-in that answers ``ping()`` however the test needs it to.

    The three Redis outcomes ``/health`` has to distinguish — answered, answered falsy, refused —
    are not all reachable against a real server (a healthy Redis never returns a falsy PONG), so
    the probe's branches are pinned here and the real-server behaviour is covered in
    ``tests/integration/test_redis_gateway.py``.
    """

    def __init__(self, *, answer: object = True, error: Exception | None = None) -> None:
        self._answer = answer
        self._error = error
        self.pings = 0

    async def ping(self) -> object:
        self.pings += 1
        if self._error is not None:
            raise self._error
        return self._answer


def _client_with_gateway(settings, gateway: object) -> TestClient:
    """An app whose Runtime carries ``gateway`` instead of a real one.

    Built with ``dataclasses.replace`` off a real ``Runtime.build`` rather than by calling
    ``Runtime(...)`` field by field: the Runtime grows a collaborator every other commit (C3's tier
    registry, C4's limiter, C9's analytics), and a hand-listed constructor call here would have to
    be updated each time — which is a test helper failing to compile, not a test failing to pass.
    Everything except the gateway stays exactly as production builds it.
    """
    runtime = dataclasses.replace(Runtime.build(settings), redis=gateway)
    return TestClient(create_app(runtime=runtime))


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
    # The injected-runtime seam never starts the runtime, so the tier registry has never read
    # `config:version` and is still serving `settings.tier_limits`. Reported as 0 — an honest
    # "no snapshot from Redis yet", which is what C10 needs this field to mean.
    assert body["config_version"] == 0


def test_health_body_has_exactly_the_documented_keys(client):
    assert set(client.get("/health").json()) == EXPECTED_KEYS


def test_health_reports_redis_ok_when_the_gateway_answers(settings):
    stub = StubGateway(answer=True)

    body = _client_with_gateway(settings, stub).get("/health").json()

    assert body["redis"] == REDIS_OK
    assert stub.pings == 1


@pytest.mark.parametrize(
    "stub",
    [
        StubGateway(answer=False),
        StubGateway(answer=None),
        StubGateway(error=BackingStoreUnavailable("redis is down", op="ping")),
    ],
    ids=["falsy-pong", "no-pong", "classified-failure"],
)
def test_health_reports_redis_unreachable_on_anything_but_a_pong(settings, stub):
    """A refusal and a non-answer are the same fact to an operator: the store is not reachable."""
    body = _client_with_gateway(settings, stub).get("/health").json()

    assert body["redis"] == REDIS_UNREACHABLE


def test_a_redis_outage_does_not_turn_the_probe_red(settings):
    """**The decision, asserted at the unit level too.**

    `status` stays "healthy", `rate_limiter` stays "active", the HTTP status stays 200. `/health`
    is what the container HEALTHCHECK, compose's `condition: service_healthy` and (C12) nginx's
    upstream check read, and every one of them treats a non-200 as "restart this replica". A
    replica that cannot reach Redis is still alive and — per C8's `FAIL_MODE=open` — still serving
    correctly through the bounded local fallback, so failing the probe would restart a healthy
    process. Worse, it would restart every replica simultaneously, because they all share the one
    Redis that is down: a dependency failure the system was built to survive would become a total
    outage. Liveness and dependency health are therefore two separate fields.
    """
    response = _client_with_gateway(
        settings, StubGateway(error=BackingStoreUnavailable("down", op="ping"))
    ).get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["redis"] == REDIS_UNREACHABLE
    assert body["status"] == STATUS_HEALTHY
    assert body["rate_limiter"] == RATE_LIMITER_ACTIVE


def test_an_unstarted_runtime_reports_redis_unreachable(client):
    """The default test seam injects a Runtime and never starts it, so the gateway is unconnected.

    That must read as "unreachable" — never as an AttributeError, and never as a fabricated "ok".
    """
    assert client.get("/health").json()["redis"] == REDIS_UNREACHABLE


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
    """It must not drift under /api/v1 — a probe that moves with the API version breaks rollouts.

    `/api/v1/health` answered 404 until C6 and answers **401** from C6 on, which is a deliberate
    consequence of the limiter middleware rather than a regression: it runs above the router, so a
    request to a path that does not exist is classified and authenticated *before* anything asks
    whether a handler exists. Routing first would make the 404-vs-401 difference a free
    path-enumeration oracle for a caller holding no credential at all.

    What this test is actually about is unchanged and still asserted: `/health` is served here and
    is not served there.
    """
    assert client.get("/health").status_code == 200
    assert client.get("/api/v1/health").status_code == 401


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
    # No runtime means no gateway to probe and no tier registry to read — both reported honestly
    # rather than guessed at.
    assert body["redis"] == REDIS_UNREACHABLE
    assert body["config_version"] == 0


def test_health_through_the_production_lifespan_path():
    """Drive an app built the way uvicorn builds it — no injected Runtime, lifespan and all.

    Every other test here uses the hermetic ``create_app(runtime=...)`` seam, which skips the
    lifespan entirely. That is the right default, but it would leave the startup path that
    actually ships as the one path nothing exercises: the Runtime construction, the settings read,
    and the Redis gateway's open/close. Entering the TestClient as a context manager is what runs
    startup and shutdown.

    Note what is NOT asserted: the value of ``redis``. This is a unit test, and whether a Redis
    happens to be reachable from wherever it runs is not its subject —
    ``tests/integration/test_redis_gateway.py`` asserts ``"ok"`` against a real server. What IS
    asserted is that the lifespan started the gateway and, on the way out, stopped it: a shutdown
    that leaks the pool turns a restart loop into a connection leak against a server whose
    connection count is a finite, shared resource.
    """
    app = create_app()

    with TestClient(app) as lifespan_client:
        response = lifespan_client.get("/health")
        assert app.state.runtime.redis.is_connected is True

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["rate_limiter"] == "active"
    assert body["version"] == API_VERSION
    assert body["redis"] in {"ok", "unreachable"}
    # A runtime built by the lifespan, not injected by a fixture.
    assert isinstance(app.state.runtime, Runtime)
    # ...and torn down by it.
    assert app.state.runtime.redis.is_connected is False


def test_rate_limit_headers_are_readable_cross_origin(client):
    """Every header this API uses to report limit state must be exposed to browser JavaScript.

    ``EXPOSE_HEADERS`` is data, not a branch, so coverage says nothing about it — the list counts
    as "covered" the moment the app is constructed. It needs a direct assertion precisely because
    of how it fails: a header dropped from the list is still sent on the wire and still received by
    the browser, and only ``response.headers.get(...)`` in the dashboard's JavaScript comes back
    null. No Python anywhere notices, which is what makes this untestable by every other means.

    The ten names are written out literally rather than imported from ``src.main``: asserting the
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
        # Added at C6, when RateLimitMiddleware became the first thing to emit it. It is not
        # CORS-safelisted, so without it a browser client that got a 401 could read the status
        # and not the challenge — i.e. could not discover that this API accepts an `ApiKey`
        # scheme at all. Changing this set is meant to take a deliberate edit; this is one.
        "www-authenticate",
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
