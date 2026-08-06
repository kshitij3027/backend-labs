"""``X-Served-By`` — the one header that says *which replica* produced a response.

C12 put two API replicas behind an nginx load balancer and proved the round-robin works by reading
``served_by`` out of the ``/health`` **body**. That is enough for a probe and not enough for
anything else: C13's distributed double-spend check fires a burst at a *metered* route and its
central assertion is ``len({X-Served-By across the burst}) >= 2`` — the guard that stops the whole
check passing trivially against a single replica, i.e. against the one configuration in which the
bug this project exists to catch cannot occur. A body field cannot carry that, because the
responses in the burst are 200s and 429s from routes whose bodies are pinned contracts.

Until this commit the name existed **only** in :data:`src.main.EXPOSE_HEADERS`, the CORS allowlist.
It was advertised to browsers as readable and emitted by nothing — a header that "exists" in
exactly the way no Python assertion notices and only ``response.headers.get(...)`` returning
``null`` in the dashboard's JavaScript reveals. This file is the assertion set that keeps it real.

.. rubric:: What is actually being pinned here, in one list

* It is on **every** terminal path, not merely the 2xx: the limiter's 429 short-circuit, the 401,
  both 503s, a handler that raised, a bug raised *inside* the limiter, and the exempt paths
  (``/health``, ``/docs``, ``/dashboard/api/stats``) that never reach the enforcement layer at all.
* Its value is :data:`src.api.health.SERVED_BY` — **one** notion of "which replica am I" in the
  process, not a second :func:`socket.gethostname` call that could drift from the first.
* It is never emitted twice.
* It is in :data:`src.main.EXPOSE_HEADERS`, so a browser is allowed to read it.
* Adding it changed **nothing** about the exempt path: still no ``X-RateLimit-*``, still no
  ``X-Quota-*``, still no identity lookup, still no decision script, still no Redis.

.. rubric:: Why the two 500 tests are the reason :class:`ServedByMiddleware` is a separate class

Both drive a response that :class:`~src.middleware.RateLimitMiddleware` cannot decorate however it
is written. ``ServerErrorMiddleware`` sits **outside** every middleware ``add_middleware``
registers, and it writes its 500 through the ``send`` *it* was handed — so a ``send`` wrapper
installed anywhere below it is not on that response's path. Those two tests fail against any
implementation that lives inside the user-middleware stack, which is precisely the design mistake
they exist to prevent.
"""

from __future__ import annotations

import dataclasses
import socket
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from src.api.health import SERVED_BY
from src.config import Settings
from src.main import EXPOSE_HEADERS, Runtime, create_app
from src.middleware import SERVED_BY_HEADER, ServedByMiddleware
from src.models import (
    CredentialKind,
    DenyReason,
    LimitDecision,
    Principal,
    QuotaPeriodState,
)
from src.redis_client import BackingStoreOverloaded, BackingStoreUnavailable

#: The lower-cased spelling every lookup below uses. ``httpx``/``TestClient`` header mappings are
#: case-insensitive, but the raw ASGI assertions are not — and the case is part of the contract.
HEADER = SERVED_BY_HEADER.lower()

#: Every rate/quota header a metered response can carry, used to assert **absence** on the exempt
#: paths. Deliberately the same tuple ``tests/unit/test_middleware.py`` uses: what this commit must
#: not have changed is exactly what that file already pins.
RATE_LIMIT_HEADER_PREFIXES = ("x-ratelimit-", "x-quota-", "retry-after")

PRINCIPAL = Principal(user_id="alice", credential=CredentialKind.API_KEY, key_id="demo-free")


# =============================================================================================
# Doubles
# =============================================================================================


def make_decision(**overrides: Any) -> LimitDecision:
    """A plausible allowed decision, with any field overridden.

    A local factory rather than an import from ``test_middleware.py``: a test module that reaches
    into another test module for its fixtures couples two suites that are about different things,
    and this one is about a header neither the limiter nor the decision knows exists.
    """
    base: dict[str, Any] = {
        "allowed": True,
        "reason": DenyReason.NONE,
        "tier": "free",
        "user_id": "alice",
        "endpoint": "GET:/api/v1/whoami",
        "cost": 1,
        "bucket_limit": 60,
        "bucket_remaining": 41,
        "bucket_reset_sec": 3,
        "window_limit": 60,
        "window_used": 19,
        "window_reset_sec": 37,
        "daily_limit": 1000,
        "daily_used": 120,
        "daily_reset_at": 1_786_752_000,
        "daily_state": QuotaPeriodState.ACTIVE,
        "monthly_limit": 25_000,
        "monthly_used": 4_200,
        "monthly_reset_at": 1_788_220_800,
        "monthly_state": QuotaPeriodState.ACTIVE,
        "retry_after_sec": 0,
        "degraded": False,
        "server_now_ms": 1_786_700_000_000,
        "latency_ms": 0.42,
    }
    base.update(overrides)
    return LimitDecision(**base)


def denied(reason: DenyReason = DenyReason.RATE_LIMIT, **overrides: Any) -> LimitDecision:
    """A refused decision, with a realistic non-zero ``retry_after_sec``."""
    base: dict[str, Any] = {
        "allowed": False,
        "reason": reason,
        "bucket_remaining": 0,
        "retry_after_sec": 4,
    }
    base.update(overrides)
    return make_decision(**base)


class StubIdentity:
    """Stands in for :class:`~src.identity.IdentityResolver`, counting what it was asked."""

    def __init__(self, principal: Principal | None = PRINCIPAL, *, error: Exception | None = None):
        self._principal = principal
        self._error = error
        self.calls = 0

    async def resolve(self, headers: Any) -> Principal | None:
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._principal


class StubLimiter:
    """Stands in for :class:`~src.limiter.Limiter`, returning a chosen decision.

    ``degraded`` is a real attribute rather than a leftover: ``/health`` and
    ``/dashboard/api/stats`` both read it off the limiter, and those two exempt paths are part of
    what this file asserts. A stub missing it would fail them for a reason that has nothing to do
    with the header under test.
    """

    def __init__(self, decision: LimitDecision | None = None) -> None:
        self._decision = decision if decision is not None else make_decision()
        self.degraded = False
        self.calls: list[tuple[str, str, int]] = []

    async def check(self, user_id: str, endpoint_label: str, cost: int) -> LimitDecision:
        self.calls.append((user_id, endpoint_label, cost))
        return self._decision


class StubApp:
    """A raw-ASGI downstream app, for driving :class:`ServedByMiddleware` on its own.

    ``omit_headers`` drives the case an ASGI ``http.response.start`` message is allowed to be in:
    no ``headers`` key at all. A middleware that assumed the key was there would ``TypeError`` on
    the first raw-ASGI sub-app anybody mounted.
    """

    def __init__(
        self,
        *,
        headers: list[tuple[bytes, bytes]] | None = None,
        omit_headers: bool = False,
    ) -> None:
        self.headers = headers if headers is not None else [(b"content-type", b"text/plain")]
        self.omit_headers = omit_headers
        self.calls = 0
        self.sends: list[Any] = []
        self.scopes: list[dict[str, Any]] = []

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        self.calls += 1
        self.scopes.append(scope)
        # Recorded so a test can assert whether the middleware handed the app the ORIGINAL `send`
        # or a wrapper — the only way to observe the non-HTTP pass-through from outside.
        self.sends.append(send)
        start: dict[str, Any] = {"type": "http.response.start", "status": 200}
        if not self.omit_headers:
            start["headers"] = self.headers
        await send(start)
        await send({"type": "http.response.body", "body": b"ok"})


async def drive(app: Any, scope: dict[str, Any]) -> list[dict[str, Any]]:
    """Run one request through ``app`` and return every raw ASGI message it wrote."""
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:  # pragma: no cover - nothing here reads a body
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await app(scope, receive, send)
    return messages


def raw_headers(messages: list[dict[str, Any]]) -> list[tuple[bytes, bytes]]:
    start = next(m for m in messages if m["type"] == "http.response.start")
    return list(start.get("headers", []))


def names(messages: list[dict[str, Any]]) -> list[str]:
    """Every header name in order, **with duplicates kept**, so a double-emit is visible."""
    return [name.decode("latin-1").lower() for name, _ in raw_headers(messages)]


def http_scope(path: str = "/api/v1/whoami") -> dict[str, Any]:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "client": ("127.0.0.1", 51234),
        "server": ("testserver", 80),
    }


# =============================================================================================
# App fixtures
#
# Two shapes, because the two halves of the contract need different things:
#
#   * `stub_app` keeps the REAL settings/tiers/redis/analytics (so the exempt handlers — `/health`
#     and `/dashboard/api/stats` — really run) and swaps only identity and the limiter, so every
#     terminal path can be produced deterministically with no Redis and no clock.
#   * `plain_app` is the untouched injected-runtime seam, for the paths that need nothing at all.
#
# `dataclasses.replace` off a real `Runtime.build`, and not a hand-listed `Runtime(...)`: the
# Runtime grows a collaborator every other commit, and a constructor call spelled out here would
# be a test helper failing to import rather than a test failing to pass. Same reasoning as
# `tests/unit/test_health.py::_client_with_gateway`.
# =============================================================================================


def build_app(settings: Settings, *, identity: Any = None, limiter: Any = None) -> FastAPI:
    runtime = dataclasses.replace(
        Runtime.build(settings),
        identity=identity if identity is not None else StubIdentity(),
        limiter=limiter if limiter is not None else StubLimiter(),
    )
    return create_app(runtime=runtime)


def mount(app: FastAPI, path: str, handler: Any) -> None:
    """Append a plain Starlette route after construction.

    Appended to ``app.router.routes`` directly rather than declared with ``@app.get`` so it is
    invisible to :func:`src.api.protected.verify_route_pricing` (which runs during ``create_app``)
    and cannot be mistaken for a route the project ships.
    """
    app.router.routes.append(Route(path, handler, methods=["GET"]))


# =============================================================================================
# 1. The name, the value, and the CORS contract
# =============================================================================================


def test_the_emitted_header_name_is_the_one_cors_exposes():
    """The anti-drift pin: the constant the middleware sends is the constant CORS advertises.

    ``tests/unit/test_health.py`` pins the exposed list against ten **literals**, which is the
    right shape for a contract — but it would pass just as happily if the emitter started sending
    ``X-Replica`` instead. This is the other half: the name in the allowlist is the name on the
    wire, asserted through the same constant both sides read.
    """
    assert SERVED_BY_HEADER == "X-Served-By"
    assert SERVED_BY_HEADER in EXPOSE_HEADERS


def test_the_value_is_the_single_notion_of_replica_identity(settings):
    """**One** answer to "which replica am I", asserted rather than assumed.

    ``src/api/health.py`` resolved the hostname once at import and publishes it as
    :data:`~src.api.health.SERVED_BY`; the ``/health`` body, the admin API, the dashboard payload
    and now this header all read that same constant. A second ``socket.gethostname()`` call would
    look identical for as long as nothing went wrong and would be invisible on the day it did.
    """
    client = TestClient(build_app(settings))

    response = client.get("/health")

    assert response.headers[HEADER] == SERVED_BY
    # ...the same string the body has carried since C12, and the same one the OS reports.
    assert response.json()["served_by"] == SERVED_BY
    assert SERVED_BY == socket.gethostname()


def test_the_value_is_resolved_once_at_import_not_per_request(settings, monkeypatch):
    """A hostname cannot change under a running process, so it is not read per request.

    Driven by breaking :func:`socket.gethostname` *after* import: if anything on the response path
    called it, this would raise (or start reporting the sabotage). It cannot, because the value was
    encoded to bytes once, at import, from the constant ``src.api.health`` had already resolved.
    """

    def exploded() -> str:  # pragma: no cover - the point is that nothing calls it
        raise AssertionError("gethostname() was called on the response path")

    monkeypatch.setattr(socket, "gethostname", exploded)
    client = TestClient(build_app(settings))

    for _ in range(3):
        assert client.get("/docs").headers[HEADER] == SERVED_BY


# =============================================================================================
# 2. The ASGI mechanics, against the middleware on its own
# =============================================================================================


@pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
async def test_non_http_scopes_pass_straight_through(scope_type):
    """The #1 bug in hand-written ASGI middleware, and it bites this one hardest of all.

    This callable wraps the ENTIRE stack, so it is the first thing uvicorn invokes with the
    ``lifespan`` scope at startup — a scope with no path, no method and no
    ``http.response.start`` to decorate. It must hand the app the original ``send`` untouched
    rather than a wrapper that could never fire, which is the assertion below: same object, not
    merely equivalent behaviour.
    """
    downstream = StubApp()
    middleware = ServedByMiddleware(downstream)
    scope: dict[str, Any] = {"type": scope_type}
    sent: list[Any] = []

    async def receive() -> dict[str, Any]:  # pragma: no cover - never awaited by the stub app
        return {"type": "lifespan.startup"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await middleware(scope, receive, send)

    assert downstream.calls == 1
    assert downstream.sends[0] is send
    assert downstream.scopes[0] is scope
    assert scope == {"type": scope_type}


async def test_the_header_is_appended_as_lower_case_bytes():
    """ASGI says header names are lower-case **bytes**; a ``str`` here is silently mangled.

    Not "usually mangled": the behaviour depends on which server is running, so a ``str`` name
    passes every in-process test and fails in production on one deployment out of two.
    """
    messages = await drive(ServedByMiddleware(StubApp()), http_scope())

    assert names(messages).count(HEADER) == 1
    for name, value in raw_headers(messages):
        assert isinstance(name, bytes)
        assert isinstance(value, bytes)
        assert name == name.lower()
    assert (HEADER.encode(), SERVED_BY.encode()) in raw_headers(messages)


async def test_a_response_start_without_headers_is_still_stamped():
    """``headers`` is optional in an ASGI response-start message. Absent must not mean skipped."""
    messages = await drive(ServedByMiddleware(StubApp(omit_headers=True)), http_scope())

    assert raw_headers(messages) == [(HEADER.encode(), SERVED_BY.encode())]


async def test_every_header_the_app_produced_survives():
    """The stamp decorates; it does not replace. Nothing the app wrote is touched or reordered."""
    app_headers = [(b"content-type", b"application/json"), (b"x-custom", b"kept")]
    messages = await drive(ServedByMiddleware(StubApp(headers=list(app_headers))), http_scope())

    assert raw_headers(messages)[: len(app_headers)] == app_headers
    assert names(messages)[-1] == HEADER


async def test_a_downstream_value_is_left_alone_and_never_duplicated():
    """**Two contradictory values for one header is worse than either value alone.**

    An HTTP client takes whichever its parser reaches first, so the answer would depend on library
    internals — the identical failure ``RateLimitMiddleware`` refuses to introduce on
    ``X-RateLimit-Remaining`` with ``MutableHeaders.update``, and the exact reason
    ``nginx/nginx.conf`` deliberately does **not** synthesise this header from ``$upstream_addr``.
    """
    downstream = StubApp(headers=[(HEADER.encode(), b"set-by-the-app")])

    messages = await drive(ServedByMiddleware(downstream), http_scope())

    assert names(messages).count(HEADER) == 1
    assert dict(raw_headers(messages))[HEADER.encode()] == b"set-by-the-app"


async def test_a_non_conformant_downstream_name_is_also_not_duplicated():
    """The gap a plain ``==`` comparison would leave, closed rather than documented.

    ASGI requires lower-case names and neither Starlette nor FastAPI can emit anything else, so
    this defends against a raw-ASGI sub-app someone mounts later — the same case
    ``RateLimitMiddleware`` closes for the limit headers by lower-casing before it writes.
    """
    downstream = StubApp(headers=[(b"X-Served-By", b"set-by-a-raw-subapp")])

    messages = await drive(ServedByMiddleware(downstream), http_scope())

    assert [name.lower() for name, _ in raw_headers(messages)].count(HEADER.encode()) == 1


async def test_the_apps_own_header_list_is_not_mutated_in_place():
    """A new list, never ``raw.append(...)``.

    The message may carry a tuple, or a list some other layer is still holding a reference to.
    Mutating a sequence handed over in passing is a bug that surfaces only in whichever component
    happened to keep it — which is nobody, until it is somebody.
    """
    app_headers = [(b"content-type", b"text/plain")]
    downstream = StubApp(headers=app_headers)

    messages = await drive(ServedByMiddleware(downstream), http_scope())

    assert app_headers == [(b"content-type", b"text/plain")]
    assert len(raw_headers(messages)) == 2


# =============================================================================================
# 3. Every terminal path through the assembled application
# =============================================================================================


def test_an_allowed_request_carries_it(settings):
    """The 200 — the only path a naive implementation gets right."""
    client = TestClient(build_app(settings))

    response = client.get("/api/v1/whoami", headers={"X-API-Key": "anything"})

    assert response.status_code == 200
    assert response.headers[HEADER] == SERVED_BY
    # ...alongside, not instead of, the limit headers the decision produced.
    assert response.headers["X-RateLimit-Limit"] == "60"


@pytest.mark.parametrize(
    "reason",
    [
        DenyReason.RATE_LIMIT,
        DenyReason.SLIDING_WINDOW,
        DenyReason.QUOTA_DAILY,
        DenyReason.QUOTA_MONTHLY,
    ],
)
def test_the_429_short_circuit_carries_it(settings, reason):
    """**The path C13 actually reads it from.**

    The limiter writes this response by hand, as raw ASGI messages, and never invokes the wrapped
    app — so nothing downstream had an opportunity to add a header. A burst that trips the limiter
    is exactly the burst C13 asserts two distinct replicas across, and it is all 429s.
    """
    client = TestClient(build_app(settings, limiter=StubLimiter(denied(reason))))

    response = client.get("/api/v1/whoami", headers={"X-API-Key": "anything"})

    assert response.status_code == 429
    assert response.headers[HEADER] == SERVED_BY


def test_the_401_carries_it(settings):
    """Refused before a bucket was ever consulted — and still attributable to a replica."""
    client = TestClient(build_app(settings, identity=StubIdentity(None)))

    response = client.get("/api/v1/whoami")

    assert response.status_code == 401
    assert response.headers[HEADER] == SERVED_BY
    assert response.headers["WWW-Authenticate"]


@pytest.mark.parametrize(
    "error",
    [
        BackingStoreUnavailable("redis is down", op="identity:apikey"),
        BackingStoreOverloaded("no connection available", op="identity:apikey"),
    ],
    ids=["store-outage", "pool-overload"],
)
def test_the_identity_503_carries_it(settings, error):
    """The fail-closed identity refusal, in both of its causes.

    This is the 5xx an operator is most likely to be triaging when they ask "is it one replica or
    both?" — a credential store outage looks identical to a single replica with a saturated pool
    until you can attribute the responses.
    """
    client = TestClient(build_app(settings, identity=StubIdentity(error=error)))

    response = client.get("/api/v1/whoami", headers={"X-API-Key": "anything"})

    assert response.status_code == 503
    assert response.headers[HEADER] == SERVED_BY


@pytest.mark.parametrize("degraded", [True, False], ids=["fail-closed", "pool-overload"])
def test_the_limiter_503_carries_it(settings, degraded):
    """``DenyReason.BACKING_STORE`` — the refusal that is not a 429, in both of its flavours."""
    decision = denied(reason=DenyReason.BACKING_STORE, retry_after_sec=5, degraded=degraded)
    client = TestClient(build_app(settings, limiter=StubLimiter(decision)))

    response = client.get("/api/v1/whoami", headers={"X-API-Key": "anything"})

    assert response.status_code == 503
    assert response.headers[HEADER] == SERVED_BY


def test_a_handler_that_raised_carries_it(settings):
    """**The test that decides where this middleware lives.**

    A handler exception does not come back as a status code: it propagates past every middleware
    ``add_middleware`` registered, to ``ServerErrorMiddleware``, which writes the 500 through the
    ``send`` *it* was handed and then re-raises. A ``send`` wrapper installed anywhere inside the
    user-middleware stack — including inside ``RateLimitMiddleware``, however carefully — is
    simply not on this response's path.

    So this assertion fails against every implementation except one registered outside the whole
    stack, which is what :class:`src.main._ServedByApp` exists to do. A 500 with no replica on it
    is the single most useless 500 a two-replica deployment can produce.
    """
    app = build_app(settings)

    async def boom(request: Any) -> PlainTextResponse:
        raise RuntimeError("the handler blew up")

    mount(app, "/boom", boom)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/boom", headers={"X-API-Key": "anything"})

    assert response.status_code == 500
    assert response.headers[HEADER] == SERVED_BY


def test_a_bug_raised_inside_the_limiter_itself_carries_it(settings):
    """The same 500, raised *above* the router rather than inside it.

    ``_runtime_of`` raises a ``RuntimeError`` naming the wiring bug when ``app.state.runtime`` is
    missing — deliberately, because every alternative would dress a bug up as a policy. It is
    still a 500 written by ``ServerErrorMiddleware``, and it is still the moment somebody needs to
    know which replica was misconfigured.
    """
    app = build_app(settings)
    del app.state.runtime
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/api/v1/whoami", headers={"X-API-Key": "anything"})

    assert response.status_code == 500
    assert response.headers[HEADER] == SERVED_BY


def test_a_router_404_carries_it(settings):
    """An unrouted path is metered above the router, so the 404 is a decorated app response."""
    client = TestClient(build_app(settings))

    response = client.get("/definitely/not/a/route", headers={"X-API-Key": "anything"})

    assert response.status_code == 404
    assert response.headers[HEADER] == SERVED_BY


# =============================================================================================
# 4. The exempt paths — the branch that never reaches the enforcement layer
# =============================================================================================

#: Every exempt shape that answers with a body of its own. ``/dashboard/api/stats`` is the one C15
#: polls and ``/health`` is the one C13 probes through the load balancer, so both matter for real
#: rather than as list padding.
EXEMPT_PATHS = [
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/dashboard/api/stats",
]


@pytest.mark.parametrize("path", EXEMPT_PATHS)
def test_exempt_paths_carry_it_too(settings, path):
    """``/health`` is the path C13 probes through the load balancer, so this is load-bearing.

    The exempt branch short-circuits before the limiter installs its ``send`` wrapper, which is
    exactly why folding this header into ``RateLimitMiddleware`` would have needed a second
    emission site on that branch. Wrapping the stack from outside means there is no branch here at
    all: the response is stamped because every response is.
    """
    client = TestClient(build_app(settings))

    response = client.get(path)

    assert response.status_code == 200
    assert response.headers[HEADER] == SERVED_BY


@pytest.mark.parametrize("path", EXEMPT_PATHS)
def test_exempt_paths_still_advertise_no_limit_they_never_evaluated(settings, path):
    """**The property this commit must not have changed**, re-asserted rather than assumed.

    No bucket was consulted on these paths, so any ``X-RateLimit-*`` or ``X-Quota-*`` number would
    be fabricated — and a client can detect a missing header while it cannot detect a wrong one.
    The stub identity and limiter are the proof that the enforcement layer was never entered at
    all: not a header, not a lookup, not a decision.
    """
    identity, limiter = StubIdentity(), StubLimiter()
    client = TestClient(build_app(settings, identity=identity, limiter=limiter))

    response = client.get(path)

    assert response.status_code == 200
    assert not [
        name for name in response.headers if name.lower().startswith(RATE_LIMIT_HEADER_PREFIXES)
    ]
    assert identity.calls == 0
    assert limiter.calls == []


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
def test_a_documentation_path_touches_no_redis_at_all(settings, path):
    """"Unmetered" spelled as a number rather than as an absence of headers.

    ``RedisGateway.calls`` counts every operation this process attempts against the store —
    :meth:`~src.redis_client.RedisGateway.run` is the single door, and ``run_script`` and ``ping``
    both go through it. These three paths are the ones with no handler-side store access either
    (``/health`` pings by design and ``/dashboard/api/stats`` reads counters), so the whole request
    is provably Redis-free end to end, exactly as it was before this header existed.
    """
    runtime = dataclasses.replace(
        Runtime.build(settings), identity=StubIdentity(), limiter=StubLimiter()
    )
    client = TestClient(create_app(runtime=runtime))

    response = client.get(path)

    assert response.status_code == 200
    assert response.headers[HEADER] == SERVED_BY
    assert runtime.redis.calls == 0


def test_the_admin_surface_is_exempt_from_metering_and_still_stamped(settings):
    """Exempt from *metering* is not exempt from *authentication* — and both answers are attributed.

    The control plane's 401 is written by a router dependency, below everything, on a path the
    limiter never touches. It is the one shape that is simultaneously exempt and refused, so it
    would fall through a per-branch implementation twice over.
    """
    identity, limiter = StubIdentity(), StubLimiter()
    client = TestClient(build_app(settings, identity=identity, limiter=limiter))

    response = client.get("/api/v1/admin/tiers")

    assert response.status_code == 401
    assert response.headers[HEADER] == SERVED_BY
    assert identity.calls == 0
    assert limiter.calls == []


# =============================================================================================
# 5. Duplication and CORS, through the real stack
# =============================================================================================


def test_a_handler_that_sets_it_is_not_given_a_second_value(settings):
    """The duplicate guard where it would actually happen: a real route setting the header itself."""
    app = build_app(settings)

    async def opinionated(request: Any) -> PlainTextResponse:
        return PlainTextResponse("ok", headers={SERVED_BY_HEADER: "handler-said-so"})

    mount(app, "/opinionated", opinionated)
    client = TestClient(app)

    response = client.get("/opinionated", headers={"X-API-Key": "anything"})

    assert response.status_code == 200
    # `httpx` joins repeated headers with ", ", so a duplicate is visible in the value itself.
    assert response.headers[HEADER] == "handler-said-so"
    assert response.headers.get_list(HEADER) == ["handler-said-so"]


def test_it_survives_the_cors_wrapper_and_is_exposed_to_browser_javascript(settings):
    """Sent **and** readable, which are two different facts and only one of them is free.

    ``ServedByMiddleware`` sits outside ``CORSMiddleware``, so its stamp is applied after CORS has
    finished with the message — safe, because CORS only ever adds headers on the way out. What
    being outside CORS does *not* buy is browser access: a cross-origin ``fetch`` may read a
    non-safelisted response header only if ``Access-Control-Expose-Headers`` names it. That list is
    emitted by CORS from :data:`src.main.EXPOSE_HEADERS`, which is why both halves are asserted on
    one response here.
    """
    client = TestClient(build_app(settings))

    response = client.get("/health", headers={"Origin": "http://localhost:5173"})

    exposed = {
        name.strip().lower()
        for name in response.headers["access-control-expose-headers"].split(",")
    }
    assert HEADER in exposed
    assert response.headers["access-control-allow-origin"] == "*"
    assert response.headers[HEADER] == SERVED_BY


def test_a_preflight_answered_by_cors_carries_it(settings):
    """A response the app's router never sees at all — CORS short-circuits it above everything.

    Included because it is the clearest demonstration of what "outside the whole stack" buys: this
    response is produced by a middleware, for a request the limiter is never given, and it is
    still attributable to the replica that produced it.
    """
    client = TestClient(build_app(settings))

    response = client.options(
        "/api/v1/whoami",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-api-key",
        },
    )

    assert response.status_code == 200
    assert response.headers[HEADER] == SERVED_BY
