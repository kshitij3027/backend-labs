"""Unit tests for the pure-ASGI :class:`~src.middleware.RateLimitMiddleware`.

These drive the middleware **as an ASGI callable** — a hand-built ``scope``, a ``receive`` that is
never called, and a ``send`` that records raw messages — rather than through a FastAPI app. That is
deliberate and it is what makes these unit tests rather than slow integration tests:

* The properties under test are ASGI-level facts. "The wrapped app is never invoked on a 429", "the
  header names written into ``http.response.start`` are lower-case bytes", "``lifespan`` passes
  through untouched" — none of them are observable through an HTTP client, which sees only a
  decoded response and cannot tell a short-circuit from a 429 the router produced.
* The three scope types the guard exists for (``lifespan``, ``websocket``, ``http``) are not all
  reachable through ``TestClient`` without standing up a websocket route.
* A stub limiter can return a *chosen* decision in microseconds, so every branch of the denial path
  is exercised without a Redis, a tier table or a clock.

The real wiring — CORS ordering, headers landing on a real response, a real Redis decrementing a
real bucket — is asserted in ``tests/integration/test_middleware_flow.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import pytest
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from src.config import Settings
from src.identity import WWW_AUTHENTICATE
from src.keys import classify
from src.main import EXPOSE_HEADERS, Runtime, create_app
from src.middleware import (
    EXEMPT_EXACT_PATHS,
    EXEMPT_PATH_PREFIXES,
    JSON_CONTENT_TYPE,
    SCOPE_DECISION_KEY,
    SCOPE_ENDPOINT_KEY,
    SERVICE_UNAVAILABLE_ERROR,
    UNAUTHORIZED_ERROR,
    WWW_AUTHENTICATE_HEADER,
    RateLimitMiddleware,
    is_exempt,
)
from src.models import (
    ERROR_QUOTA,
    ERROR_RATE_LIMIT,
    CredentialKind,
    DenyReason,
    LimitDecision,
    Principal,
    QuotaPeriodState,
)
from src.redis_client import BackingStoreOverloaded, BackingStoreUnavailable

#: Every rate/quota header a metered response can carry. Used to assert **absence** on the paths
#: that must not advertise a limit they never evaluated.
RATE_LIMIT_HEADER_PREFIXES = ("x-ratelimit-", "x-quota-", "retry-after")


# =============================================================================================
# Doubles
# =============================================================================================


def make_decision(**overrides: Any) -> LimitDecision:
    """Build a plausible allowed decision, with any field overridden.

    A local factory rather than an import from ``test_models.py``: these tests care about three or
    four fields of a 24-field record, and a test module that reaches into another test module for
    its fixtures couples two suites that are about different things.
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
    """A refused decision. ``retry_after_sec`` defaults to a realistic non-zero value."""
    base: dict[str, Any] = {
        "allowed": False,
        "reason": reason,
        "bucket_remaining": 0,
        "window_used": 60,
        "retry_after_sec": 4,
    }
    base.update(overrides)
    return make_decision(**base)


class StubApp:
    """The wrapped downstream app: counts its invocations and replies with a fixed response.

    ``calls`` is what the "a 429 costs zero downstream work" test asserts on, and it is the only
    way to observe that property — from outside, a short-circuited 429 and a 429 produced by a
    handler are the same bytes.
    """

    def __init__(
        self,
        *,
        status: int = 200,
        headers: list[tuple[bytes, bytes]] | None = None,
        body: bytes = b'{"ok":true}',
        omit_headers: bool = False,
    ) -> None:
        self.status = status
        self.headers = headers if headers is not None else [(b"content-type", b"text/plain")]
        self.body = body
        # A raw-ASGI app is allowed to omit `headers` from `http.response.start` entirely; the
        # middleware must cope rather than AttributeError. This flag drives that case.
        self.omit_headers = omit_headers

        self.calls = 0
        self.scopes: list[dict[str, Any]] = []

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        self.calls += 1
        self.scopes.append(scope)
        start: dict[str, Any] = {"type": "http.response.start", "status": self.status}
        if not self.omit_headers:
            start["headers"] = list(self.headers)
        await send(start)
        await send({"type": "http.response.body", "body": self.body})


class StubIdentity:
    """Stands in for :class:`~src.identity.IdentityResolver`.

    ``resolver`` maps the raw ASGI header list onto a principal, so a test can resolve a *different*
    principal per request — which is what the concurrency test needs.
    """

    def __init__(
        self,
        principal: Principal | None = None,
        *,
        error: Exception | None = None,
        resolver: Any = None,
        gate: Rendezvous | None = None,
    ) -> None:
        self._principal = principal
        self._error = error
        self._resolver = resolver
        self._gate = gate
        self.calls = 0

    async def resolve(self, headers: Any) -> Principal | None:
        self.calls += 1
        if self._error is not None:
            raise self._error
        if self._gate is not None:
            await self._gate.arrive()
        if self._resolver is not None:
            return self._resolver(headers)
        return self._principal


class StubLimiter:
    """Stands in for :class:`~src.limiter.Limiter`, recording exactly what it was asked."""

    def __init__(
        self,
        decision: LimitDecision | None = None,
        *,
        error: Exception | None = None,
        factory: Any = None,
        gate: Rendezvous | None = None,
    ) -> None:
        self._decision = decision
        self._error = error
        self._factory = factory
        self._gate = gate
        self.calls: list[tuple[str, str, int]] = []

    async def check(self, user_id: str, endpoint_label: str, cost: int) -> LimitDecision:
        self.calls.append((user_id, endpoint_label, cost))
        if self._error is not None:
            raise self._error
        if self._gate is not None:
            await self._gate.arrive()
        if self._factory is not None:
            return self._factory(user_id)
        assert self._decision is not None
        return self._decision


class Rendezvous:
    """Holds every arriving coroutine until ``expected`` of them have arrived, then releases all.

    This is the instrument the state-leak test is built on. Random sleeps would interleave the
    requests *sometimes*; this interleaves them *always*, and maximally: every request is
    guaranteed to be suspended inside ``RateLimitMiddleware.__call__`` at the same instant, so any
    per-request value that had been parked on the middleware instance is overwritten N times before
    the first response is written.
    """

    def __init__(self, expected: int) -> None:
        self._expected = expected
        self._arrived = 0
        self._open = asyncio.Event()

    async def arrive(self) -> None:
        self._arrived += 1
        if self._arrived >= self._expected:
            self._open.set()
        await self._open.wait()


class StubRuntime:
    """The two collaborators :class:`~src.middleware.LimiterRuntime` declares, and nothing else."""

    def __init__(self, identity: StubIdentity, limiter: StubLimiter) -> None:
        self.identity = identity
        self.limiter = limiter


class StubState:
    """``app.state`` — a plain attribute bag, exactly what Starlette's ``State`` is."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime


class StubASGIApp:
    """The object the middleware finds at ``scope["app"]``. Starlette puts the real app there."""

    def __init__(self, runtime: Any) -> None:
        self.state = StubState(runtime)


# =============================================================================================
# Scope + send harness
# =============================================================================================


class Captured:
    """The raw ASGI messages one request produced, with the handful of readers a test wants."""

    def __init__(self, messages: list[dict[str, Any]]) -> None:
        self.messages = messages

    @property
    def start(self) -> dict[str, Any]:
        return next(m for m in self.messages if m["type"] == "http.response.start")

    @property
    def status(self) -> int:
        return int(self.start["status"])

    @property
    def raw_headers(self) -> list[tuple[bytes, bytes]]:
        return list(self.start.get("headers", []))

    @property
    def headers(self) -> dict[str, str]:
        """Header names lower-cased to ``str``. Duplicates collapse — see :meth:`names`."""
        return {
            name.decode("latin-1").lower(): value.decode("latin-1")
            for name, value in self.raw_headers
        }

    def names(self) -> list[str]:
        """Every header name **in order, with duplicates kept**, so a double-emit is visible."""
        return [name.decode("latin-1").lower() for name, _ in self.raw_headers]

    @property
    def body(self) -> bytes:
        return b"".join(
            m.get("body", b"") for m in self.messages if m["type"] == "http.response.body"
        )

    def json(self) -> Any:
        return json.loads(self.body)


def http_scope(
    *,
    path: str = "/api/v1/whoami",
    method: str = "GET",
    headers: list[tuple[bytes, bytes]] | None = None,
    app: Any = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A minimal but well-formed HTTP scope.

    ``state`` is omitted by default because ``httpx.ASGITransport`` and Starlette's ``TestClient``
    both omit it — the middleware has to create it, and defaulting to "absent" here means the
    common case is the one the tests exercise.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers if headers is not None else [],
        "client": ("127.0.0.1", 51234),
        "server": ("testserver", 80),
        "app": app,
    }
    if state is not None:
        scope["state"] = state
    return scope


async def drive(middleware: RateLimitMiddleware, scope: dict[str, Any]) -> Captured:
    """Run one request through ``middleware`` and return everything it wrote."""
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:  # pragma: no cover - a metered GET has no body to read
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await middleware(scope, receive, send)
    return Captured(messages)


def build(
    settings: Settings,
    *,
    app: StubApp | None = None,
    identity: StubIdentity | None = None,
    limiter: StubLimiter | None = None,
) -> tuple[RateLimitMiddleware, StubApp, StubIdentity, StubLimiter, StubASGIApp]:
    """Wire one middleware over one stub app, and hand back every double for assertion."""
    downstream = app if app is not None else StubApp()
    resolver = identity if identity is not None else StubIdentity(PRINCIPAL)
    checker = limiter if limiter is not None else StubLimiter(make_decision())
    asgi_app = StubASGIApp(StubRuntime(resolver, checker))
    return (
        RateLimitMiddleware(downstream, settings=settings),
        downstream,
        resolver,
        checker,
        asgi_app,
    )


PRINCIPAL = Principal(user_id="alice", credential=CredentialKind.API_KEY, key_id="demo-free")


# =============================================================================================
# 1. The scope-type guard
# =============================================================================================


@pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
async def test_non_http_scopes_pass_straight_through(settings, scope_type):
    """**The #1 bug in hand-written ASGI middleware**, asserted rather than reasoned about.

    A `lifespan` scope has no `path` and no `method` at all, so a middleware that reaches for
    either before checking `scope["type"]` raises a KeyError *during application startup* — before
    a single request is served, with a traceback that points at the rate limiter rather than at the
    missing guard. A `websocket` scope does have a path, which is worse: it would be classified,
    authenticated and charged one token for a handshake that then carries an unbounded number of
    frames.
    """
    middleware, downstream, identity, limiter, _app = build(settings)
    # Deliberately missing `path`, `method` and `app` — the shape the real server sends, and the
    # shape that proves nothing below the guard was reached.
    scope: dict[str, Any] = {"type": scope_type}

    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:  # pragma: no cover - never awaited by the stub app
        return {"type": "lifespan.startup"}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await middleware(scope, receive, send)

    assert downstream.calls == 1
    # The identical scope object, untouched: nothing added, nothing classified.
    assert downstream.scopes[0] is scope
    assert scope == {"type": scope_type}
    assert identity.calls == 0
    assert limiter.calls == []


# =============================================================================================
# 2. Exempt paths
# =============================================================================================


EXEMPT_PATHS = [
    "/health",
    "/health/",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/favicon.ico",
    "/dashboard",
    "/dashboard/",
    "/dashboard/index.html",
    "/static/app.css",
    "/static/js/dashboard.js",
    "/api/v1/admin",
    "/api/v1/admin/tiers",
    "/api/v1/admin/users/alice/tier",
]


@pytest.mark.parametrize("path", EXEMPT_PATHS)
async def test_exempt_paths_are_never_metered_and_carry_no_limit_headers(settings, path):
    """Unmetered, unauthenticated, and — the part worth asserting — **unadvertised**.

    An exempt response must not carry `X-RateLimit-*`: no bucket was consulted, so any number
    would be fabricated, and the dashboard polling `/dashboard/api/stats` would happily render it.
    """
    middleware, downstream, identity, limiter, asgi_app = build(settings)

    captured = await drive(middleware, http_scope(path=path, app=asgi_app))

    assert downstream.calls == 1
    assert identity.calls == 0
    assert limiter.calls == []
    assert not [
        name for name in captured.names() if name.startswith(RATE_LIMIT_HEADER_PREFIXES)
    ]
    # And the app's own response is untouched.
    assert captured.status == 200
    assert captured.headers["content-type"] == "text/plain"


@pytest.mark.parametrize(
    "path",
    [
        # A liveness probe's prefix must not exempt an endpoint that merely starts with it.
        "/healthz",
        "/healthz-of-mine",
        "/health-check",
        # The one that would matter most: an unmetered, unauthenticated route created by a naming
        # coincidence next to the admin API.
        "/api/v1/administrator",
        "/api/v1/adminx/tiers",
        "/dashboardx",
        "/dashboard-v2/index.html",
        "/staticfiles/app.css",
        "/docsy",
        "/openapi.json.bak",
        # A metered path that merely *contains* an exempt one. (This one classifies as the
        # `{id}` template rather than "other", which is why the assertion below counts the
        # metering rather than pinning a label.)
        "/api/v1/logs/health",
    ],
)
async def test_near_miss_paths_are_NOT_exempt(settings, path):
    """`startswith` on a bare prefix is the bug; a path-segment boundary is the fix.

    Every path here is one character away from an exemption, and every one of them must be metered.
    A miss in this direction is not a cosmetic bug: it is an unauthenticated, unmetered route that
    an attacker can find by reading the exempt list in the README.
    """
    middleware, downstream, identity, limiter, asgi_app = build(settings)

    captured = await drive(middleware, http_scope(path=path, app=asgi_app))

    assert is_exempt(path) is False
    assert identity.calls == 1
    assert len(limiter.calls) == 1
    assert downstream.calls == 1
    assert captured.headers["x-ratelimit-limit"] == "60"


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/health", True),
        ("/health/", True),
        ("/health//", True),
        ("/dashboard", True),
        ("/dashboard/", True),
        ("/dashboard/a/b/c", True),
        ("/api/v1/admin", True),
        ("/api/v1/admin/", True),
        ("/", False),
        ("//", False),
        ("", False),
        ("/api/v1/whoami", False),
        ("/healthz", False),
        ("/api/v1/administrator", False),
        ("/dashboardx", False),
        # Pinned by the verifier: these four must not move.
        ("/api/v1/logs/query/", False),
        ("/api/v1/whoami/", False),
        # A dot in a path is not a dot *segment* — these stay exempt.
        ("/openapi.json", True),
        ("/favicon.ico", True),
        ("/static/app.min.css", True),
    ],
)
def test_is_exempt_matches_exactly_or_on_a_segment_boundary(path, expected):
    """The matcher on its own, including the shapes an HTTP client can actually send.

    `//` is not hypothetical — `curl http://host//` sends it — and it rstrips to the empty string,
    which is why the normaliser floors at `/` rather than trusting `rstrip` to leave a path behind.
    """
    assert is_exempt(path) is expected


#: Every spelling of "walk out of an exempt prefix" that reaches the middleware. The
#: percent-encoded forms arrive **already decoded** (the ASGI scope carries the decoded path), so
#: the first three are literally the same string by the time `is_exempt` sees them; the last is
#: double-encoded and therefore survives as a literal segment, which the `%` guard covers.
TRAVERSAL_PATHS = [
    "/api/v1/admin/../../v1/logs/query",
    "/api/v1/admin/./../../logs/query",
    "/api/v1/admin/../logs/query",
    "/health/../api/v1/logs/query",
    "/dashboard/../api/v1/logs/query",
    "/static/../../api/v1/whoami",
    "/api/v1/admin/%2e%2e/logs/query",
    "/api/v1/admin/%252e%252e/logs/query",
]


@pytest.mark.parametrize("path", TRAVERSAL_PATHS)
def test_a_dot_segment_can_never_win_exemption(path):
    """**Defence in depth.** A traversal spelling must not buy unmetered, unauthenticated access.

    Every one of these string-prefixes an exempt entry while *naming* a metered endpoint. They are
    safe today only because of two facts this module does not own — Starlette never resolves dot
    segments, so they 404, and `StaticFiles` carries its own traversal check. Either can change
    without this file being edited: a `Mount` under `/dashboard` that resolves its own segments, or
    a proxy in front that normalises before forwarding, turns the coincidence into a live bypass.

    Note the direction of the fix: refused exemption, **not** normalised. Normalising would make
    `is_exempt` decide what `/dashboard/../health` "really means" and then require it to agree with
    whatever the router decides — which is the disagreement being avoided in the first place.
    """
    assert is_exempt(path) is False


@pytest.mark.parametrize("path", TRAVERSAL_PATHS)
async def test_a_traversal_path_is_metered_and_authenticated(settings, path):
    """The end of the same story: refused exemption means it goes down the full metered path."""
    middleware, downstream, identity, limiter, asgi_app = build(
        settings, identity=StubIdentity(None)
    )

    captured = await drive(middleware, http_scope(path=path, app=asgi_app))

    # Unauthenticated, so it is refused before it reaches anything.
    assert captured.status == 401
    assert identity.calls == 1
    assert downstream.calls == 0
    assert limiter.calls == []


def test_the_exempt_list_is_the_documented_one():
    """The set of holes in the enforcement layer is small, literal, and pinned.

    Asserted as a whole set rather than membership by membership: an exemption *added* is as much a
    contract change as one removed, and the added one is the one nobody notices.
    """
    assert EXEMPT_EXACT_PATHS == frozenset(
        {"/health", "/docs", "/redoc", "/openapi.json", "/favicon.ico"}
    )
    assert EXEMPT_PATH_PREFIXES == ("/dashboard", "/static", "/api/v1/admin")


# =============================================================================================
# 3. The operability switch
# =============================================================================================


async def test_disabled_passes_through_and_emits_no_headers(settings):
    """`RATE_LIMIT_ENABLED=false` must not merely allow everything — it must claim nothing.

    Emitting `X-RateLimit-Remaining: 60` while nothing is being counted is a lie a client will
    build pacing logic on top of. A header describing a limit that was never evaluated is worse
    than no header, because the client cannot tell the difference.
    """
    off = settings.model_copy(update={"rate_limit_enabled": False})
    middleware, downstream, identity, limiter, asgi_app = build(off)

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert downstream.calls == 1
    assert identity.calls == 0
    assert limiter.calls == []
    assert not [
        name for name in captured.names() if name.startswith(RATE_LIMIT_HEADER_PREFIXES)
    ]


async def test_disabled_still_classifies_so_analytics_keeps_a_label(settings):
    """C9 must still be able to say *which* endpoint was called while enforcement is off.

    The switch is also C14's overhead baseline, so "with the limiter" and "without it" are compared
    on the same graph — and a request recorded against no endpoint at all is a hole in exactly the
    comparison the switch exists for.
    """
    off = settings.model_copy(update={"rate_limit_enabled": False})
    middleware, _downstream, _identity, _limiter, asgi_app = build(off)
    scope = http_scope(path="/api/v1/logs/query", app=asgi_app)

    await drive(middleware, scope)

    assert scope["state"][SCOPE_ENDPOINT_KEY] == "GET:/api/v1/logs/query"
    # ...and no decision, because none was made.
    assert SCOPE_DECISION_KEY not in scope["state"]


# =============================================================================================
# 4. Classification and weighted cost
# =============================================================================================


@pytest.mark.parametrize(
    ("method", "path", "label", "cost"),
    [
        ("GET", "/api/v1/logs/query", "GET:/api/v1/logs/query", 5),
        ("POST", "/api/v1/logs/ingest", "POST:/api/v1/logs/ingest", 2),
        ("GET", "/api/v1/whoami", "GET:/api/v1/whoami", 1),
        ("GET", "/api/v1/logs/42", "GET:/api/v1/logs/{id}", 1),
        # An unrouted path is still classified and still charged — see the flow's step 4.
        ("GET", "/nope", "other", 1),
    ],
)
async def test_the_classified_label_and_weighted_cost_reach_the_limiter(
    settings, method, path, label, cost
):
    """The weighted-cost bonus, at its one call site: category -> `ENDPOINT_COSTS` -> the script."""
    middleware, _downstream, _identity, limiter, asgi_app = build(settings)

    await drive(middleware, http_scope(path=path, method=method, app=asgi_app))

    assert limiter.calls == [("alice", label, cost)]


async def test_head_is_charged_the_price_of_the_get_it_is_served_from(settings):
    """Fix for the method-side pricing bypass, observed where it matters: at the limiter call.

    A plain Starlette `Route(methods=["GET"])` auto-adds HEAD, so `HEAD /api/v1/logs/query` is
    served by the 5-token handler. Before the alias it was charged 1 token on the `other` bucket —
    the expensive endpoint at the cheap price, billed to a key that is not even the one being used.
    """
    middleware, _downstream, _identity, limiter, asgi_app = build(settings)

    await drive(middleware, http_scope(path="/api/v1/logs/query", method="HEAD", app=asgi_app))

    assert limiter.calls == [("alice", "GET:/api/v1/logs/query", 5)]


def test_every_method_the_router_dispatches_classifies_identically(settings):
    """**The guard against a priced route reachable by a method the table does not enumerate.**

    Walks the assembled application and asserts, per route, that every method the router will
    dispatch to that route classifies to the same label and category. One route means one handler
    means one price; a route whose methods disagree is a request served by X and charged as Y.

    A plain `Route` is mounted on the project's most expensive path first, because that is exactly
    the shape that makes the bug live — FastAPI's `APIRoute` 405s an unlisted method, while a plain
    Starlette `Route` silently accepts HEAD. Without that mount the assertion would pass over four
    exempt documentation routes and prove nothing.
    """
    app = create_app(runtime=Runtime.build(settings))

    async def priced(request: Any) -> PlainTextResponse:  # pragma: no cover - never dispatched
        return PlainTextResponse("ok")

    app.router.routes.append(Route("/api/v1/logs/query", priced, methods=["GET"]))

    checked = 0
    for route in app.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        if not methods or path is None:
            # A Mount or a WebSocketRoute — neither carries a method set to disagree about.
            continue
        # Path templates are not URLs; substitute a sample segment so a future `/{id}` route is
        # covered by this invariant rather than silently skipped by it.
        sample = re.sub(r"\{[^/}]*\}", "42", path)
        classifications = {classify(method, sample) for method in methods}
        assert len(classifications) == 1, (
            f"route {path} dispatches {sorted(methods)} but classifies them as "
            f"{sorted(classifications)} — one of those methods is served by this handler and "
            "charged as a different endpoint"
        )
        checked += 1

    assert checked >= 1
    # ...and the mounted route really is the HEAD-accepting shape this test exists for.
    assert app.routes[-1].methods == {"GET", "HEAD"}


async def test_an_unpriced_category_falls_back_to_the_default_cost(settings):
    """A category `ENDPOINT_COSTS` does not price must not KeyError on the hot path.

    An operator can drop `logs_query` from the env without touching the route table, and the
    failure would otherwise be a 500 for every caller of the project's most expensive endpoint.
    """
    trimmed = settings.model_copy(update={"endpoint_costs": {"default": 3}})
    middleware, _downstream, _identity, limiter, asgi_app = build(trimmed)

    await drive(middleware, http_scope(path="/api/v1/logs/query", app=asgi_app))

    assert limiter.calls == [("alice", "GET:/api/v1/logs/query", 3)]


# =============================================================================================
# 5. The 401
# =============================================================================================


async def test_no_principal_produces_a_complete_401(settings):
    """Status, challenge, JSON body, `content-length` — and **no** rate-limit headers."""
    middleware, downstream, _identity, limiter, asgi_app = build(
        settings, identity=StubIdentity(None)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 401
    # RFC 9110 §11.6.1: a 401 without a challenge is malformed. The value is generated from
    # ACCEPTED_SCHEMES, so it advertises BOTH schemes rather than only Bearer.
    assert captured.headers["www-authenticate"] == WWW_AUTHENTICATE
    assert "ApiKey" in captured.headers["www-authenticate"]
    assert "Bearer" in captured.headers["www-authenticate"]

    assert captured.headers["content-type"] == JSON_CONTENT_TYPE
    assert captured.headers["content-length"] == str(len(captured.body))
    assert int(captured.headers["content-length"]) > 0

    body = captured.json()
    assert body["error"] == UNAUTHORIZED_ERROR
    assert body["detail"]

    # No principal means no bucket. Any number here would be fiction.
    assert not [
        name for name in captured.names() if name.startswith(RATE_LIMIT_HEADER_PREFIXES)
    ]

    # And nothing downstream ran, and nothing was charged.
    assert downstream.calls == 0
    assert limiter.calls == []


async def test_the_401_does_not_say_which_credential_failed(settings):
    """Distinguishing "unknown key" from "expired token" on the wire is an enumeration oracle.

    `IdentityResolver.resolve` collapses every caller-side failure to `None` for exactly this
    reason; the middleware must not reintroduce the distinction by inspecting headers itself.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, identity=StubIdentity(None)
    )
    headers = [(b"x-api-key", b"revoked-key-12345")]

    captured = await drive(middleware, http_scope(headers=headers, app=asgi_app))

    text = captured.body.decode()
    assert "revoked" not in text.lower()
    assert "revoked-key-12345" not in text
    assert set(captured.json()) == {"error", "detail"}


def test_www_authenticate_is_exposed_to_browser_javascript():
    """The one header this middleware emits that does not come from `LimitDecision.headers()`.

    `WWW-Authenticate` is not CORS-safelisted, so without it in `expose_headers` a browser client
    that got a 401 could read the status and not the challenge — i.e. could not discover that this
    API accepts an `ApiKey` scheme at all. `tests/unit/test_models.py` pins the other eight.
    """
    assert WWW_AUTHENTICATE_HEADER in EXPOSE_HEADERS


# =============================================================================================
# 6. The 429
# =============================================================================================


@pytest.mark.parametrize(
    ("reason", "error"),
    [
        (DenyReason.RATE_LIMIT, ERROR_RATE_LIMIT),
        (DenyReason.SLIDING_WINDOW, ERROR_RATE_LIMIT),
        (DenyReason.QUOTA_DAILY, ERROR_QUOTA),
        (DenyReason.QUOTA_MONTHLY, ERROR_QUOTA),
    ],
)
async def test_a_denied_decision_produces_a_complete_429(settings, reason, error):
    """The spec's two literal error strings, 429 for both, `reason` carrying the distinction.

    Status 429 for a quota problem as well as a rate problem is deliberate: the spec names 429, and
    every HTTP client library already has retry behaviour attached to it. Using 402/403 for the
    quota family would be defensible in a green-field API and wrong here.
    """
    decision = denied(reason)
    middleware, downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(decision)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 429
    assert captured.headers["content-type"] == JSON_CONTENT_TYPE
    assert captured.headers["content-length"] == str(len(captured.body))

    body = captured.json()
    assert body["error"] == error
    assert body["reason"] == reason.value
    assert body["detail"]
    assert body["quota"]["daily"]["limit"] == decision.daily_limit
    assert body["quota"]["monthly"]["limit"] == decision.monthly_limit

    # Every header the decision describes, on the rejection as well as on the 200.
    for name, value in decision.headers().items():
        assert captured.headers[name.lower()] == value

    assert int(captured.headers["retry-after"]) >= 1
    assert downstream.calls == 0


async def test_retry_after_is_never_zero_even_when_the_decision_says_so(settings):
    """`Retry-After: 0` is a retry storm the limiter manufactured against itself.

    A caller told to wait zero seconds retries immediately, is refused again, is told zero again.
    The floor lives on `LimitDecision._retry_after`; this asserts the middleware actually emits
    the floored value rather than the raw field.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(denied(retry_after_sec=0))
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.headers["retry-after"] == "1"


async def test_a_denied_request_never_reaches_the_wrapped_app(settings):
    """**"A rejected request costs zero downstream work"**, asserted on a call counter.

    This is the property that keeps the limiter a gate rather than a queue. If a refused request
    still paid for routing, dependency resolution and a handler, a caller could saturate the
    service with requests it is *refusing* — which is the load the limiter exists to shed.
    """
    middleware, downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(denied())
    )

    for _ in range(5):
        captured = await drive(middleware, http_scope(app=asgi_app))
        assert captured.status == 429

    assert downstream.calls == 0
    assert downstream.scopes == []


async def test_short_circuit_header_names_are_lower_case_bytes(settings):
    """ASGI says header names are lower-case **bytes**; a `str` here is silently mangled.

    The middleware writes these messages by hand (an `HTTPException` cannot work — the exception
    handlers live below this middleware, inside the router), so nothing else is doing the encoding.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(denied())
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    for name, value in captured.raw_headers:
        assert isinstance(name, bytes)
        assert isinstance(value, bytes)
        assert name == name.lower()


async def test_the_denial_writes_exactly_two_asgi_messages(settings):
    """Start then body, with no trailing empty chunk — a complete, non-streaming response."""
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(denied())
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert [m["type"] for m in captured.messages] == [
        "http.response.start",
        "http.response.body",
    ]
    assert captured.messages[1].get("more_body", False) is False


# =============================================================================================
# 7. The allowed path
# =============================================================================================


async def test_headers_are_appended_to_the_apps_own_response(settings):
    """The limiter decorates; it does not replace. The handler's status, body and headers survive."""
    decision = make_decision()
    app = StubApp(
        status=201,
        headers=[(b"content-type", b"application/json"), (b"x-custom", b"kept")],
        body=b'{"created":true}',
    )
    middleware, downstream, _identity, _limiter, asgi_app = build(
        settings, app=app, limiter=StubLimiter(decision)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert downstream.calls == 1
    assert captured.status == 201
    assert captured.body == b'{"created":true}'
    assert captured.headers["x-custom"] == "kept"
    assert captured.headers["content-type"] == "application/json"

    for name, value in decision.headers().items():
        assert captured.headers[name.lower()] == value
    # Advertised on the 200 too, not only on rejection: a client cannot pace itself off
    # information it receives only once it has already been refused.
    assert "retry-after" not in captured.headers


async def test_a_handler_set_limit_header_is_replaced_not_duplicated(settings):
    """`MutableHeaders.update`, not `.append`.

    Two contradictory `X-RateLimit-Remaining` values is the one outcome worse than either value on
    its own: an HTTP client picks whichever its parser reaches first, so the caller's pacing
    depends on library internals.
    """
    app = StubApp(headers=[(b"x-ratelimit-remaining", b"999999")])
    middleware, _downstream, _identity, _limiter, asgi_app = build(settings, app=app)

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.names().count("x-ratelimit-remaining") == 1
    assert captured.headers["x-ratelimit-remaining"] == "41"


async def test_a_non_conformant_header_name_is_replaced_not_duplicated(settings):
    """The gap `.update()` alone leaves, closed rather than documented.

    `MutableHeaders.__setitem__` compares the *stored* bytes against the lower-cased name it is
    setting, so `b"X-RateLimit-Limit"` from a raw-ASGI sub-app would be appended alongside our
    value instead of replaced — the very duplicate `.update()` is chosen to prevent. ASGI requires
    lower-case names and neither Starlette nor FastAPI can emit anything else, so this defends
    against a sub-app someone mounts later; the middleware lower-cases the app's names in the same
    single list build `MutableHeaders` was going to make anyway.
    """
    app = StubApp(
        headers=[(b"X-RateLimit-Limit", b"999999"), (b"X-Custom", b"kept")],
    )
    middleware, _downstream, _identity, _limiter, asgi_app = build(settings, app=app)

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.names().count("x-ratelimit-limit") == 1
    assert captured.headers["x-ratelimit-limit"] == "60"
    # Every other header the app produced survives — normalised, but present and unchanged.
    assert captured.headers["x-custom"] == "kept"
    assert all(name == name.lower() for name, _ in captured.raw_headers)


async def test_a_response_start_without_headers_is_still_decorated(settings):
    """A raw-ASGI app may omit `headers` entirely; `MutableHeaders` would AttributeError on it."""
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, app=StubApp(omit_headers=True)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 200
    assert captured.headers["x-ratelimit-limit"] == "60"


async def test_quota_headers_are_omitted_when_the_daily_gate_is_unenforced(settings):
    """`X-Quota-*` describes a counter. When there is no ceiling, there is no counter to describe.

    Reporting `X-Quota-Remaining: 0` for a caller whose quota is simply not being enforced would
    tell them they are exhausted when they are not. The rule lives on `LimitDecision.headers`; this
    asserts the middleware emits exactly what the decision says and adds nothing of its own.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings,
        limiter=StubLimiter(
            make_decision(daily_limit=0, daily_state=QuotaPeriodState.UNENFORCED)
        ),
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert "x-quota-limit" not in captured.headers
    assert "x-quota-remaining" not in captured.headers
    assert "x-quota-reset" not in captured.headers
    assert captured.headers["x-ratelimit-limit"] == "60"


async def test_the_response_status_is_captured_for_c9(settings, caplog):
    """C9's analytics record needs the status code, which only the response knows.

    The seam is the `status_code` local captured in `send_wrapper`; the debug line is what makes
    the capture observable today. C9 replaces the line with `analytics.record(...)`.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, app=StubApp(status=418)
    )

    with caplog.at_level(logging.DEBUG, logger="src.middleware"):
        await drive(middleware, http_scope(app=asgi_app))

    assert "418" in caplog.text
    assert "GET:/api/v1/whoami" in caplog.text


# =============================================================================================
# 8. `scope["state"]`
# =============================================================================================


async def test_the_decision_is_stashed_on_scope_state(settings):
    """"Transparent to route handlers" means **available, never mandatory**.

    A handler that wants the decision reads `request.state.rlq_decision`; one that has never heard
    of this middleware behaves identically. No dependency to declare, nothing to inherit.
    """
    decision = make_decision()
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(decision)
    )
    scope = http_scope(app=asgi_app)

    await drive(middleware, scope)

    assert scope["state"][SCOPE_DECISION_KEY] is decision
    assert scope["state"][SCOPE_ENDPOINT_KEY] == "GET:/api/v1/whoami"


async def test_the_decision_is_stashed_on_the_denied_path_too(settings):
    """C9 wants the decision whether or not the request was admitted."""
    decision = denied()
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(decision)
    )
    scope = http_scope(app=asgi_app)

    await drive(middleware, scope)

    assert scope["state"][SCOPE_DECISION_KEY] is decision


async def test_an_existing_scope_state_is_reused_not_replaced(settings):
    """uvicorn supplies `state` per request; httpx and TestClient do not. Both must work.

    Replacing it would drop whatever the server (or a middleware above) had already put there —
    which under uvicorn is the shallow copy of the lifespan state.
    """
    existing: dict[str, Any] = {"set_by_the_server": 1}
    middleware, _downstream, _identity, _limiter, asgi_app = build(settings)
    scope = http_scope(app=asgi_app, state=existing)

    await drive(middleware, scope)

    assert scope["state"] is existing
    assert existing["set_by_the_server"] == 1
    assert SCOPE_DECISION_KEY in existing


# =============================================================================================
# 9. Concurrency — the cross-request state-leak test
# =============================================================================================


async def test_concurrent_principals_never_see_each_others_numbers(settings):
    """**The test that would catch per-request state on the middleware instance.**

    One middleware instance serves every concurrent request in the process. A value parked on
    `self` at the start of request A and read at the end of request A is read *after* request B has
    overwritten it, because both awaits on this path are I/O and the event loop suspends at each.

    Random sleeps would interleave the requests sometimes. The rendezvous interleaves them always
    and maximally: all 40 requests are suspended inside `__call__` at the same instant — first at
    identity resolution, then again at the limiter — so any instance field would be overwritten 39
    times before the first response is written, and every caller would receive the last caller's
    numbers.

    Each request's expected `X-RateLimit-Remaining` is derived from its own principal id, so a
    leak is not merely detectable but names the request it leaked from.
    """
    concurrency = 40
    identity_gate = Rendezvous(concurrency)
    limiter_gate = Rendezvous(concurrency)

    def principal_for(headers: list[tuple[bytes, bytes]]) -> Principal:
        raw = dict(headers)[b"x-api-key"].decode()
        return Principal(user_id=raw, credential=CredentialKind.API_KEY, key_id=raw)

    def decision_for(user_id: str) -> LimitDecision:
        # `window_limit - window_used` is the binding term, so `effective_remaining` — and
        # therefore `X-RateLimit-Remaining` — is exactly this caller's index.
        index = int(user_id.removeprefix("user-"))
        return make_decision(
            user_id=user_id,
            window_limit=1000,
            window_used=1000 - index,
            bucket_remaining=1000,
            daily_limit=1000,
            daily_used=1000 - index,
        )

    middleware, downstream, identity, limiter, asgi_app = build(
        settings,
        identity=StubIdentity(resolver=principal_for, gate=identity_gate),
        limiter=StubLimiter(factory=decision_for, gate=limiter_gate),
    )

    async def one(index: int) -> tuple[int, Captured]:
        scope = http_scope(
            app=asgi_app, headers=[(b"x-api-key", f"user-{index}".encode())]
        )
        return index, await drive(middleware, scope)

    results = await asyncio.gather(*(one(index) for index in range(concurrency)))

    assert identity.calls == concurrency
    assert downstream.calls == concurrency
    for index, captured in results:
        assert captured.status == 200
        # Its own principal's remaining allowance, not the last one resolved.
        assert captured.headers["x-ratelimit-remaining"] == str(index)
        assert captured.headers["x-quota-remaining"] == str(index)

    # ...and each request was metered against its own principal and its own bucket.
    assert sorted(user for user, _label, _cost in limiter.calls) == sorted(
        f"user-{index}" for index in range(concurrency)
    )


async def test_the_middleware_instance_holds_no_per_request_attributes(settings):
    """A structural check on top of the behavioural one above.

    The behavioural test proves today's implementation does not leak; this one makes the *next*
    edit that adds `self._decision = ...` fail immediately, at the line responsible, instead of
    failing intermittently under load six months later.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(settings)
    before = set(vars(middleware))

    await drive(middleware, http_scope(app=asgi_app))
    await drive(middleware, http_scope(path="/api/v1/logs/query", app=asgi_app))

    assert set(vars(middleware)) == before
    assert before == {"app", "_settings", "_default_cost", "_identity_retry_after"}


# =============================================================================================
# 10. Failure handling — C8's two branches
#
# **This section changed at C8, and the change is the commit.** Until C7 both awaits on this path
# let `BackingStoreUnavailable` propagate unhandled, and the test here asserted exactly that,
# parametrized over both stages, because C6 had no business choosing a policy on C8's behalf.
#
# C8 chose, and the two stages now diverge — which is the whole point of the C5 verification note
# that demanded they be separate branches:
#
#   * identity  -> a 503 emitted HERE. Never a principal, never a pass-through, `FAIL_MODE`
#                  deliberately not consulted. The tests below are strictly stronger than the
#                  propagation assertion they replace: propagation only proved nobody had decided,
#                  while these prove no request is admitted, no downstream work happens, and no
#                  limit header is fabricated.
#   * limiter   -> handled inside `Limiter.check`, which returns a decision rather than raising, so
#                  there is NO handler here at all. The rendering split (429 for a real overage,
#                  503 for `BACKING_STORE`) is asserted below; the policy itself lives in
#                  `tests/unit/test_degradation.py`.
# =============================================================================================


async def test_an_identity_store_failure_is_a_503_and_never_a_principal(settings):
    """**The authentication-bypass guard**, and the most important assertion in this file.

    Failing open when *limits* could not be checked serves an unmetered request to a caller we
    identified — the documented degradation. Failing open when *identity* could not be resolved
    would serve an unauthenticated request to anyone holding any string, for as long as the store
    is down. That is not a degradation, it is an authentication bypass, and `src.identity`'s C8
    rubric measured how cheaply an attacker manufactures the outage that triggers it: 200 distinct
    unknown API keys were enough to take the shared pool and breaker out, pre-auth.

    So the request is refused, and the assertions below are the three ways a "refusal" could still
    have leaked something: the app must not run, the response must not be a 2xx/4xx that reads as
    a verdict, and it must carry no rate-limit numbers, because none were measured.
    """
    error = BackingStoreUnavailable("redis is down", op="identity:apikey")
    middleware, downstream, identity, limiter, asgi_app = build(
        settings, identity=StubIdentity(error=error)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 503
    # The wrapped app was never invoked: the request was refused, not served.
    assert downstream.calls == 0
    # ...and it was never metered either. Metering an unidentified caller would mean charging
    # somebody's bucket for a request whose owner was never established.
    assert identity.calls == 1
    assert limiter.calls == []
    # Retry-After is present and >= 1. A `Retry-After: 0` on a 503 is a retry storm, and 503 is the
    # status clients retry hardest against.
    assert int(captured.headers["retry-after"]) >= 1
    # No fabricated allowance, exactly as on the 401 path: no gate was evaluated, so any
    # X-RateLimit-* number would be invented — and a client cannot detect a wrong header.
    assert not [
        name for name in captured.names() if name.startswith(RATE_LIMIT_HEADER_PREFIXES[:2])
    ]
    assert "x-ratelimit-degraded" not in captured.headers
    body = captured.json()
    # NOT one of the two spec literals: a client pattern-matching "Rate limit exceeded" must never
    # be told that an unavailable credential store was a limit it exceeded.
    assert body["error"] not in {ERROR_RATE_LIMIT, ERROR_QUOTA}
    assert body["error"] == SERVICE_UNAVAILABLE_ERROR


@pytest.mark.parametrize("fail_mode", ["open", "closed"])
async def test_the_identity_503_is_not_configurable_by_fail_mode(settings, fail_mode):
    """`FAIL_MODE` governs what happens when *limits* cannot be checked, and nothing else.

    There is no deployment for which "we could not establish who you are, so come in" is the right
    answer, so making it configurable would be offering it.

    Parametrized over **both** settings rather than only the interesting one. `open` is the value
    under which a shared handler would have let the request through, so it is the one that would
    catch the bypass — but asserting only there leaves "identity does not consult FAIL_MODE" as a
    claim about a single value rather than about the setting, and a future handler keyed on
    `closed` would slip past. Two cases, one property, no reading between the lines.
    """
    middleware, downstream, _identity, limiter, asgi_app = build(
        settings.model_copy(update={"fail_mode": fail_mode}),
        identity=StubIdentity(error=BackingStoreUnavailable("down", op="identity:apikey")),
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 503
    assert downstream.calls == 0
    # Never metered either: there is no principal to meter, in either mode.
    assert limiter.calls == []


async def test_a_saturated_pool_on_the_identity_path_is_also_a_503(settings):
    """`BackingStoreOverloaded` is a `BackingStoreUnavailable`, so the identity branch covers it.

    The distinction between the two matters enormously to the *limiter* (one degrades, the other
    refuses) and not at all here: whether the store was unreachable or this process had no
    connection to reach it with, nothing was learned about the caller and the answer is the same.
    Inheriting the branch rather than adding a second one is the reason the exception is a
    subclass — see the third rubric in `src.redis_client`.
    """
    middleware, downstream, _identity, _limiter, asgi_app = build(
        settings,
        identity=StubIdentity(
            error=BackingStoreOverloaded("no connection available", op="identity:apikey")
        ),
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 503
    assert downstream.calls == 0


async def test_a_backing_store_denial_is_a_503_and_not_a_429(settings):
    """**429 means "you are over your limit". This caller is not — we could not find out.**

    `DenyReason.BACKING_STORE` is the one reason the decision script cannot produce: the limiter
    builds it by hand when it reached no verdict at all (`FAIL_MODE=closed` with the store down, or
    a saturated connection pool). Rendering it as 429 would tell a client library to back off
    against a limit it never hit, and would make an operator reading a 429 graph believe callers
    were being throttled when the enforcement layer was simply unavailable.
    """
    decision = denied(reason=DenyReason.BACKING_STORE, retry_after_sec=5, degraded=True)
    middleware, downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(decision)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 503
    assert downstream.calls == 0
    assert captured.headers["retry-after"] == "5"
    # The degraded marker rides along, because THIS refusal came from the degraded policy — it is
    # what tells a caller the difference between "you were refused by a limit" and "you were
    # refused because the limiter could not run".
    assert captured.headers["x-ratelimit-degraded"] == "1"
    assert captured.json()["error"] == SERVICE_UNAVAILABLE_ERROR


async def test_a_non_degraded_backing_store_denial_omits_the_degraded_header(settings):
    """The pool-exhaustion 503: refused, but nothing was degraded.

    The store is healthy and this replica ran out of connections to it. Marking that as degradation
    would blame Redis for local backpressure — the misdiagnosis C4's verification named — and would
    make `X-RateLimit-Degraded` mean two different things on the wire.
    """
    decision = denied(reason=DenyReason.BACKING_STORE, retry_after_sec=1, degraded=False)
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(decision)
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 503
    assert "x-ratelimit-degraded" not in captured.headers
    assert captured.headers["retry-after"] == "1"


@pytest.mark.parametrize(
    "reason",
    [
        DenyReason.RATE_LIMIT,
        DenyReason.SLIDING_WINDOW,
        DenyReason.QUOTA_DAILY,
        DenyReason.QUOTA_MONTHLY,
    ],
)
async def test_every_real_gate_still_refuses_with_the_spec_429(settings, reason):
    """The 503 branch must not have widened into "any denial the middleware finds confusing".

    Four real gates, four 429s with the spec's literal body. This is the guard that keeps the
    `BACKING_STORE` special case from becoming the general case.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings, limiter=StubLimiter(denied(reason=reason))
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 429
    assert captured.json()["error"] in {ERROR_RATE_LIMIT, ERROR_QUOTA}


async def test_a_degraded_allowed_decision_flows_through_with_its_header(settings):
    """The fail-open path is a **normal 200**, decorated — not a special response.

    The middleware does not know or care that the decision came from a local bucket: it wraps
    `send` and appends `decision.headers()` exactly as it does for a Redis-backed decision, so the
    degraded marker and the omission of every `X-Quota-*` both arrive through the one definition of
    what a decision's headers are.
    """
    middleware, downstream, _identity, _limiter, asgi_app = build(
        settings,
        limiter=StubLimiter(
            make_decision(degraded=True, daily_limit=0, monthly_limit=0, window_limit=30)
        ),
    )

    captured = await drive(middleware, http_scope(app=asgi_app))

    assert captured.status == 200
    assert downstream.calls == 1
    assert captured.headers["x-ratelimit-degraded"] == "1"
    assert captured.headers["x-ratelimit-limit"] == "30"
    assert not [name for name in captured.names() if name.startswith("x-quota-")]


async def test_the_middleware_wraps_no_handler_at_all_around_the_limiter(settings):
    """**The structural guard**, restored deliberately after C8 inverted the propagation tests.

    Until C7 a parametrized test asserted that `BackingStoreUnavailable` propagated from *both*
    awaits, and the limiter half of it was the only thing pinning that this file adds no `except`
    of its own around `runtime.limiter.check`. C8 moved that policy inside `Limiter.check` — which
    now returns a decision rather than raising — so the old assertion had nothing left to say and
    was removed with it. That left the guard itself unwitnessed.

    It is worth witnessing. The entire C8 structure rests on the fail-open policy being decided
    **once**, in the module that owns `FAIL_MODE`; a second `except BackingStoreUnavailable` added
    here later would be a divergent policy in the wrong module, silently overriding the first for
    every caller — and, because `Limiter.check` no longer raises that type, such a handler would be
    dead code that looked load-bearing until somebody "fixed" the limiter to raise again.

    Asserted with a type the middleware has no reason to know about: if `check` raises anything at
    all, it must reach the caller unchanged. A blanket handler here would swallow it and turn a bug
    into a fabricated verdict.
    """
    sentinel = RuntimeError("the limiter blew up in a way nobody anticipated")
    middleware, downstream, _identity, limiter, asgi_app = build(
        settings, limiter=StubLimiter(error=sentinel)
    )

    with pytest.raises(RuntimeError) as raised:
        await drive(middleware, http_scope(app=asgi_app))

    assert raised.value is sentinel
    # It really did reach the limiter, so this is not passing because the request stopped earlier.
    assert len(limiter.calls) == 1
    assert downstream.calls == 0


async def test_a_backing_store_failure_from_the_limiter_would_also_propagate(settings):
    """The same guard, aimed at the one type it would be most tempting to catch here.

    `Limiter.check` does not raise `BackingStoreUnavailable` — that is C8's whole point, and
    `tests/unit/test_degradation.py` proves it in every fail mode. So this drives a stub that does,
    and asserts the middleware still has nothing to say about it: the absence of a handler is the
    property, and this is the shape a re-added one would be caught by.
    """
    middleware, _downstream, _identity, _limiter, asgi_app = build(
        settings,
        limiter=StubLimiter(error=BackingStoreUnavailable("down", op="script:rlq")),
    )

    with pytest.raises(BackingStoreUnavailable) as raised:
        await drive(middleware, http_scope(app=asgi_app))

    assert raised.value.op == "script:rlq"


async def test_a_missing_runtime_raises_rather_than_serving_unmetered(settings):
    """A wiring bug must not read as a policy.

    Passing the request through would be a silent unmetered request — the exact failure this
    project exists to make impossible, arriving through a forgotten `await runtime.start()`. A 401
    would be a lie (nothing was asked about the credential) and a 503 would dress a bug in this
    process up as a store outage, which is what would make C8 fail *open* on it.
    """
    middleware, downstream, _identity, _limiter, _asgi_app = build(settings)

    with pytest.raises(RuntimeError, match="no runtime on app.state"):
        await drive(middleware, http_scope(app=StubASGIApp(None)))

    assert downstream.calls == 0
