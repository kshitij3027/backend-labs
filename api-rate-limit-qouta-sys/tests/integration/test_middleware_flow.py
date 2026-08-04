"""The whole enforcement layer, end to end, against a real Redis and a real app.

``tests/unit/test_middleware.py`` proves the middleware's ASGI mechanics against stubs. This file
proves the thing those stubs stand in for: a real
:class:`~src.identity.IdentityResolver` resolving a seeded demo key, a real
:class:`~src.limiter.Limiter` running the real Lua script against ``redis:7-alpine``, and a real
CORS middleware wrapped around the whole thing — driven through ``httpx.ASGITransport``, which is
the same in-process transport C12's distributed double-spend test uses.

.. rubric:: The routes here are mounted BY THE TEST, on purpose

C7 owns ``src/api/protected.py``. Until it lands, this suite mounts two throwaway handlers on the
app it just built — at the two paths :data:`src.keys.ROUTE_TABLE` already prices differently
(``GET /api/v1/whoami`` at cost 1, ``GET /api/v1/logs/query`` at cost 5) — so the weighted-cost
bonus can be observed decrementing a real bucket by 5 rather than 1.

They are local to this module for a reason beyond commit scope: the classifier and the router are
*independent* in this project by design (see the "agreeing with the router is a SECURITY property"
rubric in :mod:`src.keys`), so a route existing or not existing changes nothing about what a
request is labelled or charged. Mounting them here therefore proves exactly what C7 will prove,
with no ``src/`` change to unpick afterwards.

.. rubric:: Why some assertions land on a 404

An unrouted path is still metered — that is the point of running above the router — so a metered
request to a path with no handler comes back 404 **with the full set of rate-limit headers on it**.
Several tests below use that deliberately: it is the cleanest available proof that the headers are
appended to whatever the app produced rather than to a response the middleware wrote itself.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from src.config import Settings
from src.identity import DEMO_KEY_BY_TIER, WWW_AUTHENTICATE
from src.main import EXPOSE_HEADERS, Runtime, create_app
from src.middleware import JSON_CONTENT_TYPE, SCOPE_DECISION_KEY
from src.models import ERROR_RATE_LIMIT, LimitDecision, Tier

#: An origin that is not this app's, so CORS has something to answer about. The shipped
#: ``CORS_ORIGINS`` default is ``*``, so any value works; a literal one makes the intent obvious.
BROWSER_ORIGIN = "http://localhost:5173"

#: The free tier ships at 60 rpm / burst 60, so 61 requests is the smallest number that must
#: produce a refusal from *some* gate. Which gate refuses is deliberately not asserted — the
#: bucket and the account-wide window are sized identically for this tier, so either is correct
#: and pinning one would make the test about the tier table rather than about the middleware.
FREE_TIER_RPM = 60


def _mount_probe_routes(app: FastAPI) -> None:
    """Add the two cost-differentiated probes this suite drives. See the module docstring.

    ``request.state.rlq_decision`` is echoed back because it is the one way to observe, from
    outside the process, that the "transparent to route handlers" stash actually arrived — and
    that a handler reading it needs no dependency, no decorator and no import.
    """

    @app.get("/api/v1/whoami")
    async def whoami(request: Request) -> dict[str, Any]:  # pragma: no cover - driven over HTTP
        decision: LimitDecision = request.state.rlq_decision
        return {
            "user_id": decision.user_id,
            "tier": decision.tier,
            "endpoint": decision.endpoint,
            "cost": decision.cost,
        }

    @app.get("/api/v1/logs/query")
    async def logs_query() -> dict[str, str]:  # pragma: no cover - driven over HTTP
        return {"rows": "stub"}

    # A **plain Starlette** route, not a FastAPI `APIRoute`, and that is the whole point of it:
    # `Route(methods=["GET"])` auto-adds HEAD (`self.methods.add("HEAD")`) while an `APIRoute`
    # 405s it. It is mounted on a path priced at 5 tokens so `HEAD` can be shown being served by
    # the expensive handler *and* charged the expensive price — the divergence Fix 1 closes. C15's
    # dashboard and any future `Mount` are this same shape.
    async def plain_priced(request: Request) -> PlainTextResponse:  # pragma: no cover - over HTTP
        return PlainTextResponse("ok")

    # Mounted on the *same* path as the FastAPI route above, which is how this actually shows up:
    # GET matches the `APIRoute` FULL and is dispatched there, while HEAD only PARTIAL-matches it
    # (path yes, method no) and Starlette keeps looking — landing on this one.
    app.router.routes.append(Route("/api/v1/logs/query", plain_priced, methods=["GET"]))


@pytest.fixture()
async def limited_app(redis_settings: Settings):
    """A real app over a real, freshly flushed Redis, with the demo credentials seeded.

    ``create_app(runtime=...)`` skips the FastAPI lifespan by design, so this fixture takes on the
    lifespan's two jobs explicitly — ``runtime.start()`` on the way in and ``runtime.stop()`` on
    the way out. That responsibility split is documented on :func:`src.main.create_app`.

    The flush happens **between** connecting and seeding, and the order is load-bearing: seeding
    first and flushing afterwards would delete the ``apikey:v1:*`` records the whole suite
    authenticates against, and every test would fail on a 401 that looks like a middleware bug.
    """
    runtime = Runtime.build(redis_settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()

    app = create_app(runtime=runtime)
    _mount_probe_routes(app)
    try:
        yield app
    finally:
        try:
            await runtime.redis.client.flushdb()
        finally:
            await runtime.stop()


@pytest.fixture()
async def api(limited_app: FastAPI):
    """An ``httpx`` client speaking ASGI directly to the app — no socket, no server."""
    transport = httpx.ASGITransport(app=limited_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as instance:
        yield instance


def key_headers(tier: Tier) -> dict[str, str]:
    """``X-API-Key`` for one seeded demo principal, read from the declaration the server seeded."""
    return {"X-API-Key": DEMO_KEY_BY_TIER[tier]}


# =============================================================================================
# The happy path
# =============================================================================================


async def test_a_demo_key_gets_200_with_the_full_header_set(api: httpx.AsyncClient):
    """The shipped ``demo-free-key`` authenticates, is metered, and is told its allowance.

    Headers on the **200**, not only on the rejection: a client cannot pace itself off information
    it receives only after it has already been refused.
    """
    response = await api.get("/api/v1/whoami", headers=key_headers(Tier.FREE))

    assert response.status_code == 200
    assert response.json()["user_id"] == "demo-free"
    assert response.json()["tier"] == "free"
    assert response.json()["endpoint"] == "GET:/api/v1/whoami"

    assert response.headers["X-RateLimit-Limit"] == str(FREE_TIER_RPM)
    assert int(response.headers["X-RateLimit-Remaining"]) == FREE_TIER_RPM - 1
    assert int(response.headers["X-RateLimit-Reset"]) >= 0
    assert response.headers["X-Quota-Limit"] == "1000"
    assert response.headers["X-Quota-Remaining"] == "999"
    assert int(response.headers["X-Quota-Reset"]) > 0
    # Allowed, so no Retry-After: there is nothing to wait for.
    assert "Retry-After" not in response.headers


async def test_remaining_decrements_across_successive_requests(api: httpx.AsyncClient):
    """The counter is in Redis, so the second request sees what the first one spent.

    This is the assertion that would fail if the bucket lived in process memory — which is the bug
    this whole project exists to not have. It is proved *across replicas* in C12; here it is proved
    across requests, which is the same store read from the same middleware.
    """
    headers = key_headers(Tier.FREE)
    remaining = []
    quota_remaining = []
    for _ in range(4):
        response = await api.get("/api/v1/whoami", headers=headers)
        assert response.status_code == 200
        remaining.append(int(response.headers["X-RateLimit-Remaining"]))
        quota_remaining.append(int(response.headers["X-Quota-Remaining"]))

    assert remaining == [59, 58, 57, 56]
    assert quota_remaining == [999, 998, 997, 996]


async def test_headers_land_on_a_response_the_middleware_did_not_write(
    api: httpx.AsyncClient,
):
    """An unrouted path is metered too, and the 404 the router produced carries the headers.

    Running above the router is what makes this true, and it is a security property rather than a
    convenience: a limiter that only sees requests the router recognised is one a caller bypasses
    by sending `GET /nope`.
    """
    response = await api.get("/nope", headers=key_headers(Tier.FREE))

    assert response.status_code == 404
    assert int(response.headers["X-RateLimit-Remaining"]) == FREE_TIER_RPM - 1


async def test_the_decision_reaches_the_handler(api: httpx.AsyncClient):
    """"Transparent to route handlers" — the handler read `request.state.rlq_decision`.

    The probe route declares no dependency and imports nothing from the middleware; it just reads
    an attribute that is there. A route added by someone who has never heard of this middleware
    behaves identically, which is the other half of the same property.
    """
    response = await api.get("/api/v1/whoami", headers=key_headers(Tier.PREMIUM))

    body = response.json()
    assert body["user_id"] == "demo-premium"
    assert body["cost"] == 1
    assert SCOPE_DECISION_KEY == "rlq_decision"


# =============================================================================================
# Weighted cost — the in-scope bonus
# =============================================================================================


async def test_a_cost_5_endpoint_decrements_remaining_by_five(api: httpx.AsyncClient):
    """`ENDPOINT_COSTS` in action: `logs_query` is 5 tokens, `whoami` is 1.

    A read that fans out across the log store is not the same unit of work as a whoami, and
    charging both one token prices the expensive call as though it were free.

    `X-RateLimit-Remaining` reports the **binding** gate, which here is the account-wide sliding
    window — the token buckets are per `(user, endpoint)` and therefore separate for these two
    paths, while the window counts every request the account makes. That is exactly why the delta
    is readable across two different endpoints at all.
    """
    headers = key_headers(Tier.FREE)

    first = await api.get("/api/v1/whoami", headers=headers)
    second = await api.get("/api/v1/whoami", headers=headers)
    third = await api.get("/api/v1/logs/query", headers=headers)
    fourth = await api.get("/api/v1/logs/query", headers=headers)

    remaining = [int(r.headers["X-RateLimit-Remaining"]) for r in (first, second, third, fourth)]

    assert all(r.status_code == 200 for r in (first, second, third, fourth))
    # cost 1, cost 1, cost 5, cost 5.
    assert remaining[0] - remaining[1] == 1
    assert remaining[1] - remaining[2] == 5
    assert remaining[2] - remaining[3] == 5

    # The quota counter is charged the same weight, from the same script call.
    assert int(fourth.headers["X-Quota-Remaining"]) == 1000 - (1 + 1 + 5 + 5)


async def test_head_on_a_plain_route_is_served_and_charged_the_full_price(
    limited_app: FastAPI, api: httpx.AsyncClient
):
    """**The method-side pricing bypass, reproduced against the real stack and shown closed.**

    A plain Starlette `Route(methods=["GET"])` auto-adds HEAD, so this request is genuinely served
    by the 5-token handler. Keyed on the exact method, the classifier called it `("other",
    "default")` — 1 token, charged to `rate_limit:{demo-free}:other`, a bucket that has nothing to
    do with the endpoint being used. The caller got the expensive endpoint at a fifth of the price
    *and* the overspend was invisible in that endpoint's own metering.

    Both halves are asserted: the price (Remaining drops by 5, not 1) and the key it was drawn
    from (the GET bucket exists, the `other` bucket does not).
    """
    runtime = limited_app.state.runtime
    headers = key_headers(Tier.FREE)

    warmup = await api.get("/api/v1/whoami", headers=headers)
    head = await api.head("/api/v1/logs/query", headers=headers)

    assert head.status_code == 200
    assert int(warmup.headers["X-RateLimit-Remaining"]) == FREE_TIER_RPM - 1
    assert int(head.headers["X-RateLimit-Remaining"]) == FREE_TIER_RPM - 1 - 5

    buckets = {key.decode() for key in await runtime.redis.client.keys("rate_limit:*")}
    assert "rate_limit:{demo-free}:GET:/api/v1/logs/query" in buckets
    assert "rate_limit:{demo-free}:other" not in buckets


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/admin/../../v1/logs/query",
        "/api/v1/admin/./../../logs/query",
        "/api/v1/admin/%2e%2e/logs/query",
        "/health/../api/v1/logs/query",
    ],
)
async def test_a_traversal_spelling_never_buys_a_free_pass(api: httpx.AsyncClient, path: str):
    """Defence in depth, end to end: no traversal spelling is served unmetered or unauthenticated.

    Each of these string-prefixes an exempt entry while naming a metered endpoint, so a matcher
    that looked only at prefixes would hand back an unmetered, unauthenticated response.

    .. rubric:: httpx resolves dot segments before sending, which is the point rather than a flaw

    Two of these four arrive at the application already collapsed onto `/api/v1/logs/query` — the
    *client* normalised them. That is exactly the scenario the guard exists for, demonstrated by
    the first component that happened to sit in front of the app: whoever is upstream (httpx here,
    nginx or a service mesh in production) may resolve dot segments differently from, or at a
    different time than, the app does. Which spelling reaches `is_exempt` is therefore not
    something this service gets to decide, so the safe answer cannot depend on it.

    So this test asserts the property that has to hold under *every* upstream: anonymous never gets
    a 2xx, and a credentialed request is always metered. The raw, un-normalised path — the one no
    HTTP client will send — is driven straight into the ASGI scope in
    `tests/unit/test_middleware.py`, which is the only place it can be presented at all.
    """
    anonymous = await api.get(path)
    assert anonymous.status_code == 401

    metered = await api.get(path, headers=key_headers(Tier.FREE))
    assert metered.status_code in {200, 404}
    # Metered means metered: the headers are there and the allowance really moved.
    assert int(metered.headers["X-RateLimit-Remaining"]) < FREE_TIER_RPM


# =============================================================================================
# Refusal, and per-principal isolation
# =============================================================================================


async def _drain_free_tier(api: httpx.AsyncClient) -> httpx.Response:
    """Fire requests as ``demo-free`` until one is refused; return that response."""
    headers = key_headers(Tier.FREE)
    for _ in range(FREE_TIER_RPM + 5):
        response = await api.get("/api/v1/whoami", headers=headers)
        if response.status_code == 429:
            return response
    raise AssertionError(
        f"the free tier admitted more than {FREE_TIER_RPM + 5} requests in one window — "
        "the limiter is not enforcing anything"
    )


async def test_exhausting_the_free_tier_produces_a_complete_429(api: httpx.AsyncClient):
    """The rejection a client actually receives: status, body, headers, and a usable backoff."""
    response = await _drain_free_tier(api)

    assert response.status_code == 429
    assert response.headers["content-type"] == JSON_CONTENT_TYPE
    assert response.headers["content-length"] == str(len(response.content))

    body = response.json()
    assert body["error"] == ERROR_RATE_LIMIT
    # Which of the two rate gates refused is a real distinction (see `DenyReason`) and both are
    # correct here, because this tier's burst and rpm are deliberately the same number.
    assert body["reason"] in {"rate_limit", "sliding_window"}
    assert body["detail"]

    # >= 1, always. `Retry-After: 0` would be a retry storm the limiter manufactured against
    # itself: retry immediately, be refused, be told 0 again.
    assert int(response.headers["Retry-After"]) >= 1
    assert body["retry_after"] == int(response.headers["Retry-After"])
    assert response.headers["X-RateLimit-Remaining"] == "0"


async def test_a_premium_key_still_succeeds_while_free_is_exhausted(api: httpx.AsyncClient):
    """**Per-principal isolation**, at the moment it matters.

    A limiter that refused everybody once one caller misbehaved would be a global rate limit with
    extra steps. The bucket key carries the user id (`rate_limit:{demo-free}:...`), so draining one
    principal's allowance is invisible to every other principal — asserted here at the instant the
    first one is being refused, not before or after it.
    """
    denied = await _drain_free_tier(api)
    assert denied.status_code == 429

    premium = await api.get("/api/v1/whoami", headers=key_headers(Tier.PREMIUM))

    assert premium.status_code == 200
    # Premium's ceiling is strictly higher, and it is untouched by free's spending.
    assert int(premium.headers["X-RateLimit-Limit"]) == 300
    assert int(premium.headers["X-RateLimit-Remaining"]) == 299


async def test_health_is_reachable_while_a_principal_is_fully_limited(
    api: httpx.AsyncClient,
):
    """**The exemption is load-bearing, not cosmetic.**

    The container HEALTHCHECK polls `/health` every 10 s from one source address and compose
    restarts the replica on a non-200. Metered, this probe would 429 during exactly the traffic
    the limiter is there to shed — so the limiter would take a healthy replica down by working
    correctly. It is also unauthenticated: the healthcheck carries no credential at all.
    """
    assert (await _drain_free_tier(api)).status_code == 429

    probe = await api.get("/health")

    assert probe.status_code == 200
    assert probe.json()["status"] == "healthy"
    # Unmetered means unadvertised: no counter was consulted, so there is no number to report.
    assert "X-RateLimit-Remaining" not in probe.headers
    assert "Retry-After" not in probe.headers


# =============================================================================================
# The 401
# =============================================================================================


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-API-Key": "not-a-real-key"},
        {"Authorization": "Bearer not-a-jwt"},
        {"Authorization": "Basic dXNlcjpwYXNz"},
    ],
    ids=["no-credential", "unknown-key", "bad-token", "unsupported-scheme"],
)
async def test_an_unresolvable_caller_gets_a_401_with_a_challenge(
    api: httpx.AsyncClient, headers: dict[str, str]
):
    """Every caller-side identity failure is one fact on the wire, and it names no bucket."""
    response = await api.get("/api/v1/whoami", headers=headers)

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == WWW_AUTHENTICATE
    assert response.headers["content-type"] == JSON_CONTENT_TYPE
    assert response.headers["content-length"] == str(len(response.content))
    assert response.json()["error"] == "Unauthorized"

    assert not [name for name in response.headers if name.lower().startswith("x-ratelimit-")]
    assert not [name for name in response.headers if name.lower().startswith("x-quota-")]


async def test_an_unauthenticated_request_never_reaches_the_handler(
    api: httpx.AsyncClient,
):
    """The 401 short-circuits above the router, so an unrouted path 401s rather than 404s.

    That ordering is deliberate: routing an unauthenticated request first would make 404-vs-401 a
    free path-enumeration oracle for anyone with no credential at all.
    """
    response = await api.get("/definitely/not/a/route")

    assert response.status_code == 401


# =============================================================================================
# CORS — the middleware ordering, asserted from outside
# =============================================================================================


def _exposed(response: httpx.Response) -> set[str]:
    return {
        name.strip().lower()
        for name in response.headers["access-control-expose-headers"].split(",")
    }


async def test_cors_exposes_every_limit_header_ON_A_429(api: httpx.AsyncClient):
    """**The ordering test.** CORS must be OUTSIDE the limiter, or a browser cannot read a 429.

    The limiter short-circuits its 429 without calling anything below it. Registered outside CORS,
    that response would carry no `Access-Control-Allow-Origin` and no
    `Access-Control-Expose-Headers`, so a browser `fetch` would surface the rejection as an opaque
    network error and the client could read neither the status nor the `Retry-After` telling it
    when to come back. The rate limiter would be invisible to precisely the client the dashboard
    is written in.

    Starlette applies middleware in reverse registration order (last added = outermost), so this
    only holds because `src.main.create_app` registers the limiter *before* CORS. That is exactly
    the kind of fact a comment cannot keep true.
    """
    headers = {**key_headers(Tier.FREE), "Origin": BROWSER_ORIGIN}
    for _ in range(FREE_TIER_RPM + 5):
        response = await api.get("/api/v1/whoami", headers=headers)
        if response.status_code == 429:
            break
    else:  # pragma: no cover - only reachable if the limiter stopped enforcing
        raise AssertionError("the free tier was never refused")

    assert response.status_code == 429
    assert response.headers["access-control-allow-origin"] == "*"
    assert _exposed(response) == {name.lower() for name in EXPOSE_HEADERS}
    # ...and the headers the browser is now permitted to read are actually present.
    assert "Retry-After" in response.headers
    assert "X-RateLimit-Remaining" in response.headers


async def test_cors_exposes_every_limit_header_on_a_200(api: httpx.AsyncClient):
    """The same list on the admitted path, so a browser client can pace itself before it is cut off."""
    response = await api.get(
        "/api/v1/whoami", headers={**key_headers(Tier.FREE), "Origin": BROWSER_ORIGIN}
    )

    assert response.status_code == 200
    assert _exposed(response) == {name.lower() for name in EXPOSE_HEADERS}


async def test_cors_exposes_the_challenge_on_a_401(api: httpx.AsyncClient):
    """`WWW-Authenticate` is not CORS-safelisted; without exposing it the challenge is invisible."""
    response = await api.get("/api/v1/whoami", headers={"Origin": BROWSER_ORIGIN})

    assert response.status_code == 401
    assert "www-authenticate" in _exposed(response)


async def test_a_preflight_is_answered_by_cors_and_never_charged(api: httpx.AsyncClient):
    """A browser's own protocol overhead must not come out of the caller's quota.

    With CORS outermost, the preflight `OPTIONS` is answered before the limiter is reached at all,
    so it consumes nothing — asserted by checking that the very next real request still reports a
    full allowance.
    """
    preflight = await api.options(
        "/api/v1/whoami",
        headers={
            "Origin": BROWSER_ORIGIN,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "x-api-key",
        },
    )

    assert preflight.status_code == 200
    assert "X-RateLimit-Remaining" not in preflight.headers

    follow_up = await api.get("/api/v1/whoami", headers=key_headers(Tier.FREE))
    assert int(follow_up.headers["X-RateLimit-Remaining"]) == FREE_TIER_RPM - 1


# =============================================================================================
# The operability switch, through the real stack
# =============================================================================================


async def test_the_switch_off_serves_unmetered_and_advertises_nothing(
    redis_settings: Settings,
):
    """`RATE_LIMIT_ENABLED=false` through the *same* middleware stack, not a different app.

    The middleware is registered unconditionally and self-disables, precisely so this measurement
    compares one application in two configurations rather than two applications — see the ordering
    block in `src.main.create_app`. What "off" must mean: no 401 for a caller with no credential,
    no counter touched, and **no headers claiming a limit that was never evaluated**.
    """
    off = redis_settings.model_copy(update={"rate_limit_enabled": False})
    runtime = Runtime.build(off)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()
    app = create_app(runtime=runtime)
    _mount_probe_routes(app)

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as api:
            # No credential at all, and far more requests than the free tier's ceiling.
            for _ in range(FREE_TIER_RPM + 10):
                response = await api.get("/api/v1/logs/query")
                assert response.status_code == 200

        assert not [n for n in response.headers if n.lower().startswith("x-ratelimit-")]
        assert not [n for n in response.headers if n.lower().startswith("x-quota-")]
        # Nothing was counted, either — the switch stops the write as well as the header.
        assert await runtime.redis.client.keys("rate_limit:*") == []
    finally:
        try:
            await runtime.redis.client.flushdb()
        finally:
            await runtime.stop()
