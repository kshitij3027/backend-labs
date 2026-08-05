"""The metered stub downstream, driven over HTTP against a real Redis and the real middleware.

``tests/integration/test_middleware_flow.py`` is about the *enforcement layer*: who is refused,
what the headers say, which gate fired. This file is about the surface that layer protects — the
four routes :data:`src.keys.ROUTE_TABLE` prices — and about the one property that connects the
two: **the router and the classifier must agree about what every path is.**

.. rubric:: The cross-check is the reason this file exists

Everything else here is a stub returning stub data, and a suite that only asserted "the JSON has
the right keys" would be testing a fixture. The assertions worth their run time are:

* every mounted ``/api/v1`` route prices to the :data:`src.keys.ROUTE_TABLE` row it is supposed
  to, and the check that proves it *fails* when any of the three declarations drift apart;
* ``/whoami`` still answers with ``RATE_LIMIT_ENABLED=false`` — the transparency property, which
  is the difference between a handler that reads the limiter's decision and one that *depends* on
  it;
* a request the handler rejects with a ``422`` was still **metered**, because the limiter ran
  above the router and the cost was consumed before the body was ever parsed.

Driven through ``httpx.ASGITransport`` — no socket, no server — against ``redis:7-alpine`` over
the compose network.
"""

from __future__ import annotations

from dataclasses import replace

import httpx
import pytest
from fastapi import FastAPI
from starlette.routing import Route

import src.api.protected as protected
import src.main as main
from src.api.protected import (
    CORPUS_SEED,
    CORPUS_SIZE,
    MAX_PAGE_SIZE,
    STORE_CAPACITY,
    LogEntry,
    LogLevel,
    LogStore,
    generate_corpus,
    mounted_v1_routes,
    query_logs,
    read_log,
    resolve_route,
    sample_path,
    verify_route_pricing,
    whoami,
)
from src.config import Settings
from src.identity import DEMO_KEY_BY_TIER, issue_token
from src.keys import ROUTE_TABLE, classify
from src.main import Runtime, create_app
from src.models import Tier

#: ``(tier, user_id, rpm, daily quota)`` for the three seeded demo principals, straight from
#: ``DEFAULT_TIER_LIMITS_SPEC``. Written out rather than read off ``Settings`` so the suite states
#: what the shipped tier table *is*, instead of asserting that it equals itself.
DEMO_TIERS = [
    (Tier.FREE, "demo-free", 60, 1000),
    (Tier.PREMIUM, "demo-premium", 300, 50000),
    (Tier.ENTERPRISE, "demo-enterprise", 1000, 500000),
]

#: The free tier's per-minute ceiling, so the root_path tests can state a charge as a delta from
#: a full allowance rather than as a bare number.
FREE_TIER_RPM = 60

#: A prefix an ASGI server or a proxy might mount this application under. Nothing in this
#: repository sets one today — which is exactly why the tests that use it exist.
ROOT_PATH = "/gw"


@pytest.fixture()
async def protected_app(redis_settings: Settings):
    """A real app over a real, freshly flushed Redis, with the demo credentials seeded.

    ``create_app(runtime=...)`` skips the FastAPI lifespan by design, so this fixture takes the
    lifespan's two jobs on explicitly. The flush happens **between** connecting and seeding: the
    other order would delete the ``apikey:v1:*`` records every test here authenticates against,
    and the whole file would fail on 401s that look like a middleware bug.
    """
    runtime = Runtime.build(redis_settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()

    app = create_app(runtime=runtime)
    try:
        yield app
    finally:
        try:
            await runtime.redis.client.flushdb()
        finally:
            await runtime.stop()


@pytest.fixture()
async def api(protected_app: FastAPI):
    """An ``httpx`` client speaking ASGI directly to the app — no socket, no server."""
    transport = httpx.ASGITransport(app=protected_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as instance:
        yield instance


@pytest.fixture()
async def gateway_api(protected_app: FastAPI):
    """The same app, mounted under ``/gw`` the way a server or proxy would mount it.

    ``ASGITransport(root_path=...)`` reproduces the real deployment shape exactly: the scope
    carries the **full** path (``/gw/api/v1/logs/query``) *and* a ``root_path`` of ``/gw``, and it
    is the application's job to know that the router only ever sees the difference between them.
    """
    transport = httpx.ASGITransport(app=protected_app, root_path=ROOT_PATH)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as instance:
        yield instance


def key_headers(tier: Tier) -> dict[str, str]:
    """``X-API-Key`` for one seeded demo principal, read from the declaration the server seeded."""
    return {"X-API-Key": DEMO_KEY_BY_TIER[tier]}


# =============================================================================================
# The classifier/router cross-check — the security property, not the stub
# =============================================================================================


def test_the_router_prefix_is_the_one_main_documents():
    """``src/api/protected.py`` re-spells ``/api/v1`` because importing it back would be a cycle.

    ``src.main`` imports this router, so the router cannot import ``src.main.API_V1_PREFIX``. The
    duplication is therefore unavoidable and is pinned here instead of hoped for — a v2 that moved
    the prefix in one file and not the other would otherwise mount the whole metered surface at a
    path the classifier prices as ``other``.
    """
    assert protected.API_V1_PREFIX == main.API_V1_PREFIX


def test_every_mounted_route_prices_to_its_route_table_row(protected_app: FastAPI):
    """**The pricing cross-check, asserted against the app that actually ships.**

    Four routes, four ``ROUTE_TABLE`` rows, and the mapping between them is checked by running
    each mounted path back through the real :func:`src.keys.classify`. That is the whole point:
    the classifier decides what a request *costs* and Starlette decides what it *does*, and any
    input they disagree about is a caller being served endpoint X at endpoint Y's price.

    The weights are asserted too, so this test states the contract end to end: ``logs/query`` is
    five times the price of ``whoami``, and ``logs/{id}`` — the parameterised row — is priced the
    same as ``whoami`` no matter which id is in the path.
    """
    settings = protected_app.state.runtime.settings

    assert mounted_v1_routes(protected_app) == {
        ("GET", "/api/v1/whoami"),
        ("GET", "/api/v1/logs/query"),
        ("GET", "/api/v1/logs/{log_id}"),
        ("POST", "/api/v1/logs/ingest"),
    }

    # The report is the artifact — one line per route, in contract order.
    report = verify_route_pricing(protected_app)
    assert len(report) == len(protected.ROUTE_CONTRACT) == len(ROUTE_TABLE)
    assert all("dispatch=" in line for line in report)

    priced = {
        (contract.method, contract.path): settings.endpoint_costs[contract.category]
        for contract in protected.ROUTE_CONTRACT
    }
    assert priced == {
        ("GET", "/api/v1/logs/query"): 5,
        ("POST", "/api/v1/logs/ingest"): 2,
        ("GET", "/api/v1/whoami"): 1,
        ("GET", "/api/v1/logs/{log_id}"): 1,
    }

    # Two different ids collapse onto ONE label — the reason the parameterised row exists at all.
    assert classify("GET", "/api/v1/logs/log-00001") == classify("GET", "/api/v1/logs/log-99999")
    assert classify("GET", "/api/v1/logs/log-00001") == ("GET:/api/v1/logs/{id}", "default")

    # And the sample-path substitution the check relies on does what it claims.
    assert sample_path("/api/v1/logs/{log_id}") == "/api/v1/logs/42"
    assert sample_path("/api/v1/whoami") == "/api/v1/whoami"


def test_a_route_the_table_does_not_price_fails_the_check(app: FastAPI, monkeypatch):
    """Check 1: a served endpoint with no ``ROUTE_TABLE`` row is a startup failure.

    An unpriced metered route is charged ``other``/1 regardless of what it costs to serve, which
    is exactly the silent 80% discount this whole mechanism exists to make impossible.
    """
    monkeypatch.setattr(protected, "ROUTE_CONTRACT", protected.ROUTE_CONTRACT[:-1])

    with pytest.raises(RuntimeError, match="ROUTE_CONTRACT describe different endpoints"):
        verify_route_pricing(app)


def test_a_renamed_route_fails_the_check(app: FastAPI, monkeypatch):
    """Check 2: **the failure this mechanism was built for.**

    Someone renames a path in ``src/api/protected.py`` and does not touch ``ROUTE_TABLE``. Nothing
    about routing breaks — the endpoint serves perfectly — and the classifier silently reprices it
    to ``("other", "default")``. No routing test notices, because routing is fine. This one does,
    because it asks the classifier what the *mounted* path costs rather than what the table says.
    """
    # The label is left alone so check 1 (coverage) still passes and check 2 (pricing) is the one
    # that fires — i.e. this reproduces "the path moved, the table did not" and nothing else.
    renamed = tuple(
        replace(contract, path="/api/v1/whoami-v2")
        if contract.label == "GET:/api/v1/whoami"
        else contract
        for contract in protected.ROUTE_CONTRACT
    )
    monkeypatch.setattr(protected, "ROUTE_CONTRACT", renamed)

    with pytest.raises(RuntimeError, match=r"prices '/api/v1/whoami-v2' as \('other', 'default'\)"):
        verify_route_pricing(app)


def test_declaring_the_wildcard_route_first_fails_the_check(app: FastAPI):
    """Check 3: **the ordering bug that every template-level check is blind to.**

    Starlette matches in declaration order, first full match wins, and ``/logs/{log_id}`` matches
    ``/logs/query`` perfectly well — ``query`` is a fine path segment. Moved above it, every query
    request is dispatched to ``read_log``, misses the store and comes back 404 **while still being
    charged 5 tokens**, because the classifier is a separate regex table that knows nothing about
    the router's order.

    Nothing about the paths, the labels or the prices changes, so checks 1 and 2 pass unmoved.
    Only asking the router which handler it would reach can see it, which is exactly why the check
    resolves each contract row through :func:`~src.api.protected.resolve_route` rather than
    comparing templates.
    """
    routes = app.router.routes
    wildcard = next(route for route in routes if route.path == "/api/v1/logs/{log_id}")
    routes.remove(wildcard)
    routes.insert(0, wildcard)

    # The reordering is real: the router now sends a query request to the wrong handler.
    assert resolve_route(app, "GET", "/api/v1/logs/query").endpoint is read_log

    with pytest.raises(RuntimeError, match="route dispatch mismatch"):
        verify_route_pricing(app)


def test_resolve_route_answers_the_way_starlette_dispatches(app: FastAPI):
    """The three answers the router can give: a handler, a 405 candidate, or nothing.

    ``resolve_route`` is a miniature of ``Router.app``'s matching loop, so its fidelity is what
    check 3 rests on. The PARTIAL case is the one worth pinning: a path that exists under another
    method is *not* "no route", it is the 405 the router would produce, and collapsing the two
    would make the dispatch check quietly unable to tell a missing route from a wrong method.
    """
    assert resolve_route(app, "GET", "/api/v1/whoami").endpoint is whoami
    assert resolve_route(app, "GET", "/api/v1/logs/query").endpoint is query_logs
    assert resolve_route(app, "GET", "/api/v1/logs/log-00001").endpoint is read_log

    # Path matches, method does not -> PARTIAL, i.e. the route that will answer 405.
    assert resolve_route(app, "POST", "/api/v1/whoami").endpoint is whoami

    # Nothing matches at all.
    assert resolve_route(app, "GET", "/definitely/not/a/route") is None


def test_an_undeclared_mounted_route_fails_the_check(app: FastAPI):
    """Check 3: a route added under ``/api/v1`` without a contract row is a startup failure.

    Without this, checks 1 and 2 could be satisfied by a route simply not being looked at — the
    declaration would stay tidy while the application served an unpriced metered endpoint.
    """

    async def stray(request):  # pragma: no cover - never dispatched, only routed over
        raise AssertionError("unreachable")

    app.router.routes.append(Route("/api/v1/stray", stray, methods=["GET"]))

    with pytest.raises(RuntimeError, match="Mounted but undeclared"):
        verify_route_pricing(app)


def test_exempt_and_unversioned_routes_are_outside_the_check(app: FastAPI):
    """``/health`` and the docs paths are not priced, and must not be *required* to be.

    They are exempt from metering entirely, so demanding a ``ROUTE_TABLE`` row for them would be
    asking for a price nobody charges. C10's ``/api/v1/admin/*`` is exempt too and is filtered by
    the single definition of exemption (:func:`src.middleware.is_exempt`) rather than by a second
    list that could drift from it.
    """
    mounted = mounted_v1_routes(app)

    assert not [path for _, path in mounted if path.startswith("/api/v1/admin")]
    assert ("GET", "/health") not in mounted
    assert ("GET", "/openapi.json") not in mounted


# =============================================================================================
# root_path — the divergence no template-level cross-check can see
#
# The middleware takes its path from `starlette.routing.get_route_path(scope)`, which is what the
# router itself matches on. Read `scope["path"]` instead and the two agree only while nothing
# mounts the app under a prefix — and `verify_route_pricing` cannot notice, because it compares
# path TEMPLATES and a template is root_path-independent. These three tests are the only thing
# standing between that and a silent 5x discount.
# =============================================================================================


async def test_a_root_path_mount_does_not_discount_the_expensive_endpoint(
    protected_app: FastAPI, gateway_api: httpx.AsyncClient
):
    """**A 5x pricing bypass, reproduced and shown closed.**

    Mounted under ``/gw``, ``scope["path"]`` is ``/gw/api/v1/logs/query`` while the router matches
    ``/api/v1/logs/query``. A middleware reading the raw path classifies that as
    ``("other", "default")`` — 1 token — and Starlette serves it from the real 5-token handler
    anyway. The caller gets the project's most expensive endpoint at a fifth of the price, the
    charge lands on ``rate_limit:{demo-free}:other``, and the overspend is invisible in the
    metering for the endpoint that was actually used.

    Both halves are asserted, so reverting the middleware to ``scope["path"]`` fails this test
    twice: the price (5, not 1) and the bucket key it was drawn from.
    """
    redis = protected_app.state.runtime.redis.client

    response = await gateway_api.get(
        f"{ROOT_PATH}/api/v1/logs/query", headers=key_headers(Tier.FREE)
    )

    # The expensive handler really ran — this is not a 404 that happens to be cheap.
    assert response.status_code == 200
    assert set(response.json()) == {"items", "page"}
    assert response.json()["page"]["total"] == CORPUS_SIZE

    assert int(response.headers["X-RateLimit-Remaining"]) == FREE_TIER_RPM - 5

    buckets = {key.decode() for key in await redis.keys("rate_limit:*")}
    assert buckets == {"rate_limit:{demo-free}:GET:/api/v1/logs/query"}
    assert "rate_limit:{demo-free}:other" not in buckets


async def test_a_root_path_mount_does_not_break_the_health_exemption(
    gateway_api: httpx.AsyncClient,
):
    """``/gw/health`` must still be exempt, or the container healthcheck starts getting 401s.

    :func:`src.middleware.is_exempt` matches literal path segments, so a raw ``/gw/health`` misses
    every entry in the allowlist and falls through to metering — which, for a probe that carries
    no credential, is a 401. Compose treats a non-200 as "restart this replica", so the limiter
    would take down a replica that is serving perfectly, and it would do it to every replica at
    once. This is the same failure C12's "nginx must not add a path prefix" note describes,
    reached from inside the application rather than from the proxy.
    """
    probe = await gateway_api.get(f"{ROOT_PATH}/health")

    assert probe.status_code == 200
    assert probe.json()["status"] == "healthy"
    # Exempt means unmetered means unadvertised.
    assert not [
        name for name in probe.headers if name.lower().startswith(("x-ratelimit-", "x-quota-"))
    ]


async def test_the_prefixed_and_bare_forms_are_one_endpoint(
    protected_app: FastAPI, api: httpx.AsyncClient, gateway_api: httpx.AsyncClient
):
    """One logical endpoint, one price, one bucket — however the deployment spells the path.

    The stronger claim than "the prefixed form is charged correctly": the two forms are not two
    endpoints. If they drew from different buckets, a caller who could reach the service both ways
    would have two allowances, which is the same per-key overspend the parameterised ``{id}`` row
    exists to prevent.
    """
    redis = protected_app.state.runtime.redis.client
    headers = key_headers(Tier.FREE)

    bare_query = await api.get("/api/v1/logs/query", headers=headers)
    prefixed_query = await gateway_api.get(f"{ROOT_PATH}/api/v1/logs/query", headers=headers)
    bare_whoami = await api.get("/api/v1/whoami", headers=headers)
    prefixed_whoami = await gateway_api.get(f"{ROOT_PATH}/api/v1/whoami", headers=headers)

    responses = (bare_query, prefixed_query, bare_whoami, prefixed_whoami)
    assert [r.status_code for r in responses] == [200, 200, 200, 200]
    # 5, 5, 1, 1 off one shared account-wide window.
    assert [int(r.headers["X-RateLimit-Remaining"]) for r in responses] == [55, 50, 49, 48]

    # The prefixed request is metered against the same principal, under the same label.
    assert prefixed_whoami.json()["user_id"] == "demo-free"
    assert prefixed_whoami.json()["endpoint"] == "GET:/api/v1/whoami"
    assert prefixed_whoami.json()["cost"] == 1

    assert {key.decode() for key in await redis.keys("rate_limit:*")} == {
        "rate_limit:{demo-free}:GET:/api/v1/logs/query",
        "rate_limit:{demo-free}:GET:/api/v1/whoami",
    }


# =============================================================================================
# GET /api/v1/whoami — the clean probe
# =============================================================================================


async def test_whoami_returns_the_documented_shape(api: httpx.AsyncClient):
    """The full body, and the property that the body and the headers cannot disagree.

    ``limits`` is projected straight off the live :class:`~src.models.LimitDecision` rather than
    recomputed, so ``effective_remaining`` in the body is the *same number* the middleware put in
    ``X-RateLimit-Remaining`` — asserted here, because a debug endpoint that reports a different
    allowance than the header is worse than one that reports none.
    """
    response = await api.get("/api/v1/whoami", headers=key_headers(Tier.FREE))

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {
        "user_id",
        "credential",
        "tier",
        "endpoint",
        "cost",
        "metered",
        "limits",
    }
    assert body["user_id"] == "demo-free"
    assert body["credential"] == "api_key"
    assert body["tier"] == "free"
    assert body["endpoint"] == "GET:/api/v1/whoami"
    assert body["cost"] == 1
    assert body["metered"] is True

    limits = body["limits"]
    assert limits["window_limit"] == 60
    assert limits["bucket_limit"] == 60
    assert limits["bucket_remaining"] == 59
    assert limits["effective_remaining"] == 59
    assert limits["daily_limit"] == 1000
    assert limits["daily_remaining"] == 999
    assert limits["monthly_limit"] == 25000
    assert limits["monthly_remaining"] == 24999
    assert limits["degraded"] is False

    assert str(limits["effective_remaining"]) == response.headers["X-RateLimit-Remaining"]
    assert str(limits["daily_remaining"]) == response.headers["X-Quota-Remaining"]
    assert str(limits["daily_reset_at"]) == response.headers["X-Quota-Reset"]


@pytest.mark.parametrize(
    ("tier", "user_id", "rpm", "daily"),
    DEMO_TIERS,
    ids=[tier.value for tier, *_ in DEMO_TIERS],
)
async def test_whoami_reports_the_right_tier_for_each_demo_key(
    api: httpx.AsyncClient, tier: Tier, user_id: str, rpm: int, daily: int
):
    """Each seeded key resolves to its own principal, on its own tier, with its own ceiling.

    The tier is **not** read from the credential: it comes from ``user:{id}`` inside the decision
    script, on every request. That is what makes a tier change take effect on the very next call
    on every replica, and it is why this assertion is worth making through the whole stack rather
    than against the identity resolver alone.
    """
    response = await api.get("/api/v1/whoami", headers=key_headers(tier))

    assert response.status_code == 200
    body = response.json()
    assert body["user_id"] == user_id
    assert body["tier"] == tier.value
    assert body["limits"]["window_limit"] == rpm
    assert body["limits"]["daily_limit"] == daily
    assert int(response.headers["X-RateLimit-Limit"]) == rpm


async def test_whoami_reports_a_jwt_credential_as_jwt(
    api: httpx.AsyncClient, redis_settings: Settings
):
    """The other accepted scheme. Identity from the token, **authority from the store**.

    The token carries no ``tier`` claim and the resolver would ignore one if it did, so the tier
    below is the one recorded in ``user:{demo-free}`` — proving that a self-signed identity cannot
    self-select a limit.
    """
    token = issue_token("demo-free", settings=redis_settings)

    response = await api.get(
        "/api/v1/whoami", headers={"Authorization": f"Bearer {token}"}
    )

    assert response.status_code == 200
    body = response.json()
    assert body["credential"] == "jwt"
    assert body["user_id"] == "demo-free"
    assert body["tier"] == "free"


async def test_whoami_still_answers_with_the_limiter_switched_off(redis_settings: Settings):
    """**The transparency property.** A handler that 500s when the limiter is off is coupled to it.

    With ``RATE_LIMIT_ENABLED=false`` the middleware returns at step 3, above the point where a
    principal is resolved or a decision is stashed. The obvious spelling —
    ``request.state.rlq_decision`` — raises ``AttributeError`` there, so this route would 500 on
    every request in a configuration the project explicitly supports (it is C14's overhead
    baseline). Reading defensively is what makes "available, never mandatory" true.

    Note what is still reported: the classified ``endpoint``, because the middleware classifies
    *above* the switch, and the credential *kind*, because that is a pure header parse. What is
    not reported is anything that would have to be invented — a fabricated allowance is worse than
    a missing one, since a client can detect the second and will pace itself off the first.
    """
    off = redis_settings.model_copy(update={"rate_limit_enabled": False})
    runtime = Runtime.build(off)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()
    app = create_app(runtime=runtime)

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as api:
            # No credential at all: the switch turns off authentication with everything else.
            anonymous = await api.get("/api/v1/whoami")

            assert anonymous.status_code == 200
            body = anonymous.json()
            assert body["metered"] is False
            assert body["limits"] is None
            assert body["user_id"] is None
            assert body["tier"] is None
            assert body["cost"] is None
            assert body["credential"] is None
            assert body["endpoint"] == "GET:/api/v1/whoami"
            # No headers claiming a limit that was never evaluated.
            assert not [n for n in anonymous.headers if n.lower().startswith("x-ratelimit-")]

            # A credential presented anyway is still reported by KIND — the parse is pure, and it
            # is deliberately not a second authentication path: nothing was verified here.
            with_key = await api.get("/api/v1/whoami", headers=key_headers(Tier.FREE))
            assert with_key.status_code == 200
            assert with_key.json()["credential"] == "api_key"
            assert with_key.json()["metered"] is False

            # The other three routes work unmetered too — the stub has no idea the gate exists.
            assert (await api.get("/api/v1/logs/query")).status_code == 200
            assert (await api.get("/api/v1/logs/log-00001")).status_code == 200

        # Nothing was counted either: the switch stops the write as well as the header.
        assert await runtime.redis.client.keys("rate_limit:*") == []
    finally:
        try:
            await runtime.redis.client.flushdb()
        finally:
            await runtime.stop()


# =============================================================================================
# GET /api/v1/logs/query — filtering, clamping, paging
# =============================================================================================


async def test_logs_query_returns_a_paginated_envelope(api: httpx.AsyncClient):
    """An envelope and never a bare array, and the served rows are the deterministic oracle.

    The corpus is seeded from a private ``random.Random`` and anchored to a fixed instant, so the
    entries this API serves can be recomputed *in the test process* and compared row by row. An
    assertion whose expected value came from the server would only prove the server agrees with
    itself.
    """
    response = await api.get("/api/v1/logs/query?limit=5", headers=key_headers(Tier.FREE))

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"items", "page"}
    assert body["page"] == {
        "limit": 5,
        "offset": 0,
        "returned": 5,
        "total": CORPUS_SIZE,
        "has_more": True,
    }

    oracle = generate_corpus()[:5]
    assert [item["id"] for item in body["items"]] == [entry.id for entry in oracle]
    assert [item["level"] for item in body["items"]] == [entry.level.value for entry in oracle]
    assert [item["service"] for item in body["items"]] == [entry.service for entry in oracle]
    assert [item["message"] for item in body["items"]] == [entry.message for entry in oracle]


async def test_logs_query_filters_by_level_and_by_service(api: httpx.AsyncClient):
    """Both filters, separately and together, graded against the corpus rather than a constant.

    Each expected count is computed from :func:`~src.api.protected.generate_corpus`, so the
    assertion stays true if the seed or the weights ever change — and stays *meaningful*, because
    the guards below refuse a filter that matched everything or nothing.
    """
    corpus = generate_corpus()
    errors = [entry for entry in corpus if entry.level is LogLevel.ERROR]
    auth = [entry for entry in corpus if entry.service == "auth-svc"]
    auth_errors = [entry for entry in errors if entry.service == "auth-svc"]

    # A filter that matches all of the corpus, or none of it, would make the test vacuous.
    assert 0 < len(errors) < len(corpus)
    assert 0 < len(auth) < len(corpus)
    assert 0 < len(auth_errors) < len(errors)

    headers = key_headers(Tier.FREE)
    by_level = await api.get("/api/v1/logs/query?level=ERROR&limit=100", headers=headers)
    by_service = await api.get("/api/v1/logs/query?service=auth-svc&limit=100", headers=headers)
    by_both = await api.get(
        "/api/v1/logs/query?level=ERROR&service=auth-svc&limit=100", headers=headers
    )
    no_match = await api.get("/api/v1/logs/query?service=nothing-svc", headers=headers)

    assert by_level.json()["page"]["total"] == len(errors)
    assert by_service.json()["page"]["total"] == len(auth)
    assert by_both.json()["page"]["total"] == len(auth_errors)
    assert {item["level"] for item in by_level.json()["items"]} == {"ERROR"}
    assert {item["service"] for item in by_service.json()["items"]} == {"auth-svc"}

    # An empty match set is an empty page, not a 404: the filter was valid, it just matched
    # nothing, and those are different facts.
    assert no_match.status_code == 200
    assert no_match.json() == {
        "items": [],
        "page": {"limit": 20, "offset": 0, "returned": 0, "total": 0, "has_more": False},
    }


async def test_an_over_large_limit_is_clamped_and_never_rejected(api: httpx.AsyncClient):
    """**Clamp, do not 422.** The server already knows the right answer, so it hands it over.

    Rejecting is defensible in isolation and makes every naive client's first request fail while
    teaching it nothing it could not have been told by simply being given the ceiling. The
    effective value comes back in ``page.limit``, so the caller can see exactly what it got.
    """
    headers = key_headers(Tier.FREE)

    huge = await api.get("/api/v1/logs/query?limit=100000", headers=headers)
    zero = await api.get("/api/v1/logs/query?limit=0", headers=headers)
    negative = await api.get("/api/v1/logs/query?limit=-5", headers=headers)
    exact = await api.get("/api/v1/logs/query?limit=7", headers=headers)

    assert [r.status_code for r in (huge, zero, negative, exact)] == [200, 200, 200, 200]
    assert huge.json()["page"]["limit"] == MAX_PAGE_SIZE
    assert huge.json()["page"]["returned"] == MAX_PAGE_SIZE
    # There is no reading of "give me zero rows" more useful than "give me one".
    assert zero.json()["page"]["limit"] == 1
    assert negative.json()["page"]["limit"] == 1
    assert exact.json()["page"]["limit"] == 7
    assert exact.json()["page"]["returned"] == 7


async def test_offset_pages_through_the_corpus_without_overlap(api: httpx.AsyncClient):
    """Consecutive pages are disjoint and contiguous, and the walk ends honestly.

    ``has_more`` is derived from the *filtered* total, so a client can tell "this is the last
    page" from "your filter matched nothing" without issuing a second request.
    """
    headers = key_headers(Tier.FREE)

    first = await api.get("/api/v1/logs/query?limit=10&offset=0", headers=headers)
    second = await api.get("/api/v1/logs/query?limit=10&offset=10", headers=headers)
    past_the_end = await api.get(
        f"/api/v1/logs/query?limit=10&offset={CORPUS_SIZE}", headers=headers
    )
    negative_offset = await api.get("/api/v1/logs/query?limit=3&offset=-5", headers=headers)

    ids = [entry.id for entry in generate_corpus()]
    assert [item["id"] for item in first.json()["items"]] == ids[0:10]
    assert [item["id"] for item in second.json()["items"]] == ids[10:20]
    assert second.json()["page"]["offset"] == 10
    assert second.json()["page"]["has_more"] is True

    assert past_the_end.json()["page"]["returned"] == 0
    assert past_the_end.json()["page"]["total"] == CORPUS_SIZE
    assert past_the_end.json()["page"]["has_more"] is False

    # A negative offset is floored, not rejected — same reasoning as the limit.
    assert negative_offset.json()["page"]["offset"] == 0
    assert [item["id"] for item in negative_offset.json()["items"]] == ids[0:3]


# =============================================================================================
# GET /api/v1/logs/{id}
# =============================================================================================


async def test_a_single_entry_is_fetchable_by_id(api: httpx.AsyncClient):
    """The row the list endpoint showed is the row the fetch endpoint returns, byte for byte."""
    headers = key_headers(Tier.FREE)
    listed = (await api.get("/api/v1/logs/query?limit=1", headers=headers)).json()["items"][0]

    fetched = await api.get(f"/api/v1/logs/{listed['id']}", headers=headers)

    assert fetched.status_code == 200
    assert fetched.json() == listed


async def test_an_unknown_id_404s_cleanly(api: httpx.AsyncClient):
    """A 404 with the id echoed back — bounded, so the body cannot be used as a reflector.

    It is a 404 and not a 200-with-null: "there is no such entry" is a different fact from "here
    is an entry with nothing in it", and only one of them is what happened.
    """
    response = await api.get("/api/v1/logs/log-does-not-exist", headers=key_headers(Tier.FREE))

    assert response.status_code == 404
    assert "log-does-not-exist" in response.json()["detail"]
    # Still metered: the headers came from the middleware above the router, which never saw the
    # handler's answer.
    assert int(response.headers["X-RateLimit-Remaining"]) == 59


async def test_a_very_long_id_is_echoed_back_truncated(api: httpx.AsyncClient):
    """The 404 body must not reflect arbitrary caller input at arbitrary length."""
    absurd = "z" * 500

    response = await api.get(f"/api/v1/logs/{absurd}", headers=key_headers(Tier.FREE))

    assert response.status_code == 404
    assert len(response.json()["detail"]) < 200
    assert absurd not in response.json()["detail"]


# =============================================================================================
# POST /api/v1/logs/ingest
# =============================================================================================


async def test_ingest_appends_and_the_new_entry_is_retrievable(api: httpx.AsyncClient):
    """A 201, the new id, the resulting store size, and the entry readable straight back."""
    headers = key_headers(Tier.FREE)
    payload = {"level": "ERROR", "service": "payments-svc", "message": "card declined"}

    created = await api.post("/api/v1/logs/ingest", headers=headers, json=payload)

    assert created.status_code == 201
    body = created.json()
    assert set(body) == {"id", "size", "entry"}
    assert body["size"] == CORPUS_SIZE + 1
    assert body["entry"]["level"] == "ERROR"
    assert body["entry"]["service"] == "payments-svc"
    assert body["entry"]["message"] == "card declined"

    fetched = await api.get(f"/api/v1/logs/{body['id']}", headers=headers)
    assert fetched.status_code == 200
    assert fetched.json() == body["entry"]

    # And it is in the filtered view too, so the two read paths see one store.
    listed = await api.get(
        "/api/v1/logs/query?service=payments-svc&limit=100", headers=headers
    )
    assert body["id"] in {item["id"] for item in listed.json()["items"]}


@pytest.mark.parametrize(
    ("payload", "why"),
    [
        ({"level": "NOPE", "service": "svc", "message": "m"}, "unknown level"),
        ({"service": "svc", "message": "m"}, "missing level"),
        ({"level": "INFO", "service": "", "message": "m"}, "empty service"),
        ({"level": "INFO", "service": "svc", "message": "m", "levle": "INFO"}, "misspelled key"),
    ],
    ids=["unknown-level", "missing-level", "empty-service", "extra-key"],
)
async def test_a_malformed_ingest_body_is_a_422(
    api: httpx.AsyncClient, payload: dict[str, str], why: str
):
    """Ordinary pydantic validation, including ``extra="forbid"``.

    The misspelled-key case is the one worth spelling out: an ingest that accepted
    ``{"levle": "ERROR"}`` and stored an INFO line would be the worst kind of success — a caller
    who believes they recorded an error and a store that disagrees.
    """
    response = await api.post("/api/v1/logs/ingest", headers=key_headers(Tier.FREE), json=payload)

    assert response.status_code == 422, why


async def test_a_422_from_the_handler_was_still_metered(
    protected_app: FastAPI, api: httpx.AsyncClient
):
    """**The limiter ran above the router, so the 422 already cost its 2 tokens.**

    This is the ordering made observable. The middleware classifies, authenticates and charges
    *before* Starlette routes and long before pydantic parses a body, so a request the handler
    rejects has already consumed its weight. That is the correct behaviour — a malformed request
    is still work the service did, and a caller who could get unlimited free attempts by sending
    garbage would have found a way to bypass the limiter entirely — but it is invisible unless
    something asserts it.

    Both halves are checked: the allowance moved by the ingest weight (2, not 1), and it moved on
    the **ingest bucket**, which is the key that proves the charge was attributed to the endpoint
    the caller actually hit.
    """
    redis = protected_app.state.runtime.redis.client
    headers = key_headers(Tier.FREE)

    before = await api.get("/api/v1/whoami", headers=headers)
    rejected = await api.post(
        "/api/v1/logs/ingest", headers=headers, json={"level": "NOPE", "service": "s", "m": 1}
    )

    assert rejected.status_code == 422
    assert (
        int(before.headers["X-RateLimit-Remaining"])
        - int(rejected.headers["X-RateLimit-Remaining"])
        == 2
    )
    assert (
        int(before.headers["X-Quota-Remaining"])
        - int(rejected.headers["X-Quota-Remaining"])
        == 2
    )

    buckets = {key.decode() for key in await redis.keys("rate_limit:*")}
    assert "rate_limit:{demo-free}:POST:/api/v1/logs/ingest" in buckets

    # Charged, and correctly not stored: the body never parsed, so nothing was appended.
    still = await api.get("/api/v1/logs/query?limit=1", headers=headers)
    assert still.json()["page"]["total"] == CORPUS_SIZE


# =============================================================================================
# The store itself — determinism and the capacity bound
# =============================================================================================


def test_the_corpus_is_reproducible_and_seed_dependent():
    """Same seed in, byte-identical corpus out; a different seed produces a different one.

    The second half matters as much as the first: a "deterministic" generator that ignored its
    seed would pass the reproducibility check trivially.
    """
    assert generate_corpus(seed=CORPUS_SEED) == generate_corpus(seed=CORPUS_SEED)
    assert generate_corpus(seed=1) != generate_corpus(seed=2)

    corpus = generate_corpus()
    assert len(corpus) == CORPUS_SIZE
    assert [entry.id for entry in corpus] == sorted(entry.id for entry in corpus)
    # Ascending timestamps — insertion order and time order agree, which is what makes
    # offset/limit paging over the untouched corpus stable.
    assert [entry.ts for entry in corpus] == sorted(entry.ts for entry in corpus)


def test_the_store_evicts_the_oldest_entry_at_capacity():
    """The bound is enforced by the store, not by the limiter in front of it.

    ``RATE_LIMIT_ENABLED=false`` is a supported configuration, so "the limiter would stop them
    first" is not a bound. Eviction is explicit rather than ``deque(maxlen=...)`` so the id index
    stays exact — a silent drop would leave ``GET /logs/{id}`` returning a row the list endpoint
    no longer shows.
    """
    store = LogStore(size=3, capacity=4)
    assert len(store) == 3
    assert store.capacity == 4

    fourth = store.append(level=LogLevel.INFO, service="svc", message="fourth")
    assert len(store) == 4
    assert store.get("log-00001") is not None

    fifth = store.append(level=LogLevel.WARN, service="svc", message="fifth")
    assert len(store) == 4
    assert store.get("log-00001") is None
    assert store.get(fourth.id) is fourth
    assert store.get(fifth.id) is fifth

    # Ids are never reused after an eviction: a client's cached row must not silently start
    # referring to a different entry.
    assert (fourth.id, fifth.id) == ("log-00004", "log-00005")
    assert isinstance(fifth, LogEntry)


def test_the_shipped_store_seeds_the_shipped_corpus():
    """The default store is the default corpus, under the shipped capacity."""
    store = LogStore()

    assert len(store) == CORPUS_SIZE
    assert store.capacity == STORE_CAPACITY
    assert store.get("log-00001") == generate_corpus()[0]
    assert store.get("nope") is None
