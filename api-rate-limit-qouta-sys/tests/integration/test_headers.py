"""The rate-limit header contract, measured over HTTP against the real routes and a real Redis.

``tests/unit/test_models.py`` proves :meth:`~src.models.LimitDecision.headers` formats the right
strings from a hand-built decision. ``tests/integration/test_middleware_flow.py`` proves the
middleware attaches them. This file proves the thing neither of those can: that the **numbers on
the wire are the numbers in Redis**, across four routes with three different prices, through the
whole stack a client actually talks to.

Four properties, each of which would be a real, shippable bug if it were false:

* **Weighted cost is charged end to end.** ``X-RateLimit-Remaining`` drops by 5 on
  ``/logs/query``, 2 on ``/logs/ingest`` and 1 on ``/whoami`` and ``/logs/{id}``, and the quota
  counter moves by the same weights from the same script call. Asserted as *deltas* rather than
  absolute values, because a delta is the claim the feature actually makes.
* **Per-principal isolation.** Free is refused at its exact ceiling while premium — at that
  instant, not before or after — still succeeds, and premium's ceiling is exactly five times
  free's. A limiter that refused everybody once one caller misbehaved would be a global rate limit
  with extra steps.
* **A quota rejection never spends a token.** The four gates are read and evaluated *before* any
  mutation, so a request refused by the daily quota leaves the token bucket byte-identical. This
  is C4's cross-gate property, now observable from outside the process.
* **The exemption holds under load.** ``/health`` is still 200, with no limit headers on it, while
  a principal is fully rate limited.

.. rubric:: Why the drain counts can be asserted EXACTLY

The account-wide sliding window is the gate that binds here, and it does not refill: a tier's
``rpm`` is the ceiling for one window, and the counter only moves when a request is *admitted* (a
denial writes nothing). The token bucket does refill continuously, but it can never admit more
than the window allows, so ``allowed`` is exactly ``rpm`` for a drain that completes well inside
one window — which an in-process ``ASGITransport`` drain against a container-local Redis does by
two orders of magnitude. ``<=`` would be a weaker claim that a limiter denying *everything* also
satisfies.
"""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest
from fastapi import FastAPI

from src.config import Settings
from src.identity import DEMO_KEY_BY_TIER
from src.keys import bucket_key, daily_quota_key
from src.main import Runtime, create_app
from src.models import ERROR_QUOTA, ERROR_RATE_LIMIT, Tier

#: The shipped tier table's per-minute ceilings — ``free:60:...`` and ``premium:300:...``.
FREE_TIER_RPM = 60
PREMIUM_TIER_RPM = 300

#: The free tier's daily quota. Seeded straight into Redis by the cross-gate test below.
FREE_TIER_DAILY_QUOTA = 1000

#: Emitted on **every** metered response, admitted or refused. A client cannot pace itself off
#: information it only receives once it has already been cut off.
RATE_HEADERS = ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset")
QUOTA_HEADERS = ("X-Quota-Limit", "X-Quota-Remaining", "X-Quota-Reset")

#: A valid ingest body, so the cost of a ``POST`` is the cost of the route rather than the cost of
#: a validation failure.
INGEST_BODY = {"level": "INFO", "service": "auth-svc", "message": "header contract probe"}


@pytest.fixture()
async def metered_app(redis_settings: Settings):
    """A real app over a real, freshly flushed Redis, with the demo credentials seeded."""
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
async def api(metered_app: FastAPI):
    """An ``httpx`` client speaking ASGI directly to the app — no socket, no server."""
    transport = httpx.ASGITransport(app=metered_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as instance:
        yield instance


def key_headers(tier: Tier) -> dict[str, str]:
    """``X-API-Key`` for one seeded demo principal, read from the declaration the server seeded."""
    return {"X-API-Key": DEMO_KEY_BY_TIER[tier]}


async def drain(api: httpx.AsyncClient, tier: Tier) -> tuple[int, list[httpx.Response]]:
    """Fire ``rpm + 5`` requests at ``/whoami`` as ``tier``; return ``(allowed, refusals)``.

    ``/whoami`` deliberately: it costs 1 token and does no downstream work, so the count is a
    statement about the limiter rather than about the handler. Sequential rather than concurrent,
    so "how many got through" is a statement about the *limits* rather than about the event loop —
    the concurrent version, which proves atomicity, lives in the Lua suite.
    """
    ceiling = {Tier.FREE: FREE_TIER_RPM, Tier.PREMIUM: PREMIUM_TIER_RPM}[tier]
    allowed = 0
    refusals: list[httpx.Response] = []
    for _ in range(ceiling + 5):
        response = await api.get("/api/v1/whoami", headers=key_headers(tier))
        if response.status_code == 200:
            allowed += 1
        else:
            refusals.append(response)
    return allowed, refusals


# =============================================================================================
# The header set, on both outcomes
# =============================================================================================


async def test_the_full_header_set_is_present_on_a_200_and_on_a_429(api: httpx.AsyncClient):
    """Six headers on the admitted path, seven on the refused one.

    ``Retry-After`` is the only difference, and it is present only where it means something: on a
    200 there is nothing to wait for, and a ``Retry-After`` on a successful response would be an
    instruction a client cannot act on.
    """
    admitted = await api.get("/api/v1/whoami", headers=key_headers(Tier.FREE))

    assert admitted.status_code == 200
    for name in RATE_HEADERS + QUOTA_HEADERS:
        assert name in admitted.headers, name
    assert "Retry-After" not in admitted.headers

    _, refusals = await drain(api, Tier.FREE)
    refused = refusals[0]

    assert refused.status_code == 429
    for name in RATE_HEADERS + QUOTA_HEADERS:
        assert name in refused.headers, name
    assert refused.headers["X-RateLimit-Remaining"] == "0"
    assert int(refused.headers["Retry-After"]) >= 1


async def test_every_refusal_carries_a_usable_backoff_and_the_spec_error_string(
    api: httpx.AsyncClient,
):
    """``Retry-After >= 1`` on **every** 429, and the body's ``error`` is the spec literal.

    The floor is the whole point. ``Retry-After: 0`` tells a client to retry immediately; it is
    refused again, told ``0`` again, and the backoff the header exists to produce becomes a retry
    storm the limiter manufactured against itself — worst precisely when the service is already
    shedding load. Rounding up costs a caller at most 999 ms of extra patience.

    ``"Rate limit exceeded"`` is asserted character for character because C13's verifier does the
    same across a container boundary, where a "harmless" rewording would fail a check no Python in
    this repository would catch first.
    """
    _, refusals = await drain(api, Tier.FREE)

    assert len(refusals) >= 5
    for refused in refusals:
        assert refused.status_code == 429
        assert int(refused.headers["Retry-After"]) >= 1

        body = refused.json()
        assert body["error"] == ERROR_RATE_LIMIT
        # Which of the two *rate* gates refused is a real distinction, and both are correct here:
        # this tier's burst and rpm are deliberately the same number.
        assert body["reason"] in {"rate_limit", "sliding_window"}
        assert body["retry_after"] == int(refused.headers["Retry-After"])
        assert body["retry_after"] >= 1


# =============================================================================================
# Weighted cost — the in-scope bonus, measured end to end
# =============================================================================================


async def test_weighted_cost_is_charged_end_to_end(metered_app: FastAPI, api: httpx.AsyncClient):
    """**5 / 2 / 1 / 1, measured as deltas on the wire and confirmed in Redis.**

    A read that fans out across a log store is not the same unit of work as a whoami, and charging
    both one token prices the expensive call as though it were free.

    ``X-RateLimit-Remaining`` reports the *binding* gate. The token buckets are per
    ``(user, endpoint)`` and therefore separate for these four paths, while the account-wide
    sliding window counts every request the account makes — which is exactly why a delta is
    readable *across* different endpoints at all, and why the same weight shows up in the quota
    counter from the same script call.
    """
    redis = metered_app.state.runtime.redis.client
    headers = key_headers(Tier.FREE)

    first = await api.get("/api/v1/whoami", headers=headers)
    second = await api.get("/api/v1/whoami", headers=headers)
    query = await api.get("/api/v1/logs/query", headers=headers)
    single = await api.get("/api/v1/logs/log-00001", headers=headers)
    ingest = await api.post("/api/v1/logs/ingest", headers=headers, json=INGEST_BODY)

    assert [r.status_code for r in (first, second, query, single)] == [200, 200, 200, 200]
    assert ingest.status_code == 201

    responses = (first, second, query, single, ingest)
    remaining = [int(r.headers["X-RateLimit-Remaining"]) for r in responses]
    quota = [int(r.headers["X-Quota-Remaining"]) for r in responses]

    # whoami -> whoami (1), whoami -> logs/query (5), logs/query -> logs/{id} (1),
    # logs/{id} -> logs/ingest (2).
    assert [a - b for a, b in zip(remaining, remaining[1:])] == [1, 5, 1, 2]
    assert [a - b for a, b in zip(quota, quota[1:])] == [1, 5, 1, 2]

    # The quota counter in Redis moved by the same total, from the same atomic script call —
    # read from the store rather than inferred from the header it produced.
    daily = daily_quota_key("demo-free", datetime.now(timezone.utc))
    assert int(await redis.get(daily)) == 1 + 1 + 5 + 1 + 2

    # Four routes, four separate token buckets, each named by its CLASSIFIED label.
    buckets = {key.decode() for key in await redis.keys("rate_limit:*")}
    assert buckets == {
        "rate_limit:{demo-free}:GET:/api/v1/whoami",
        "rate_limit:{demo-free}:GET:/api/v1/logs/query",
        "rate_limit:{demo-free}:GET:/api/v1/logs/{id}",
        "rate_limit:{demo-free}:POST:/api/v1/logs/ingest",
    }

    # A second, different id collapses onto the SAME bucket — the parameterised label is what
    # bounds the key space at len(ROUTE_TABLE) + 1 per user instead of at "URLs a caller can
    # invent", each arriving with a full allowance.
    other = await api.get("/api/v1/logs/log-00002", headers=headers)
    assert other.status_code == 200
    assert {key.decode() for key in await redis.keys("rate_limit:*")} == buckets


async def test_the_expensive_endpoint_drains_five_times_faster(api: httpx.AsyncClient):
    """The weight is not cosmetic: 5-token calls exhaust the same ceiling in a fifth of the calls.

    ``60 / 5 == 12``, so the thirteenth ``/logs/query`` must be refused while a caller spending
    the same minute on ``/whoami`` would still have 48 requests left.
    """
    headers = key_headers(Tier.FREE)

    allowed = 0
    refused: httpx.Response | None = None
    for _ in range(FREE_TIER_RPM // 5 + 3):
        response = await api.get("/api/v1/logs/query", headers=headers)
        if response.status_code == 200:
            allowed += 1
        else:
            refused = response
            break

    assert allowed == FREE_TIER_RPM // 5
    assert refused is not None
    assert refused.status_code == 429
    assert refused.json()["error"] == ERROR_RATE_LIMIT


# =============================================================================================
# Per-principal isolation and the tier ladder
# =============================================================================================


async def test_free_is_refused_at_its_ceiling_while_premium_still_succeeds(
    api: httpx.AsyncClient,
):
    """**Per-principal isolation, asserted at the instant it matters.**

    A limiter that refused everybody once one caller misbehaved would be a global rate limit with
    extra steps. The bucket key carries the user id (``rate_limit:{demo-free}:...``) and so does
    the sliding-window key, so draining one principal's allowance is invisible to every other —
    checked *while* the first one is being refused, not before or after.
    """
    allowed, refusals = await drain(api, Tier.FREE)

    assert allowed == FREE_TIER_RPM
    assert refusals

    premium = await api.get("/api/v1/whoami", headers=key_headers(Tier.PREMIUM))

    assert premium.status_code == 200
    assert int(premium.headers["X-RateLimit-Limit"]) == PREMIUM_TIER_RPM
    assert int(premium.headers["X-RateLimit-Remaining"]) == PREMIUM_TIER_RPM - 1

    # ...and free is still refused, i.e. the premium call did not reset anything.
    assert (await api.get("/api/v1/whoami", headers=key_headers(Tier.FREE))).status_code == 429


async def test_premium_admits_exactly_five_times_what_free_admits(api: httpx.AsyncClient):
    """The tier ladder, as counts rather than as "more".

    "Premium gets more" is satisfied by 61. The claim the tier table actually makes is 300 against
    60, and only the exact numbers distinguish a working ladder from a limiter that happens to be
    lenient. See the module docstring for why an exact count is safe to assert here.
    """
    free_allowed, free_refusals = await drain(api, Tier.FREE)
    premium_allowed, premium_refusals = await drain(api, Tier.PREMIUM)

    assert free_allowed == FREE_TIER_RPM
    assert premium_allowed == PREMIUM_TIER_RPM
    assert premium_allowed == 5 * free_allowed
    assert len(free_refusals) == len(premium_refusals) == 5

    # Both ceilings are advertised on the wire, so a client never has to discover one by hitting it.
    assert int(free_refusals[0].headers["X-RateLimit-Limit"]) == FREE_TIER_RPM
    assert int(premium_refusals[0].headers["X-RateLimit-Limit"]) == PREMIUM_TIER_RPM


# =============================================================================================
# The cross-gate property: a quota rejection costs no token
# =============================================================================================


async def test_a_quota_rejection_does_not_spend_a_token(
    metered_app: FastAPI, api: httpx.AsyncClient
):
    """**C4's cross-gate property, now observable over HTTP.**

    The decision script reads and evaluates all four gates *before* it mutates anything, precisely
    so a request the quota refuses cannot also be charged a token. Two scripts — or one that
    consumed as it went — would debit the bucket and then discover the quota was exhausted, and
    the caller would pay for requests they were never served. There is no ordering that avoids
    that without a distributed compensating write.

    The daily counter is exhausted **out of band**, by writing it directly: no admin API exists
    until C10, and going through the limiter to exhaust the quota would spend 1000 tokens on the
    way and destroy the very thing being measured. What matters is only that the next request
    arrives with the quota gate already failing and the bucket untouched by the setup.
    """
    redis = metered_app.state.runtime.redis.client
    headers = key_headers(Tier.FREE)

    # One admitted request, so the bucket exists and holds a value worth comparing.
    warmup = await api.get("/api/v1/whoami", headers=headers)
    assert warmup.status_code == 200

    bucket = bucket_key("demo-free", "GET:/api/v1/whoami")
    tokens_before = await redis.hget(bucket, "t")
    assert tokens_before is not None

    daily = daily_quota_key("demo-free", datetime.now(timezone.utc))
    await redis.set(daily, FREE_TIER_DAILY_QUOTA)

    denied = await api.get("/api/v1/whoami", headers=headers)

    assert denied.status_code == 429
    body = denied.json()
    assert body["reason"] == "quota_daily"
    # A quota refusal is still a 429 — the spec names it, and every HTTP client already has retry
    # behaviour attached to it — but the *error string* says which kind of exhaustion it was.
    assert body["error"] == ERROR_QUOTA
    assert body["quota"]["daily"]["state"] == "exhausted"
    assert body["quota"]["daily"]["remaining"] == 0
    # Hours away, not seconds: telling a caller who is out of quota to retry in 3 seconds means
    # refusing them again for the rest of the day.
    assert int(denied.headers["Retry-After"]) >= 1

    assert await redis.hget(bucket, "t") == tokens_before
    assert int(await redis.get(daily)) == FREE_TIER_DAILY_QUOTA

    # A flood of refused requests performs zero writes — not merely the first one.
    for _ in range(5):
        assert (await api.get("/api/v1/whoami", headers=headers)).status_code == 429
    assert await redis.hget(bucket, "t") == tokens_before
    assert int(await redis.get(daily)) == FREE_TIER_DAILY_QUOTA

    # The expensive endpoint is refused by the same gate without touching its bucket either — a
    # quota is account-wide, so it cannot be worked around by moving to another endpoint.
    expensive = await api.get("/api/v1/logs/query", headers=headers)
    assert expensive.status_code == 429
    assert expensive.json()["reason"] == "quota_daily"
    assert await redis.exists(bucket_key("demo-free", "GET:/api/v1/logs/query")) == 0


# =============================================================================================
# The exemption, under load
# =============================================================================================


async def test_health_is_still_200_while_a_principal_is_fully_limited(api: httpx.AsyncClient):
    """**The exemption is load-bearing, not cosmetic.**

    The container ``HEALTHCHECK`` polls ``/health`` every 10 s from one source address and compose
    restarts the replica on a non-200. Metered, this probe would 429 during exactly the traffic
    the limiter exists to shed — so the limiter would take a healthy replica down by working
    correctly. It is unauthenticated too, so metering it would 401 the healthcheck long before it
    ever 429'd.

    Drained here through ``/logs/query`` rather than ``/whoami`` so the exhaustion arrives via the
    weighted path as well: the exemption must survive a caller who is out of allowance, however
    they spent it.
    """
    headers = key_headers(Tier.FREE)
    for _ in range(FREE_TIER_RPM // 5 + 2):
        await api.get("/api/v1/logs/query", headers=headers)

    assert (await api.get("/api/v1/logs/query", headers=headers)).status_code == 429

    probe = await api.get("/health")

    assert probe.status_code == 200
    assert probe.json()["status"] == "healthy"
    assert probe.json()["rate_limiter"] == "active"
    # Unmetered means unadvertised: no counter was consulted, so there is no number to report.
    for name in RATE_HEADERS + QUOTA_HEADERS + ("Retry-After",):
        assert name not in probe.headers, name
