"""Degradation end to end: a real app, a real Redis, and a real outage in the middle of it.

``tests/unit/test_degradation.py`` proves the policy against injected exceptions. This file proves
the thing those injections stand in for — a whole application, with a real
:class:`~src.identity.IdentityResolver`, a real :class:`~src.limiter.Limiter` and the real
middleware, serving requests while the store it depends on is genuinely unreachable, and then
recovering when it comes back.

.. rubric:: How the outage is produced, and why it is the real one

:func:`cut_redis` closes the gateway and reopens it against a **port nothing is listening on**, so
every command gets a real ``ECONNREFUSED`` from the kernel — which is exactly what
``docker compose stop redis`` produces, and exactly what the plan's manual verification does by
hand. :func:`restore_redis` points it back at the live server. Nothing is stubbed, nothing is
monkeypatched, and the circuit breaker, the connection pool and the classification all run for
real.

.. rubric:: Read the credential each test uses — it is the subject, not scenery

The C5 verification's central point is that "we could not check your limits" and "we could not
establish who you are" are different failures. That difference is *observable on the wire here*,
and the two credential forms are how:

* a **JWT** is verified with one HMAC over bytes already in memory and touches no Redis, so a
  Bearer caller keeps authenticating straight through the outage and is metered by the local
  fallback — a degraded **200**;
* an **API key** must be looked up in Redis, so an API-key caller cannot be identified at all and
  is refused with a **503**. Serving them would be admitting a request from a principal that was
  never established.

:func:`test_a_jwt_is_served_while_an_api_key_is_refused_in_the_same_outage` asserts both halves in
one test, against one app, in one outage, because the pair *is* the property.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from fastapi import FastAPI

from src.api.health import (
    POOL_OK,
    RATE_LIMITER_ACTIVE,
    RATE_LIMITER_DEGRADED,
    REDIS_OK,
    REDIS_UNREACHABLE,
    STATUS_HEALTHY,
)
from src.config import Settings
from src.identity import DEMO_KEY_BY_TIER, issue_token
from src.main import Runtime, create_app
from src.middleware import SERVICE_UNAVAILABLE_ERROR
from src.models import (
    DEGRADED_HEADER,
    QUOTA_LIMIT_HEADER,
    QUOTA_REMAINING_HEADER,
    QUOTA_RESET_HEADER,
    RATELIMIT_LIMIT_HEADER,
    Tier,
)

#: A port nothing listens on inside the test container. Connection is *refused* rather than
#: blackholed, which is what a stopped `redis` container produces — the failure this file is about.
DEAD_URL = "redis://127.0.0.1:6390/0"

#: A **blackholed** address: 192.0.2.1 is TEST-NET-1 (RFC 5737), reserved for documentation and
#: guaranteed never to be routed. The SYN is swallowed — no RST, no ICMP — so a connect can only
#: end when a timeout fires.
#:
#: Used by exactly one test, and it has to be. A refused port answers in microseconds, so a timing
#: assertion against one measures the kernel rather than the circuit breaker and would pass with the
#: breaker deleted. Only a blackhole makes "this request did not dial" observable as elapsed time.
BLACKHOLE_URL = "redis://192.0.2.1:6379/0"

#: The metered probe. Cost 1, so a burst count is a request count rather than a weighted total.
WHOAMI = "/api/v1/whoami"

#: The three headers that must vanish while degraded and come back on recovery.
QUOTA_HEADERS = (QUOTA_LIMIT_HEADER, QUOTA_REMAINING_HEADER, QUOTA_RESET_HEADER)


# =============================================================================================
# Fixtures
# =============================================================================================


async def build_app(settings: Settings) -> tuple[FastAPI, Runtime]:
    """A real app over a real, freshly flushed Redis, with the demo credentials seeded.

    ``create_app(runtime=...)`` skips the FastAPI lifespan by design, so this takes on the
    lifespan's two jobs explicitly. The flush happens **between** connecting and seeding: seeding
    first would have the flush delete the ``apikey:v1:*`` records every assertion below
    authenticates against.
    """
    runtime = Runtime.build(settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()
    return create_app(runtime=runtime), runtime


async def cut_redis(runtime: Runtime) -> None:
    """Take the store away for real: reopen the gateway against a refused port.

    The identity cache is cleared at the same time, and that is not tidying — it is what makes the
    outage *observable* on the API-key path. Entries live for ``IDENTITY_CACHE_TTL_SEC`` (5 s), so
    a key resolved a moment ago would still authenticate from memory and the test would be
    asserting about a cache hit rather than about an unreachable store.
    """
    await runtime.redis.aclose()
    runtime.redis.settings = runtime.settings.model_copy(update={"redis_url": DEAD_URL})
    await runtime.redis.connect()
    runtime.identity.clear()


async def restore_redis(runtime: Runtime) -> None:
    """Give it back. The breaker recovers through its own half-open probe, not by being reset."""
    await runtime.redis.aclose()
    runtime.redis.settings = runtime.settings
    await runtime.redis.connect()


@pytest.fixture()
async def degradable(redis_settings: Settings):
    """An app whose Redis can be cut and restored mid-test.

    ``breaker_cooldown_sec=0`` so recovery is observable in one request rather than after a
    five-second sleep: the first call past the outage is the breaker's single half-open probe, it
    succeeds, and the breaker closes. That is the **real** recovery machinery running, just with a
    cooldown short enough to assert on — no counter is poked and no state is reset by hand.
    """
    settings = redis_settings.model_copy(update={"breaker_cooldown_sec": 0})
    app, runtime = await build_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            await restore_redis(runtime)
            try:
                await runtime.redis.client.flushdb()
            finally:
                await runtime.stop()


@pytest.fixture()
async def dead(redis_settings: Settings):
    """An app that has never been able to reach Redis, with the **shipped** breaker cooldown.

    Separate from :func:`degradable` because the cooldown is the subject of the timing test: with
    the shipped 5 s the breaker actually stays open, which is what makes a flood of degraded
    requests cost nothing. A zero cooldown would half-open on every call and re-dial every time.
    """
    settings = redis_settings.model_copy(update={"redis_url": DEAD_URL})
    runtime = Runtime.build(settings)
    await runtime.start()
    transport = httpx.ASGITransport(app=create_app(runtime=runtime))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            await runtime.stop()


@pytest.fixture()
async def blackhole(redis_settings: Settings):
    """An app pointed at an address that swallows packets, with the shipped breaker cooldown.

    The only fixture whose failure *shape* matters rather than its existence: see
    :data:`BLACKHOLE_URL`.
    """
    settings = redis_settings.model_copy(update={"redis_url": BLACKHOLE_URL})
    runtime = Runtime.build(settings)
    await runtime.start()
    transport = httpx.ASGITransport(app=create_app(runtime=runtime))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            await runtime.stop()


@pytest.fixture()
async def dead_closed(redis_settings: Settings):
    """The same, configured to fail **closed** — the "the limit IS the security control" case."""
    settings = redis_settings.model_copy(
        update={"redis_url": DEAD_URL, "fail_mode": "closed"}
    )
    runtime = Runtime.build(settings)
    await runtime.start()
    transport = httpx.ASGITransport(app=create_app(runtime=runtime))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            await runtime.stop()


def bearer(settings: Settings, user_id: str = "degraded-user") -> dict[str, str]:
    """Headers for a JWT caller — the credential that needs no Redis to be established."""
    return {"Authorization": f"Bearer {issue_token(user_id, settings=settings)}"}


def api_key() -> dict[str, str]:
    """Headers for an API-key caller — the credential that requires a Redis lookup."""
    return {"X-API-Key": DEMO_KEY_BY_TIER[Tier.FREE]}


# =============================================================================================
# 1. The service keeps serving
# =============================================================================================


async def test_the_service_keeps_serving_with_redis_unreachable(dead, redis_settings):
    """**200, not 500.** The spec's graceful degradation, asserted through the whole stack.

    An unhandled ``BackingStoreUnavailable`` would surface here as a 500 on every metered request —
    the service falling over because its cache-shaped dependency did. Instead the request is served
    by the local bucket and says so on the wire, which is the difference between a degradation and
    an outage.
    """
    client, _runtime = dead

    response = await client.get(WHOAMI, headers=bearer(redis_settings))

    assert response.status_code == 200
    assert response.headers[DEGRADED_HEADER] == "1"


async def test_every_quota_header_is_absent_while_degraded_and_back_after_recovery(
    degradable, redis_settings
):
    """**The full arc**, in one test, because the interesting part is the transition.

    Healthy: all three ``X-Quota-*`` headers, from real counters in Redis. Degraded: none of them,
    because no counter was consulted and a fabricated quota number is worse than a missing one — a
    client can detect an absent header and cannot detect a wrong one. Recovered: back, and the
    degraded marker gone.
    """
    client, runtime = degradable
    headers = api_key()

    healthy = await client.get(WHOAMI, headers=headers)
    assert healthy.status_code == 200
    assert all(name in healthy.headers for name in QUOTA_HEADERS)
    assert DEGRADED_HEADER not in healthy.headers

    await cut_redis(runtime)
    degraded = await client.get(WHOAMI, headers=bearer(redis_settings))
    assert degraded.status_code == 200
    assert degraded.headers[DEGRADED_HEADER] == "1"
    assert not [name for name in QUOTA_HEADERS if name in degraded.headers]

    await restore_redis(runtime)
    recovered = await client.get(WHOAMI, headers=headers)
    assert recovered.status_code == 200
    assert DEGRADED_HEADER not in recovered.headers
    assert all(name in recovered.headers for name in QUOTA_HEADERS)


async def test_recovery_resumes_real_enforcement_rather_than_only_dropping_the_header(
    degradable, redis_settings
):
    """The header disappearing is the *symptom*; the assertion is that Redis is deciding again.

    A limiter that cleared its degraded flag but kept answering from the local bucket would look
    recovered and enforce per-replica limits forever. So the recovered response is checked for a
    quantity only the shared store can produce: the tier's full 60, rather than the replica share
    the fallback was enforcing a moment earlier.
    """
    client, runtime = degradable

    await cut_redis(runtime)
    degraded = await client.get(WHOAMI, headers=bearer(redis_settings))
    # ceil(60 / API_REPLICAS=2) — this replica's share, which is NOT the tier's number.
    assert degraded.headers[RATELIMIT_LIMIT_HEADER] == "30"
    assert runtime.limiter.degraded is True

    await restore_redis(runtime)
    recovered = await client.get(WHOAMI, headers=api_key())

    assert recovered.headers[RATELIMIT_LIMIT_HEADER] == "60"
    assert runtime.limiter.degraded is False
    assert runtime.limiter.degraded_checks >= 1  # the record of the outage is kept, not erased


# =============================================================================================
# 2. Identity is refused, not passed through
# =============================================================================================


async def test_an_api_key_caller_gets_a_503_and_never_reaches_the_handler(dead):
    """**The authentication-bypass guard, over HTTP.**

    The store that holds ``apikey:v1:*`` is unreachable, so nothing is known about this caller.
    Passing them through would serve an unauthenticated request to anyone holding any string, for
    as long as the outage lasts — and the outage is one an unauthenticated attacker can provoke,
    because identity resolution runs pre-auth on the shared pool. 503 is the only answer that is
    not a bypass.
    """
    client, _runtime = dead

    response = await client.get(WHOAMI, headers=api_key())

    assert response.status_code == 503
    assert int(response.headers["retry-after"]) >= 1
    # The handler was never reached: this is the middleware's own body, not `/whoami`'s.
    body = response.json()
    assert body["error"] == SERVICE_UNAVAILABLE_ERROR
    assert "user_id" not in body
    # No fabricated allowance either — no gate was evaluated, so no number is published.
    assert not [name for name in response.headers if name.lower().startswith("x-ratelimit-")]


async def test_a_jwt_is_served_while_an_api_key_is_refused_in_the_same_outage(
    dead, redis_settings
):
    """**Both halves of the C8 identity decision, in one outage, on one app.**

    This is the "strictly better than 503-ing everyone" property made concrete: the credential form
    that needs no Redis keeps working (degraded, and marked as such), and only the form that
    genuinely cannot be resolved is refused. Refusing both would have been simpler and would have
    taken every Bearer caller down for a lookup they never needed.
    """
    client, _runtime = dead

    with_jwt = await client.get(WHOAMI, headers=bearer(redis_settings))
    with_key = await client.get(WHOAMI, headers=api_key())

    assert with_jwt.status_code == 200
    assert with_jwt.headers[DEGRADED_HEADER] == "1"
    assert with_key.status_code == 503


async def test_a_missing_credential_is_still_a_401_during_an_outage(dead):
    """An outage must not upgrade "you sent nothing" into "we could not check".

    Nothing was asked of Redis for this request — `parse_credential` found no credential at all —
    so answering 503 would be blaming the store for the caller's own omission, and would hide a
    plain misconfiguration behind an incident.
    """
    client, _runtime = dead

    response = await client.get(WHOAMI)

    assert response.status_code == 401
    assert "www-authenticate" in response.headers


# =============================================================================================
# 3. FAIL_MODE=closed
# =============================================================================================


async def test_fail_mode_closed_refuses_with_a_503_and_a_retry_after(
    dead_closed, redis_settings
):
    """503, **not** 429, and never a 200. The deployment where the limit is the security control.

    429 would tell the caller they are over their limit. They are not — the limit could not be
    checked, and saying otherwise would have every well-behaved client back off against a ceiling
    it never hit. The degraded marker rides along because this refusal *is* the degraded policy in
    force, which is what distinguishes it from a pool-exhaustion 503.
    """
    client, runtime = dead_closed

    response = await client.get(WHOAMI, headers=bearer(redis_settings))

    assert response.status_code == 503
    assert int(response.headers["retry-after"]) >= 1
    assert response.headers[DEGRADED_HEADER] == "1"
    assert response.json()["error"] == SERVICE_UNAVAILABLE_ERROR
    assert runtime.limiter.fail_closed_denials == 1
    assert runtime.limiter.degraded_checks == 0


async def test_fail_mode_closed_still_answers_health_with_a_200(dead_closed, redis_settings):
    """Fail-closed refuses *requests*. It must not refuse the liveness probe with them.

    A probe that went red here would have the orchestrator restart a replica that is enforcing
    exactly the policy it was configured with — and restart every replica at once, since they share
    the one Redis that is down.
    """
    client, _runtime = dead_closed
    await client.get(WHOAMI, headers=bearer(redis_settings))

    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == STATUS_HEALTHY


# =============================================================================================
# 4. /health
# =============================================================================================


async def test_health_reports_degraded_while_staying_healthy_and_200(degradable, redis_settings):
    """``rate_limiter: "degraded"``, ``status: "healthy"``, **HTTP 200** — all three at once.

    The one combination that matters. `/health` is read by the container HEALTHCHECK, by compose's
    `condition: service_healthy` and by nginx's upstream check, and every one of them treats a
    non-200 as "restart this replica". A degraded replica is serving every request correctly
    through a bounded bucket; turning the probe red would restart it for working as designed.
    """
    client, runtime = degradable
    healthy = (await client.get("/health")).json()
    assert healthy["rate_limiter"] == RATE_LIMITER_ACTIVE
    assert healthy["redis"] == REDIS_OK

    await cut_redis(runtime)
    await client.get(WHOAMI, headers=bearer(redis_settings))  # put the fallback under load

    response = await client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["rate_limiter"] == RATE_LIMITER_DEGRADED
    assert body["status"] == STATUS_HEALTHY
    # Reported in its own field, so an operator can see WHY without inferring it.
    assert body["redis"] == REDIS_UNREACHABLE
    # ...and the pool is fine: this is an outage, not local backpressure. Two different incidents,
    # two different fields, and this is the one asserting they cannot be confused.
    assert body["pool"] == POOL_OK


async def test_health_returns_to_active_once_redis_decides_again(degradable, redis_settings):
    """The flag is cleared by a real decision, not by the probe's ping succeeding.

    `/health` answers "is enforcement authoritative?", and a store that is answering does not mean
    a request has been metered against it since. So the field flips on the first metered request
    after recovery, not on the first successful PING.
    """
    client, runtime = degradable
    await cut_redis(runtime)
    await client.get(WHOAMI, headers=bearer(redis_settings))
    assert (await client.get("/health")).json()["rate_limiter"] == RATE_LIMITER_DEGRADED

    await restore_redis(runtime)
    # The ping alone reports the store as reachable...
    assert (await client.get("/health")).json()["redis"] == REDIS_OK
    assert (await client.get("/health")).json()["rate_limiter"] == RATE_LIMITER_DEGRADED

    # ...and one real decision is what clears it.
    await client.get(WHOAMI, headers=api_key())

    assert (await client.get("/health")).json()["rate_limiter"] == RATE_LIMITER_ACTIVE


async def test_health_is_never_metered_or_authenticated_during_an_outage(dead):
    """The exemption has to survive the failure, or the healthcheck 503s the replica to death."""
    client, _runtime = dead

    response = await client.get("/health")

    assert response.status_code == 200
    assert not [name for name in response.headers if name.lower().startswith("x-ratelimit-")]


# =============================================================================================
# 5. Enforcement still enforces, and it costs nothing
# =============================================================================================


async def test_the_degraded_bucket_enforces_the_replica_share_and_then_429s(
    dead, redis_settings
):
    """Degraded is not unmetered. 30 through, then refusals — from **one** replica.

    ``ceil(60 / 2)``: two replicas each admitting the tier's full 60 would be the 120 this project
    exists to prevent, so the fallback deliberately enforces a fraction. The refusal is a **429**,
    not a 503, because the local bucket genuinely decided — the caller really is over the limit
    this replica is enforcing.
    """
    client, runtime = dead
    headers = bearer(redis_settings, user_id="burst-user")

    statuses = [
        (await client.get(WHOAMI, headers=headers)).status_code for _ in range(45)
    ]

    assert statuses.count(200) == 30
    assert statuses.count(429) == 15
    assert 503 not in statuses
    assert runtime.limiter.degraded_checks == 45


async def test_the_degraded_ceiling_is_account_wide_across_real_endpoints(dead, redis_settings):
    """**The 5x, asserted over the real HTTP surface rather than over the limiter.**

    Every other degradation test in this file hits ``/whoami`` and nothing else, which is exactly
    why a fallback that reproduced only the per-``(user, endpoint)`` bucket looked correct: on one
    endpoint, the bucket *is* the ceiling. Spread one principal's traffic across the routes the app
    actually serves and a bucket-only fallback hands out one allowance per route — measured at 150
    weighted units per replica against a tier that says 60.

    Driven through three genuinely different handlers with three different **weighted costs**
    (``/whoami`` 1, ``/logs/ingest`` 2, ``/logs/query`` 5), because the account-wide gate counts
    weighted units and a test using cost-1 requests everywhere would not notice a gate that
    ignored the weighting. The assertion is on the units spent, not on the request count.
    """
    client, runtime = dead
    headers = bearer(redis_settings, user_id="fanout-user")
    # (path, method, weighted cost) — the costs are ENDPOINT_COSTS', not this test's invention.
    routes = (
        (WHOAMI, "GET", 1),
        ("/api/v1/logs/ingest", "POST", 2),
        ("/api/v1/logs/query", "GET", 5),
    )

    spent = 0
    for index in range(120):
        path, method, cost = routes[index % len(routes)]
        response = await client.request(method, path, headers=headers, json={"lines": []})
        assert response.status_code in {200, 201, 422, 429}
        if response.status_code != 429:
            spent += cost

    # ceil(60 / 2) = 30 weighted units for this replica, whatever mix of endpoints produced them.
    assert spent == 30
    # ...and it really was the degraded path that decided every one of them.
    assert runtime.limiter.degraded_checks == 120
    # One account gate for the caller plus one bucket per endpoint they touched — three, not one
    # allowance each. The account gate is the entry that made the total 30 rather than 90.
    assert f"sw:{{fanout-user}}" in runtime.limiter._fallback


async def test_a_flood_against_an_unreachable_redis_costs_less_than_one_dial(
    blackhole, redis_settings
):
    """**A timing assertion, because correctness alone would hide the failure this is about.**

    Without the breaker, every degraded request first waits the full ``REDIS_TIMEOUT_MS`` on a
    socket that will never answer. The *answers* are identical either way — which is exactly why a
    test that only checked status codes would pass while the service sat at 250 ms p99 with 250
    coroutines parked on a dead socket at 1000 rps, each holding a pooled connection, and
    stampeded a recovering Redis with the whole backlog at once.

    So this runs against a **blackholed** address (RFC 5737 TEST-NET-1: the SYN is swallowed, no
    RST, no ICMP) rather than a refused port. A refused port answers in microseconds, so a timing
    assertion against one would pass whether the breaker existed or not — it would be measuring the
    kernel, not us.

    The warm-up opens the breaker; the timed phase then asserts that **60 concurrent requests cost
    less than a single dial would have**. Nothing about that bound is arbitrary: it is the
    configured timeout, and un-short-circuited these 60 would need at least two of them (60
    requests through a pool of 32).
    """
    client, runtime = blackhole
    headers = bearer(redis_settings, user_id="flood-user")

    # Warm-up: enough concurrent failures to trip the breaker. Concurrent rather than sequential so
    # this costs one timeout in total rather than one each.
    await asyncio.gather(
        *(
            client.get(WHOAMI, headers=headers)
            for _ in range(redis_settings.breaker_failures + 1)
        )
    )
    assert runtime.redis.breaker.is_open is True
    before = runtime.redis.short_circuits

    started = time.perf_counter()
    responses = await asyncio.gather(
        *(client.get(WHOAMI, headers=headers) for _ in range(60))
    )
    elapsed = time.perf_counter() - started

    # Still correct: served or refused by the local bucket, never a 500 and never a pool-exhaustion
    # 503 — an open breaker never reaches the pool at all.
    assert all(response.status_code in {200, 429} for response in responses)
    assert elapsed < redis_settings.redis_timeout_ms / 1000
    # ...and the reason was the breaker, not luck. Every one of the 60 was refused without a socket.
    assert runtime.redis.short_circuits - before == 60


async def test_a_degraded_flood_of_distinct_principals_cannot_grow_without_bound(
    dead, redis_settings
):
    """The LRU cap holds under the traffic it exists for: many principals, bounded memory.

    The fallback's key spaces are ``(user, endpoint)`` and ``user``, and the caller picks both, so
    without the cap an outage plus a flood of distinct principals would grow the heap until the pod
    is killed — the attack relocated from Redis into the process rather than stopped.

    **Two entries per principal**, not one: a per-endpoint burst bucket and an account-wide gate.
    They share a single ``OrderedDict`` under a single cap, deliberately, because two 10 000-entry
    maps would be a 20 000-entry bound wearing a 10 000-entry label — so the growth rate doubling
    is exactly the thing that has to stay inside one budget, and this assertion is where a future
    third gate would have to come and declare itself.
    """
    client, runtime = dead

    for index in range(60):
        response = await client.get(
            WHOAMI, headers=bearer(redis_settings, user_id=f"flood-{index}")
        )
        assert response.status_code == 200

    assert runtime.limiter.stats()["fallback"]["size"] == 120
    assert runtime.limiter.stats()["fallback"]["max_entries"] == 10_000
