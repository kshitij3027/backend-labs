"""``GET /dashboard/api/stats`` end to end: real middleware, real limiter, real Redis, real buckets.

``tests/integration/test_middleware_analytics.py`` proves the *write* side — that every terminal
path lands in a bucket, exactly once, at the cost the limiter charged. This file starts where that
one stops: it fires traffic through the app and then asks the **endpoint** what happened, so what
it proves is the half only the read path and the envelope can be wrong about.

Eight properties earn their run time here. The rest is contract-pinning that keeps them honest.

1. **Totals move by exactly the number of requests fired**, across a mix of 200/429/401 — and the
   per-tier, per-endpoint and per-status breakdowns match the traffic that produced them. ``==``
   and never ``>=``: a payload that over-counts and one that under-counts are different bugs, and
   ``>=`` is satisfied by both.

2. **``rate_limit_enabled`` is false when the switch is off, and the rest of the payload is empty.**
   Asserted together, in one test, because the *pair* is the point: with the switch off the
   middleware records nothing, so every other number is byte-identical to a service nobody is
   calling — while ``/health`` still says ``rate_limiter: "active"``. The test asserts that
   blindness explicitly rather than describing it.

3. **The endpoint is not metered and not authenticated.** Exhaust a principal until it is 429ing,
   then poll stats: 200, no ``X-RateLimit-*``, and — the stronger statement — the collector's
   ``records`` counter does not move, so the polls are genuinely exempt rather than merely
   generous.

4. **An hours-only window is unreachable.** ``minutes=0`` clamps to one rather than producing the
   populated-hourly-chart-beside-empty-KPIs shape that reads as an outage.

5. **Redis down is a 200 with a flag**, not a 500 and not silent zeros. Everything that never
   needed the store — tiers, config version, the switch, ``poll_ms`` — is still true and still
   served.

6. **``cost`` is attempted, not consumed.** Asserted against ``GET /api/v1/admin/users/{id}/usage``
   from the same run, so the reconciliation gap is measured rather than described.

7. **A truncated window says so.** ``window.*_requested`` is the caller's ask, not the server's
   ceiling, so ``ANALYTICS_MAX_BUCKETS`` biting is visible as ``covered < requested``. Asserted at
   a cap low enough to truncate the **default** request — the parameterless poll C15 sends — which
   is the only configuration in which the property is not vacuous.

8. **The window's minute bounds bracket the minute series and do not run into the future.**
   ``start_ms``/``end_ms`` span both series and were measured 39.7 *minutes* ahead of the read;
   ``minutes_end_ms`` is 39.7 *seconds* ahead, because the only overhang is the newest bucket still
   filling. A chart that picks the wrong pair is off by 24x, so both are pinned.

Driven through ``httpx.ASGITransport`` — no socket, no server — against ``redis:7-alpine`` over the
compose network.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from src.analytics import ANONYMOUS_USER_ID, OUTCOMES, UNKNOWN_TIER
from src.api.admin import ADMIN_TOKEN_HEADER
from src.api.dashboard import (
    DEFAULT_HOURS,
    DEFAULT_MINUTES,
    MAX_REQUESTABLE_BUCKETS,
    MIN_MINUTES,
)
from src.api.health import (
    POOL_OK,
    POOL_SATURATED,
    RATE_LIMITER_ACTIVE,
    REDIS_OK,
    REDIS_SATURATED,
    REDIS_UNREACHABLE,
    SERVED_BY,
)
from src.config import Settings
from src.identity import DEMO_CREDENTIALS, DEMO_KEY_BY_TIER, issue_token
from src.keys import MS_PER_HOUR, MS_PER_MINUTE
from src.main import Runtime, create_app
from src.models import Tier
from tests.conftest import TEST_ADMIN_TOKEN

STATS = "/dashboard/api/stats"
SHELL = "/dashboard/"
HEALTH = "/health"
ADMIN = "/api/v1/admin"
WHOAMI = "/api/v1/whoami"
LOGS_QUERY = "/api/v1/logs/query"

#: A port nothing listens on inside the test container: connection *refused*, which is what a
#: stopped `redis` container produces. Same constant and same reasoning as
#: ``tests/integration/test_admin_api.py``.
DEAD_URL = "redis://127.0.0.1:6391/0"

#: The classified endpoint labels, spelled out rather than obtained from `classify`. Reading both
#: sides of an assertion from the same function is how a broken classifier satisfies a test about
#: itself.
LABEL_WHOAMI = "GET:/api/v1/whoami"
LABEL_LOGS_QUERY = "GET:/api/v1/logs/query"

#: Shipped weights and limits, written out for the same reason.
COST_WHOAMI = 1
COST_LOGS_QUERY = 5
FREE_RPM = 60
FREE_BURST = 60
SHIPPED_MAX_BUCKETS = 120
SHIPPED_POLL_MS = 5000

#: ``tier -> demo user id``. Derived from the shipped declaration so a renamed demo principal fails
#: here rather than silently asserting about a user nobody seeded.
DEMO_USER_BY_TIER = {credential.tier: credential.user_id for credential in DEMO_CREDENTIALS}

#: **The pinned contract.** Asserted as a set, exactly as ``tests/unit/test_health.py`` pins
#: ``/health``, and for the same reason: a field added "because it is cheap" is one more thing the
#: C15 page and the C13 verifier can start depending on, and one more reason a later commit cannot
#: change it. Adding a key here should be a deliberate edit, not a side effect.
EXPECTED_KEYS = {
    # the measurement
    "totals",
    "per_minute",
    "per_hour",
    "by_status",
    "by_endpoint",
    "by_tier",
    "by_outcome",
    "top_consumers",
    "window",
    # the configuration it has to be read against
    "tiers",
    "config_version",
    "rate_limit_enabled",
    "poll_ms",
    # the health of the machinery that produced it
    "replicas",
    "degraded",
    "pool",
    "dropped",
    "buckets_read",
    # provenance
    "generated_at",
    "served_by",
}


# =============================================================================================
# Fixtures and helpers
# =============================================================================================


async def build_app(settings: Settings) -> tuple[FastAPI, Runtime]:
    """A real app over a real, freshly flushed Redis, with the demo credentials seeded.

    ``create_app(runtime=...)`` skips the FastAPI lifespan by design, so this takes on the
    lifespan's two jobs explicitly. The flush happens **between** connecting and seeding: the other
    order would delete the ``apikey:v1:*`` records every request below authenticates with, and
    every test would fail on a 401 that looks like a middleware bug.
    """
    runtime = Runtime.build(settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()
    return create_app(runtime=runtime), runtime


async def cut_redis(runtime: Runtime) -> None:
    """Take the store away for real: reopen the gateway against a refused port."""
    await runtime.redis.aclose()
    runtime.redis.settings = runtime.settings.model_copy(update={"redis_url": DEAD_URL})
    await runtime.redis.connect()


async def restore_redis(runtime: Runtime) -> None:
    """Give it back. Called by every teardown, so a cut test still cleans up after itself."""
    await runtime.redis.aclose()
    runtime.redis.settings = runtime.settings
    await runtime.redis.connect()


async def _serve(settings: Settings):
    """Yield ``(client, runtime)``, restoring Redis before the flush on the way out.

    The restore is unconditional and comes *before* the flush: a test that cut the store leaves the
    gateway pointed at a dead port, and a teardown that tried to ``FLUSHDB`` through it would fail
    on cleanup and mask the test's own result.
    """
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
async def served(redis_settings: Settings):
    """The shipped configuration: an app, a real Redis, and the real middleware in front."""
    async for pair in _serve(redis_settings):
        yield pair


@pytest.fixture()
async def unmetered(redis_settings: Settings):
    """The same app with ``RATE_LIMIT_ENABLED=false`` — the configuration C9's rubric is about."""
    async for pair in _serve(redis_settings.model_copy(update={"rate_limit_enabled": False})):
        yield pair


@pytest.fixture()
async def slow_poll(redis_settings: Settings):
    """An app whose ``DASHBOARD_POLL_MS`` is deliberately not the shipped 5000."""
    async for pair in _serve(redis_settings.model_copy(update={"dashboard_poll_ms": 1234})):
        yield pair


@pytest.fixture()
async def capped(redis_settings: Settings):
    """An app whose ``ANALYTICS_MAX_BUCKETS`` truncates even the default request.

    One bucket, against a default ask of 60 + 24. Absurd as a deployment and exactly the
    configuration under which "a partial window must not read as a complete one" is checkable —
    in the shipped one (cap 120, default ask 84) the cap never bites and the property is vacuous.
    """
    async for pair in _serve(redis_settings.model_copy(update={"analytics_max_buckets": 1})):
        yield pair


def key_headers(tier: Tier) -> dict[str, str]:
    """``X-API-Key`` for one seeded demo principal, read from the declaration the server seeded."""
    return {"X-API-Key": DEMO_KEY_BY_TIER[tier]}


def bearer(settings: Settings, user_id: str) -> dict[str, str]:
    """Headers for a JWT caller — a fresh principal needing no seeded record and no Redis lookup."""
    return {"Authorization": f"Bearer {issue_token(user_id, settings=settings)}"}


def admin_headers() -> dict[str, str]:
    return {ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN}


async def stats_of(client: httpx.AsyncClient, **params: Any) -> dict[str, Any]:
    """GET the payload, asserting the status, so no test repeats that line."""
    response = await client.get(STATS, params=params)
    assert response.status_code == 200, response.text
    return response.json()


async def drain(client: httpx.AsyncClient, headers: dict[str, str], attempts: int) -> list[int]:
    """Fire ``attempts`` sequential requests at ``/whoami`` and return the status codes."""
    return [(await client.get(WHOAMI, headers=headers)).status_code for _ in range(attempts)]


# =============================================================================================
# 1. The pinned contract
# =============================================================================================


async def test_payload_carries_exactly_the_documented_keys(served):
    """The whole envelope, asserted as a set. See :data:`EXPECTED_KEYS`."""
    client, _ = served

    assert set(await stats_of(client)) == EXPECTED_KEYS


async def test_the_empty_payload_is_fully_shaped_rather_than_sparse(served):
    """Before any traffic: zeros and empty maps, not missing keys.

    A chart handed a missing key draws a gap; one handed an explicit zero draws the flat line that
    is the truth. ``by_outcome`` in particular is seeded with every known outcome, so a rejection
    *rate* is computable on a window in which nothing was rejected.
    """
    client, _ = served

    body = await stats_of(client)

    assert body["totals"] == {
        "requests": 0,
        "cost": 0,
        "allowed": 0,
        "denied": 0,
        "degraded": 0,
    }
    assert set(body["by_outcome"]) == set(OUTCOMES)
    assert body["by_status"] == {}
    assert body["by_endpoint"] == {}
    assert body["by_tier"] == {}
    assert body["top_consumers"] == []
    # The series are populated with empty buckets rather than being empty lists: the minutes
    # exist, nothing happened in them, and that is a different statement from "no data".
    assert len(body["per_minute"]) == DEFAULT_MINUTES
    assert len(body["per_hour"]) == DEFAULT_HOURS
    assert all(bucket["requests"] == 0 for bucket in body["per_minute"])


# =============================================================================================
# 2. Totals and breakdowns move by exactly what was fired
# =============================================================================================


async def test_totals_move_by_exactly_the_number_of_requests_fired(served):
    """A mix of 200, 429 and 401, and the payload accounts for **all** of them. Exactly.

    The free tier ships at 60 rpm / burst 60 and ``/whoami`` costs 1, so 70 requests guarantee
    refusals without the test needing to know which gate produced them.

    ``==`` rather than ``>=`` in every line: a payload that counted only the happy path would
    under-report (and turn the dashboard's request count into a *success* count — the number least
    worth watching), while one that recorded on the way in and the way out would double every rate
    on the page. ``>=`` cannot tell either from correct.
    """
    client, runtime = served
    free = key_headers(Tier.FREE)

    statuses = await drain(client, free, 70)
    statuses += [(await client.get(WHOAMI)).status_code for _ in range(3)]  # no credential -> 401

    allowed = statuses.count(200)
    denied = statuses.count(429)
    unauthorised = statuses.count(401)
    total = len(statuses)

    # The traffic really was mixed — otherwise this asserts about one path wearing three names.
    assert allowed > 0
    assert denied > 0
    assert unauthorised == 3
    assert allowed + denied + unauthorised == total

    body = await stats_of(client)

    assert body["totals"]["requests"] == total
    assert body["totals"]["allowed"] == allowed
    # The outcome dimension partitions the traffic: a 401 is a refusal too. Which *kind* of
    # refusal is not lost — `by_status` separates them.
    assert body["totals"]["denied"] == denied + unauthorised
    assert body["totals"]["degraded"] == 0
    assert body["by_outcome"]["allowed"] + body["by_outcome"]["denied"] == total
    assert body["by_status"] == {"200": allowed, "429": denied, "401": unauthorised}

    # The per-minute series carries the same traffic, and it is the series the totals were folded
    # from — so a fold that read the hour buckets as well would show up here as a mismatch.
    assert sum(bucket["requests"] for bucket in body["per_minute"]) == total
    assert body["dropped"]["records"] == 0
    assert body["dropped"]["records_written"] == total
    assert runtime.analytics.records == total


async def test_per_tier_and_per_endpoint_breakdowns_match_the_traffic_fired(served):
    """Three tiers, two endpoints, and every bar is the number of requests that produced it.

    Tiers are exercised through the seeded demo API keys rather than through a JWT plus an admin
    assignment: the point of the assertion is that the *recorded* tier is the tier the decision was
    priced at, and the fewest moving parts between the two the better.

    The anonymous 401s land under the ``unknown`` tier sentinel, which is the honest answer — a 401
    never reaches ``user:{uid}``, so any tier name would be a guess — and they are still classified
    to an endpoint, because the label is computed above identity.
    """
    client, _ = served

    for _ in range(5):
        assert (await client.get(WHOAMI, headers=key_headers(Tier.PREMIUM))).status_code == 200
    for _ in range(3):
        response = await client.get(LOGS_QUERY, headers=key_headers(Tier.ENTERPRISE))
        assert response.status_code == 200
    for _ in range(2):
        assert (await client.get(WHOAMI)).status_code == 401

    body = await stats_of(client)

    assert body["by_tier"] == {
        Tier.PREMIUM.value: 5,
        Tier.ENTERPRISE.value: 3,
        UNKNOWN_TIER: 2,
    }
    assert body["by_endpoint"] == {LABEL_WHOAMI: 7, LABEL_LOGS_QUERY: 3}
    assert body["by_status"] == {"200": 8, "401": 2}
    assert body["totals"]["requests"] == 10


async def test_top_consumers_ranks_by_attempted_cost_and_names_principals(served):
    """The heavier caller tops the list even though it made fewer calls — and the ids are public.

    Ranking by cost is the point of the ZSET: three calls to a 5-token endpoint outweigh ten to a
    1-token one, and a ranking by request count would put the cheap caller on top and send an
    operator after the wrong client.

    This test is also the one that **documents the hole**. It asserts that real principal ids come
    back from an endpoint no credential was presented to. That is the block ``src/api/dashboard.py``
    names as the one to put behind ``ADMIN_TOKEN`` in a real deployment; asserting it here means the
    exposure is a stated, tested property rather than something nobody noticed.
    """
    client, _ = served

    for _ in range(3):
        assert (await client.get(LOGS_QUERY, headers=key_headers(Tier.ENTERPRISE))).status_code == 200
    for _ in range(10):
        assert (await client.get(WHOAMI, headers=key_headers(Tier.PREMIUM))).status_code == 200
    assert (await client.get(WHOAMI)).status_code == 401

    body = await stats_of(client)
    ranking = {entry["user_id"]: entry["cost"] for entry in body["top_consumers"]}

    assert ranking[DEMO_USER_BY_TIER[Tier.ENTERPRISE]] == 3 * COST_LOGS_QUERY
    assert ranking[DEMO_USER_BY_TIER[Tier.PREMIUM]] == 10 * COST_WHOAMI
    # The 401 flood is attributed to the sentinel rather than dropped — that traffic is invisible
    # in every other counter this service keeps.
    assert ranking[ANONYMOUS_USER_ID] == COST_WHOAMI
    # Heaviest first, and the heavier caller made fewer calls.
    assert body["top_consumers"][0]["user_id"] == DEMO_USER_BY_TIER[Tier.ENTERPRISE]


async def test_cost_is_attempted_and_does_not_reconcile_against_daily_used(served):
    """The gap between ``totals.cost`` and ``daily.used`` **is** the throttled demand.

    Both numbers are read from the same run: the payload's attempted cost, and the admin API's
    charged usage for the same principal. A denial writes nothing (C4's founding property), so the
    two are equal only while nothing is being refused — and here they are deliberately not.

    Stated as an assertion rather than a docstring because a future change that "fixed" the
    discrepancy by recording refusals at zero cost would make the most expensive endpoint look like
    the cheapest exactly as it started being throttled, and every comment saying so would still
    read as true.
    """
    client, runtime = served
    user_id = "attempted-vs-charged"
    caller = bearer(runtime.settings, user_id)

    statuses = await drain(client, caller, 70)
    allowed = statuses.count(200)
    denied = statuses.count(429)
    assert allowed > 0 and denied > 0

    body = await stats_of(client)
    response = await client.get(f"{ADMIN}/users/{user_id}/usage", headers=admin_headers())
    assert response.status_code == 200, response.text
    charged = response.json()["daily"]["used"]

    # Every request attempted one token, admitted or not.
    assert body["totals"]["cost"] == len(statuses) * COST_WHOAMI
    # Only the admitted ones were charged — a denial writes nothing at all.
    assert charged == allowed
    assert body["totals"]["cost"] > charged
    assert body["totals"]["cost"] - charged == denied * COST_WHOAMI


# =============================================================================================
# 3. rate_limit_enabled — the field that exists to explain an empty page
# =============================================================================================


async def test_rate_limit_enabled_is_false_and_the_payload_is_otherwise_blind(unmetered):
    """With the switch off: traffic flows, nothing is recorded, and ONE field says why.

    The two halves are asserted together on purpose. Everything below the switch — totals, both
    series, every breakdown, the ranking — is byte-identical to a healthy, fully-metered service
    that simply has no callers. ``/health`` is asserted in the same test because it is the other
    surface an operator would check, and it reports ``rate_limiter: "active"`` throughout: the
    field tracks the C8 fallback bucket, not the switch, and nothing anywhere else in this service
    names the switch at all.

    So this is the exact blindness ``rate_limit_enabled`` exists to explain, and the requests being
    fired here are additionally being served **unauthenticated** — the configuration in which the
    ambiguity is most expensive.
    """
    client, runtime = unmetered

    # No credential at all, and every one is served: the switch disables authentication with
    # enforcement, because the middleware returns before identity is resolved.
    for _ in range(25):
        assert (await client.get(WHOAMI)).status_code == 200

    body = await stats_of(client)

    assert body["rate_limit_enabled"] is False

    # ... and nothing else in the payload can tell you that 25 requests just went through.
    assert body["totals"] == {
        "requests": 0,
        "cost": 0,
        "allowed": 0,
        "denied": 0,
        "degraded": 0,
    }
    assert body["by_status"] == {}
    assert body["by_endpoint"] == {}
    assert body["by_tier"] == {}
    assert body["by_outcome"] == dict.fromkeys(OUTCOMES, 0)
    assert body["top_consumers"] == []
    assert all(bucket["requests"] == 0 for bucket in body["per_minute"])
    assert all(bucket["requests"] == 0 for bucket in body["per_hour"])
    # Not a lossy recorder either: nothing was even attempted, so the drop counters are clean and
    # an operator cannot mistake this for analytics failing.
    assert body["dropped"]["records"] == 0
    assert body["dropped"]["records_written"] == 0
    assert body["degraded"]["stats_unavailable"] is False
    assert runtime.analytics.records == 0

    # The other surface, in the same configuration, saying nothing about it.
    health = await client.get(HEALTH)
    assert health.status_code == 200
    assert health.json()["rate_limiter"] == RATE_LIMITER_ACTIVE
    assert "rate_limit_enabled" not in health.json()


async def test_rate_limit_enabled_is_true_in_the_shipped_configuration(served):
    """The field is present and correct in both configurations — not only in the interesting one."""
    client, _ = served

    assert (await stats_of(client))["rate_limit_enabled"] is True


# =============================================================================================
# 4. Unmetered and unauthenticated
# =============================================================================================


async def test_stats_is_not_rate_limited_by_the_thing_it_observes(served):
    """Exhaust a principal, then poll stats: 200 every time, and no rate-limit headers.

    The headers matter as much as the status. A header describing a limit that was never evaluated
    is a lie a client builds pacing logic on top of, and the exempt branch emits none — so their
    absence is what proves the request went *around* the limiter rather than through it and being
    allowed.
    """
    client, _ = served

    statuses = await drain(client, key_headers(Tier.FREE), 70)
    assert statuses.count(429) > 0  # the principal really is being refused

    for _ in range(10):
        response = await client.get(STATS)
        assert response.status_code == 200
        assert not [name for name in response.headers if name.lower().startswith("x-ratelimit")]
        assert "retry-after" not in response.headers


async def test_stats_is_exempt_from_metering_rather_than_merely_generous(served):
    """Polling the endpoint records nothing — it is outside the enforcement layer, not inside it.

    A route that were metered but roomy would satisfy the status-code test above and still put its
    own polls on the graph, so the dashboard's request count would include the dashboard. The
    collector's counter is what distinguishes the two.
    """
    client, runtime = served

    assert (await client.get(WHOAMI, headers=key_headers(Tier.FREE))).status_code == 200
    before = runtime.analytics.records

    for _ in range(15):
        assert (await client.get(STATS)).status_code == 200

    assert runtime.analytics.records == before
    assert (await stats_of(client))["totals"]["requests"] == 1


async def test_stats_is_reachable_unauthenticated(served):
    """No credential, a bogus credential, and a bogus admin token all answer 200 identically.

    The last two matter: a route that happened to work without a header but 401'd on a *wrong* one
    would be authenticated-with-a-loophole rather than public, and the difference shows up the
    first time a browser attaches a stale token.
    """
    client, _ = served

    for headers in (
        {},
        {"X-API-Key": "definitely-not-a-real-key"},
        {"Authorization": "Bearer not-a-token"},
        {ADMIN_TOKEN_HEADER: "wrong-admin-token"},
    ):
        response = await client.get(STATS, headers=headers)
        assert response.status_code == 200, headers
        assert set(response.json()) == EXPECTED_KEYS


# =============================================================================================
# 5. Window sizing — clamped, never 422, and never the hours-only shape
# =============================================================================================


@pytest.mark.parametrize("minutes", [0, -1, -500])
async def test_minutes_below_one_is_clamped_and_never_yields_the_hours_only_shape(
    served, minutes: int
):
    """``minutes=0`` with hours requested must not produce empty KPIs beside a populated chart.

    That shape is what a caller asking for hours alone would get — ``totals`` and every ``by_*``
    are folded from the minute buckets only — and it reads as an outage to everyone who sees it. So
    the floor is enforced by the endpoint rather than trusted to the caller: the traffic below is
    still in the totals, and the hourly series is populated beside them rather than instead of
    them.

    Clamped rather than 422'd, per the house pattern: the server already knows the right answer,
    and ``window.minutes_requested`` reports what it actually used.
    """
    client, _ = served
    assert (await client.get(WHOAMI, headers=key_headers(Tier.FREE))).status_code == 200

    body = await stats_of(client, minutes=minutes, hours=6)

    assert body["window"]["minutes_requested"] == MIN_MINUTES
    assert body["window"]["minutes_covered"] == MIN_MINUTES
    assert body["window"]["hours_covered"] == 6
    # The shape the floor exists to prevent: populated hours, empty everything else.
    assert body["totals"]["requests"] == 1
    assert body["by_endpoint"] == {LABEL_WHOAMI: 1}
    assert len(body["per_hour"]) == 6


async def test_hours_zero_is_a_legitimate_ask(served):
    """The reverse omission is fine: the live chart without the context line loses nothing.

    Asserted so the asymmetry between the two floors is a tested decision rather than an oversight
    in one of them.
    """
    client, _ = served
    assert (await client.get(WHOAMI, headers=key_headers(Tier.FREE))).status_code == 200

    body = await stats_of(client, minutes=5, hours=0)

    assert body["per_hour"] == []
    assert body["window"]["hours_requested"] == 0
    assert body["window"]["hours_covered"] == 0
    assert body["totals"]["requests"] == 1
    assert body["dropped"]["buckets"] == 0


async def test_an_over_large_ask_is_echoed_back_and_the_truncation_is_reported(served):
    """The caller's ask survives into the payload; ``covered`` is what shows the cap. Never a 422.

    The bug this pins: clamping to ``ANALYTICS_MAX_BUCKETS`` *before* the collector saw the request
    made ``requested`` and ``covered`` agree by construction, so every truncated window reported
    itself as complete. ``window.minutes_requested`` says "minute buckets the caller asked for" —
    it now does.

    Two separate bounds are visible. ``ANALYTICS_MAX_BUCKETS`` (120) caps what is **read**, applied
    by the collector to the two series *together* with minutes first — so a maximal minute window
    starves the hour series entirely, and that starvation is reported rather than hidden.
    ``MAX_REQUESTABLE_BUCKETS`` caps what a caller may **ask**, and exists only so ``dropped``
    stays inside 64 bits on an unauthenticated endpoint; it is three orders of magnitude above the
    shipped cap, so it is never the bound that bites in a real configuration.
    """
    client, _ = served

    body = await stats_of(client, minutes=100_000, hours=100_000)

    # Echoed, not silently replaced by the server's ceiling.
    assert body["window"]["minutes_requested"] == 100_000
    assert body["window"]["hours_requested"] == 100_000
    # ... and the cap is visible as the gap between the two.
    assert body["window"]["minutes_covered"] == SHIPPED_MAX_BUCKETS
    assert body["window"]["hours_covered"] == 0
    assert body["buckets_read"] == SHIPPED_MAX_BUCKETS
    assert body["dropped"]["buckets"] == 200_000 - SHIPPED_MAX_BUCKETS
    assert len(body["per_minute"]) == SHIPPED_MAX_BUCKETS
    assert body["per_hour"] == []


async def test_an_unserialisable_ask_is_trimmed_rather_than_500ing(served):
    """``minutes=10**30`` on an unauthenticated endpoint must not become a JSON encoding error.

    ``dropped`` is arithmetic on the request, so without :data:`MAX_REQUESTABLE_BUCKETS` an
    anonymous caller could push it past what orjson will encode and turn the endpoint whose whole
    purpose is not to 500 into one that does. Trimmed and reported like any other truncation.
    """
    client, _ = served

    body = await stats_of(client, minutes=10**30, hours=10**30)

    assert body["window"]["minutes_requested"] == MAX_REQUESTABLE_BUCKETS
    assert body["window"]["hours_requested"] == MAX_REQUESTABLE_BUCKETS
    assert body["window"]["minutes_covered"] == SHIPPED_MAX_BUCKETS
    assert body["dropped"]["buckets"] == 2 * MAX_REQUESTABLE_BUCKETS - SHIPPED_MAX_BUCKETS


async def test_a_low_cap_makes_the_DEFAULT_request_visibly_partial(capped):
    """The sharp case: no query parameters at all, and the payload still admits it is partial.

    This is exactly what C15's page sends. With ``ANALYTICS_MAX_BUCKETS=1`` the read covers one
    minute and no hours, and before this fix the window reported ``minutes_requested: 1`` — a
    one-minute chart presenting itself as a complete answer to a request for sixty. Nothing in the
    payload said otherwise, and the log line saying so is on a replica nobody is looking at.

    ``requested > covered`` is the invariant the model's own field descriptions promise, asserted
    here against a configuration where it actually bites.
    """
    client, _ = capped

    body = await stats_of(client)

    assert body["window"]["minutes_requested"] == DEFAULT_MINUTES
    assert body["window"]["minutes_covered"] == 1
    assert body["window"]["minutes_requested"] > body["window"]["minutes_covered"]
    assert body["window"]["hours_requested"] == DEFAULT_HOURS
    assert body["window"]["hours_covered"] == 0
    assert body["buckets_read"] == 1
    assert body["dropped"]["buckets"] == DEFAULT_MINUTES + DEFAULT_HOURS - 1
    assert len(body["per_minute"]) == 1


async def test_the_default_window_is_never_truncated(served):
    """60 + 24 fits inside the shipped cap of 120, and that is what makes it the default."""
    client, _ = served

    body = await stats_of(client)

    assert body["window"]["minutes_requested"] == DEFAULT_MINUTES
    assert body["window"]["hours_requested"] == DEFAULT_HOURS
    assert body["window"]["minutes_covered"] == DEFAULT_MINUTES
    assert body["window"]["hours_covered"] == DEFAULT_HOURS
    assert body["buckets_read"] == DEFAULT_MINUTES + DEFAULT_HOURS
    assert body["dropped"]["buckets"] == 0


async def test_a_non_numeric_window_is_the_one_422_this_endpoint_has(served):
    """``?minutes=abc`` cannot be clamped, so FastAPI's parser refuses it.

    Worth pinning: the clamping rule is about *values*, not about types. There is no sensible
    ceiling to hand a caller who did not send a number, and silently substituting the default would
    hide a client bug behind a plausible payload.
    """
    client, _ = served

    assert (await client.get(STATS, params={"minutes": "abc"})).status_code == 422


# =============================================================================================
# 6. Configuration served beside the measurement
# =============================================================================================


async def test_poll_ms_is_five_seconds_and_comes_from_configuration(served, redis_settings):
    """The dashboard's interval has ONE source of truth, and it is this field."""
    client, _ = served

    assert (await stats_of(client))["poll_ms"] == SHIPPED_POLL_MS
    assert redis_settings.dashboard_poll_ms == SHIPPED_POLL_MS


async def test_poll_ms_follows_the_setting_rather_than_a_constant(slow_poll):
    """Change ``DASHBOARD_POLL_MS`` and the payload changes with it.

    Without this the previous test is satisfied by a hard-coded 5000, which is exactly the second
    source of truth the field exists to remove.
    """
    client, _ = slow_poll

    assert (await stats_of(client))["poll_ms"] == 1234


async def test_tiers_and_config_version_reflect_a_runtime_tier_change(served):
    """A C10 tier write is visible on the dashboard payload without a restart.

    The replica that serves the ``PUT`` invalidates and re-reads its own snapshot synchronously, so
    by the time the response is written it is already enforcing the new numbers — and this endpoint
    serves that same snapshot. Reading it back from ``config:tiers`` instead would answer "what is
    stored?" while the useful question is "what is this replica enforcing?".
    """
    client, _ = served

    before = await stats_of(client)
    assert before["tiers"][Tier.FREE.value]["burst"] == FREE_BURST
    assert before["tiers"][Tier.FREE.value]["rate_limit_per_min"] == FREE_RPM

    written = await client.put(
        f"{ADMIN}/tiers/{Tier.FREE.value}", json={"burst": 11}, headers=admin_headers()
    )
    assert written.status_code == 200, written.text

    after = await stats_of(client)

    assert after["tiers"][Tier.FREE.value]["burst"] == 11
    # The other three numbers are untouched — a partial PUT, merged inside Redis.
    assert after["tiers"][Tier.FREE.value]["rate_limit_per_min"] == FREE_RPM
    assert after["config_version"] == written.json()["config_version"]
    assert after["config_version"] > before["config_version"]


async def test_replicas_reports_this_replica_and_refuses_to_invent_the_others(served):
    """No replica dimension is recorded, so none is reported — and the payload says so.

    ``configured`` is what an operator *declared* (``API_REPLICAS``), and rendering it as "2
    replicas serving" would state as measurement the one thing this payload has no evidence for, on
    the page an operator opens to find out whether a replica has stopped.
    """
    client, runtime = served

    body = await stats_of(client)

    assert body["served_by"] == SERVED_BY
    assert body["replicas"]["served_by"] == SERVED_BY
    assert body["replicas"]["configured"] == runtime.settings.api_replicas
    assert body["replicas"]["observed"] == []
    assert body["replicas"]["attributed"] is False


async def test_generated_at_is_this_replicas_clock_beside_a_redis_named_window(served):
    """Two clocks, deliberately — the gap between them is this replica's skew.

    ``generated_at`` is local wall-clock milliseconds; ``window.server_now_ms`` is Redis's ``TIME``
    at the instant of the read, which is what makes two replicas name the same window. Asserting
    both are present and plausible is what stops a later change quietly deriving one from the other
    and losing the ability to see the drift.
    """
    client, _ = served
    local_ms = int(time.time() * 1000)

    body = await stats_of(client)

    assert abs(body["generated_at"] - local_ms) < 60_000
    assert body["window"]["server_now_ms"] is not None
    assert abs(body["generated_at"] - body["window"]["server_now_ms"]) < 60_000
    assert body["window"]["end_ms"] is not None
    assert body["window"]["start_ms"] < body["window"]["end_ms"]


async def test_the_minute_bounds_bracket_the_minute_series_and_do_not_run_into_the_future(served):
    """The bounds a KPI tile and the live chart must use, and what makes them different.

    ``start_ms``/``end_ms`` span **both** series, so on the default request they describe 24 hours
    while ``totals`` and every ``by_*`` describe 60 minutes — and ``end_ms``, the close of the
    current *hour* bucket, sits up to an hour in the future. A chart plotting the minute series on
    that axis renders most of an hour of empty future under a heading that is wrong by 24x.

    So the minute series carries its own pair, and this asserts the three properties C15 needs:
    that they bracket every bucket actually returned, that the range is exactly
    ``minutes_covered`` bucket-widths wide, and that the only "future" in it is the newest bucket
    still filling — at most one minute past ``server_now_ms``, against the hour series' hour.
    """
    client, _ = served

    body = await stats_of(client)
    window = body["window"]
    now_ms = window["server_now_ms"]

    # They bracket the series they describe, exactly.
    assert window["minutes_start_ms"] == body["per_minute"][0]["start_ms"]
    assert window["minutes_end_ms"] == (
        body["per_minute"][-1]["start_ms"] + body["per_minute"][-1]["width_ms"]
    )
    assert window["minutes_end_ms"] - window["minutes_start_ms"] == (
        window["minutes_covered"] * MS_PER_MINUTE
    )

    # The read's own instant sits inside the newest minute bucket, so the only overhang is that
    # bucket still filling — one minute, not the hour series' sixty.
    assert window["minutes_start_ms"] <= now_ms < window["minutes_end_ms"]
    assert 0 < window["minutes_end_ms"] - now_ms <= MS_PER_MINUTE

    # The hour pair does the same job for its own series...
    assert window["hours_start_ms"] == body["per_hour"][0]["start_ms"]
    assert window["hours_start_ms"] <= now_ms < window["hours_end_ms"]
    assert 0 < window["hours_end_ms"] - now_ms <= MS_PER_HOUR

    # ... and the spanning pair is the union of the two, which is why it must not be used as a
    # KPI label: it reaches a day back and up to an hour forward.
    assert window["start_ms"] == min(window["minutes_start_ms"], window["hours_start_ms"])
    assert window["end_ms"] == max(window["minutes_end_ms"], window["hours_end_ms"])
    assert window["end_ms"] - window["minutes_end_ms"] >= 0


async def test_the_window_carries_no_clock_when_the_store_could_not_be_asked(served):
    """``server_now_ms`` is null during an outage rather than filled from the local clock.

    The clock is the first thing the snapshot asks Redis for, so on the degraded path it is
    genuinely unknown — and substituting this replica's wall clock would hand a consumer a
    *shared*-clock instant that no other replica agrees with, which is the per-replica skew the
    whole read path is built to avoid. Null is the value that cannot be silently plotted.
    """
    client, runtime = served
    await cut_redis(runtime)

    window = (await stats_of(client))["window"]

    assert window["server_now_ms"] is None
    assert window["minutes_start_ms"] is None
    assert window["minutes_end_ms"] is None
    assert window["hours_start_ms"] is None
    assert window["hours_end_ms"] is None
    assert window["start_ms"] is None
    assert window["end_ms"] is None


# =============================================================================================
# 7. Health signals — and what happens when the store is gone
# =============================================================================================


async def test_healthy_signals_report_a_working_write_path(served):
    """After clean traffic every "something is wrong" signal is off, and the counters agree.

    The drop counters are the point. ``AnalyticsCollector.record`` swallows every exception by
    design, so a collector that has recorded nothing for an hour looks exactly like one that is
    working — these are the only numbers where that difference is visible, and they are only
    meaningful next to the request count on the same payload.
    """
    client, _ = served

    for _ in range(5):
        assert (await client.get(WHOAMI, headers=key_headers(Tier.PREMIUM))).status_code == 200

    body = await stats_of(client)

    assert body["degraded"] == {
        "rate_limiter": False,
        "store": REDIS_OK,
        "stats_unavailable": False,
        "since_sec": None,
        "breaker": "closed",
        "detail": None,
    }
    assert body["pool"]["state"] == POOL_OK
    assert body["pool"]["max_connections"] == 32
    assert body["pool"]["overloads"] == 0
    assert body["dropped"] == {
        "buckets": 0,
        "records": 0,
        "records_written": 5,
        "errors": 0,
        "shed": 0,
        "last_error": None,
    }


async def test_stats_answers_200_with_a_flag_when_redis_is_down(served):
    """The decision this endpoint exists to get right: **200, flagged**, not 500 and not zeros.

    An observability endpoint that 500s during an incident is lost at the exact moment it is the
    thing being opened. But silent zeros would be worse than an error — they say traffic has
    stopped, which is the single most misleading thing this surface can claim. So the measurement
    is served zeroed *and labelled*, and everything that never needed the store is still true:
    ``tiers`` and ``config_version`` come from the in-process snapshot, and the switch and
    ``poll_ms`` from configuration.

    This is not a contradiction of :meth:`AnalyticsCollector.snapshot` refusing to swallow. That
    method must raise, because a caller handed silent zeros has no way to know; the flag is what
    only this layer — which has a response envelope to put a label in — is able to add.
    """
    client, runtime = served
    assert (await client.get(WHOAMI, headers=key_headers(Tier.FREE))).status_code == 200

    await cut_redis(runtime)
    response = await client.get(STATS)

    assert response.status_code == 200
    body = response.json()
    assert set(body) == EXPECTED_KEYS

    # The measurement is unknown, and says so.
    assert body["degraded"]["stats_unavailable"] is True
    assert body["degraded"]["store"] == REDIS_UNREACHABLE
    assert body["degraded"]["detail"] is not None
    assert body["totals"]["requests"] == 0
    assert body["per_minute"] == []
    assert body["per_hour"] == []
    assert body["top_consumers"] == []
    assert body["by_outcome"] == dict.fromkeys(OUTCOMES, 0)
    assert body["buckets_read"] == 0
    assert body["window"]["minutes_covered"] == 0
    assert body["window"]["hours_covered"] == 0
    # Every bucket asked for and not covered is counted, so the zeros above are provably a
    # non-reading rather than an empty window.
    assert body["dropped"]["buckets"] == DEFAULT_MINUTES + DEFAULT_HOURS

    # Everything that never needed the store is still served.
    assert body["rate_limit_enabled"] is True
    assert body["poll_ms"] == SHIPPED_POLL_MS
    assert body["tiers"][Tier.FREE.value]["rate_limit_per_min"] == FREE_RPM
    assert body["config_version"] == runtime.tiers.snapshot().version
    assert body["served_by"] == SERVED_BY
    assert body["generated_at"] > 0


async def test_the_degraded_flag_clears_when_the_store_comes_back(served):
    """A flag that latched would be a permanent alarm after one blip.

    The gateway clears ``degraded_since`` on its next success, so the payload does too — asserted
    because "the incident is over" is exactly as important to report as "the incident started".
    """
    client, runtime = served

    await cut_redis(runtime)
    assert (await stats_of(client))["degraded"]["stats_unavailable"] is True

    await restore_redis(runtime)
    recovered = await stats_of(client)

    assert recovered["degraded"]["stats_unavailable"] is False
    assert recovered["degraded"]["store"] == REDIS_OK
    assert recovered["degraded"]["detail"] is None


async def test_a_saturated_pool_is_not_reported_as_an_unreachable_store(served):
    """"We could not ask" and "it did not answer" are different diagnoses with different remedies.

    When this process runs out of pooled connections no packet reaches Redis, so its reachability
    is genuinely unknown — and reporting ``unreachable`` would blame a store that is answering
    every other client perfectly, sending an operator to debug the wrong machine mid-incident. Same
    distinction, same wording, as ``/health``'s ``redis: "saturated"``.

    ``overloaded_since`` is set directly because provoking real pool exhaustion needs a
    concurrency harness that belongs in ``tests/unit/test_overload.py``, where it already lives.
    What is under test here is the *reporting*, and the field is the gateway's own public marker.
    """
    client, runtime = served

    await cut_redis(runtime)
    runtime.redis.overloaded_since = time.monotonic()

    body = await stats_of(client)

    assert body["degraded"]["stats_unavailable"] is True
    assert body["degraded"]["store"] == REDIS_SATURATED
    assert body["pool"]["state"] == POOL_SATURATED
    assert body["pool"]["overloaded_for_sec"] is not None


# =============================================================================================
# 8. The shell C15 will fill in
# =============================================================================================


async def test_dashboard_shell_answers_a_readable_404_until_c15(served):
    """No page yet, and the 404 says so plus where the data already is.

    A 200 placeholder was the alternative and was rejected: a page that renders nothing is
    indistinguishable from a broken one, a monitor goes green on it, and C15 would then be changing
    a working URL's behaviour rather than filling a hole.
    """
    client, _ = served

    response = await client.get(SHELL)

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert "C15" in detail
    assert STATS in detail


async def test_the_shell_is_exempt_from_metering_too(served):
    """The 404 carries no rate-limit headers: ``/dashboard`` is exempt on a segment boundary.

    Worth its own line because the exemption is a *prefix* rule, so proving it for the feed does
    not prove it for the page — and the page is the surface a human authenticates from.
    """
    client, _ = served
    assert (await drain(client, key_headers(Tier.FREE), 70)).count(429) > 0

    response = await client.get(SHELL)

    assert response.status_code == 404
    assert not [name for name in response.headers if name.lower().startswith("x-ratelimit")]
