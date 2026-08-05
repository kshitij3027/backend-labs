"""Analytics through the whole app: real middleware, real limiter, real Redis, real buckets.

``tests/integration/test_analytics_redis.py`` drives :class:`~src.analytics.AnalyticsCollector`
directly, choosing its own dimensions. This file never touches the collector's ``record`` at all —
it fires HTTP requests and then reads the buckets — so what it proves is the part only the wiring
can be wrong about:

* that the middleware records on **every** terminal path rather than only the happy one;
* that a 401, which has no principal to attribute, is recorded anyway under the anonymous
  sentinel instead of being dropped;
* that the ``cost`` written to the bucket is the cost the limiter actually **charged**, not a 1
  the middleware invented on the way past — the weighted-cost bonus is only a feature if the two
  numbers are the same one.

Every assertion reads the bucket back out of Redis rather than reading a counter off the collector,
because a counter is the collector agreeing with itself. The bucket is what a dashboard will
render.
"""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest
from fastapi import FastAPI

from src.analytics import (
    ANONYMOUS_USER_ID,
    OUTCOME_ALLOWED,
    OUTCOME_DENIED,
    UNKNOWN_TIER,
)
from src.config import Settings
from src.identity import DEMO_KEY_BY_TIER
from src.keys import (
    day_string,
    hour_index,
    minute_index,
    stats_hour_key,
    stats_minute_key,
)
from src.lua import (
    RECORD_FIELD_COST,
    RECORD_FIELD_ENDPOINT_PREFIX,
    RECORD_FIELD_OUTCOME_PREFIX,
    RECORD_FIELD_REQUESTS,
    RECORD_FIELD_STATUS_PREFIX,
    RECORD_FIELD_TIER_PREFIX,
)
from src.main import Runtime, create_app
from src.models import Tier

WHOAMI = "/api/v1/whoami"
LOGS_QUERY = "/api/v1/logs/query"
LOGS_INGEST = "/api/v1/logs/ingest"

#: The shipped ``ENDPOINT_COSTS`` weights, written out rather than read from ``Settings``. The
#: point of the cost test is that the *number in the bucket* is the number the tier was charged, so
#: reading both sides from the same setting would let a mis-wired middleware satisfy it.
COST_LOGS_QUERY = 5
COST_LOGS_INGEST = 2
COST_DEFAULT = 1

#: Where the exploding handler is mounted. Deliberately NOT under ``/api/v1/logs/``: the shipped
#: router already serves ``GET /api/v1/logs/{log_id}``, which matches first and answers an unknown
#: id with its own 404 — so a probe mounted there would never reach the raising handler and the
#: test would pass for the wrong reason. Unclassified, so it prices as ``other`` at cost 1.
BOOM = "/api/v1/boom"

#: A valid ingest body. Anything the model rejects would 422 — still metered, but a different test.
INGEST_BODY = {"level": "ERROR", "service": "payments-svc", "message": "card declined"}


@pytest.fixture()
async def metered(redis_settings: Settings):
    """A real app over a real, freshly flushed Redis, with the demo credentials seeded.

    ``create_app(runtime=...)`` skips the FastAPI lifespan by design, so this takes on the
    lifespan's two jobs explicitly. The flush happens **between** connecting and seeding: seeding
    first would have the flush delete the ``apikey:v1:*`` records every request below
    authenticates with, and every test would fail on a 401 that looks like a middleware bug.
    """
    runtime = Runtime.build(redis_settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()

    app: FastAPI = create_app(runtime=runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            try:
                await runtime.redis.client.flushdb()
            finally:
                await runtime.stop()


def key_headers(tier: Tier) -> dict[str, str]:
    """``X-API-Key`` for one seeded demo principal, read from the declaration the server seeded."""
    return {"X-API-Key": DEMO_KEY_BY_TIER[tier]}


async def buckets(runtime: Runtime) -> tuple[dict[str, int], dict[str, int]]:
    """Every minute and every hour bucket in the store, summed into two field maps.

    Summed across buckets rather than read from one, and that is deliberate: a test that computed
    "the current minute" would flake once a minute, when a request lands either side of a boundary
    the test crossed between firing and reading. Summing removes the race without weakening
    anything — the assertions are about totals, and the bucketing arithmetic has its own tests in
    ``tests/unit/test_analytics.py``.

    Read by scanning **this test's own** keyspace, which is fine here for the same reason it is
    forbidden in ``src/``: a test container's flushed database has a handful of keys and no
    latency budget, while the production read side runs on the endpoint a dashboard polls every
    5 seconds against a keyspace holding every bucket, quota and rate-limit key in the system.
    """
    minutes: dict[str, int] = {}
    hours: dict[str, int] = {}
    async for raw_key in runtime.redis.client.scan_iter(match="stats:*"):
        key = raw_key.decode()
        if key.startswith("stats:top:"):
            continue
        target = minutes if key.startswith("stats:min:") else hours
        for name, value in (await runtime.redis.client.hgetall(key)).items():
            field = name.decode()
            target[field] = target.get(field, 0) + int(value)
    return minutes, hours


# =============================================================================================
# 1. Every terminal path is recorded — exactly once
# =============================================================================================


async def test_totals_move_by_exactly_the_number_of_requests_fired(metered):
    """A mix of 200, 429 and 401, and the bucket holds **all** of them. Exactly, not approximately.

    ``==`` and not ``>=``, because both failure directions are real bugs with opposite causes: a
    record on only the happy path under-counts (and produces a dashboard whose request count is a
    *success* count — the number least worth watching), while a record fired both before and after
    the response would double-count and make every rate on the page wrong by a factor of two.

    The free tier ships at 60 rpm / burst 60 and ``/whoami`` costs 1, so 70 requests guarantee some
    refusals without needing to know which gate produced them.
    """
    client, runtime = metered
    free = key_headers(Tier.FREE)

    statuses = []
    for _ in range(70):
        statuses.append((await client.get(WHOAMI, headers=free)).status_code)
    for _ in range(3):
        statuses.append((await client.get(WHOAMI)).status_code)  # no credential -> 401

    total = len(statuses)
    allowed = statuses.count(200)
    denied = statuses.count(429)
    unauthorised = statuses.count(401)

    # The traffic really was mixed — otherwise this asserts about one path wearing three names.
    assert allowed > 0
    assert denied > 0
    assert unauthorised == 3
    assert allowed + denied + unauthorised == total

    minutes, hours = await buckets(runtime)

    assert minutes[RECORD_FIELD_REQUESTS] == total
    assert hours[RECORD_FIELD_REQUESTS] == total
    assert minutes[f"{RECORD_FIELD_STATUS_PREFIX}200"] == allowed
    assert minutes[f"{RECORD_FIELD_STATUS_PREFIX}429"] == denied
    assert minutes[f"{RECORD_FIELD_STATUS_PREFIX}401"] == unauthorised
    # The outcome dimension partitions the same traffic: 429s and 401s are both refusals.
    assert minutes[f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_ALLOWED}"] == allowed
    assert minutes[f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_DENIED}"] == denied + unauthorised
    assert runtime.analytics.records == total
    assert runtime.analytics.dropped == 0


async def test_an_exempt_path_is_not_recorded(metered):
    """``/health`` is not metered, so it is not counted either — and that has to stay true.

    The container ``HEALTHCHECK`` polls it every 10 seconds from one address. Recording it would
    put a constant, synthetic, unauthenticated stream into every chart and into the top-consumer
    ranking, permanently, on a replica that is otherwise idle. The exemption is a hole in the
    *enforcement* layer by design; it must be a hole in the analytics too, or the dashboard's
    busiest caller is the orchestrator.
    """
    client, runtime = metered

    for _ in range(5):
        assert (await client.get("/health")).status_code == 200

    minutes, _hours = await buckets(runtime)
    assert minutes == {}
    assert runtime.analytics.records == 0


# =============================================================================================
# 2. The 401 is recorded, under the anonymous sentinel
# =============================================================================================


async def test_a_401_is_recorded_under_the_anonymous_sentinel(metered):
    """An auth-failure flood is **exactly** what you want visible, and it is visible nowhere else.

    An unauthenticated request never reaches a bucket, a quota or a tier, so if the middleware
    dropped it here it would be absent from every counter this service keeps — and the dashboard's
    request total would silently disagree with the load balancer's for the one traffic pattern
    nobody plans for (a key-guessing flood, a client shipped with the wrong credential, a rotation
    that went wrong).

    The cost of the sentinel is that ``anonymous`` can top the consumer ranking during such a
    flood. That is not a defect; it is the finding.
    """
    client, runtime = metered

    for _ in range(4):
        assert (await client.get(WHOAMI)).status_code == 401
    assert (
        await client.get(WHOAMI, headers={"X-API-Key": "definitely-not-a-real-key"})
    ).status_code == 401

    minutes, _hours = await buckets(runtime)
    assert minutes[RECORD_FIELD_REQUESTS] == 5
    assert minutes[f"{RECORD_FIELD_STATUS_PREFIX}401"] == 5
    # No principal and no tier were ever established, and the record says so rather than guessing.
    assert minutes[f"{RECORD_FIELD_TIER_PREFIX}{UNKNOWN_TIER}"] == 5
    # The endpoint IS known, because classification runs above identity — which is what makes an
    # unauthenticated flood attributable to the path it is hammering.
    assert minutes[f"{RECORD_FIELD_ENDPOINT_PREFIX}GET:{WHOAMI}"] == 5

    snapshot = await runtime.analytics.snapshot(minutes=2, hours=1)
    assert [(entry.user_id, entry.cost) for entry in snapshot.top_consumers] == [
        (ANONYMOUS_USER_ID, 5)
    ]


# =============================================================================================
# 3. The recorded cost is the cost that was CHARGED
# =============================================================================================


async def test_the_recorded_cost_matches_the_weighted_cost_actually_charged(metered):
    """5 for ``/logs/query``, 2 for ``/logs/ingest``, 1 for ``/whoami`` — in the bucket.

    The weighted-cost bonus is only a feature if the number the limiter charges and the number the
    dashboard reports are the same one. They come from different places — the limiter is handed
    ``cost`` before the script runs, the collector is handed it after the response is sent — so a
    middleware that recomputed, defaulted or hard-coded either would produce a service that bills
    5 and reports 1, with no error anywhere and a top-consumer ranking that is quietly wrong.

    Cross-checked against the **quota counter** rather than only against the constants, because
    that counter is what the decision script itself charged: if the two agree, the reported cost is
    the charged cost by construction rather than by two literals matching.
    """
    client, runtime = metered
    premium = key_headers(Tier.PREMIUM)

    assert (await client.get(LOGS_QUERY, headers=premium)).status_code == 200
    assert (await client.post(LOGS_INGEST, headers=premium, json=INGEST_BODY)).status_code == 201
    assert (await client.get(WHOAMI, headers=premium)).status_code == 200

    expected_cost = COST_LOGS_QUERY + COST_LOGS_INGEST + COST_DEFAULT
    minutes, hours = await buckets(runtime)

    assert minutes[RECORD_FIELD_REQUESTS] == 3
    assert minutes[RECORD_FIELD_COST] == expected_cost
    assert hours[RECORD_FIELD_COST] == expected_cost
    assert minutes[f"{RECORD_FIELD_TIER_PREFIX}premium"] == 3
    # Three requests, three distinct classified labels — never a raw path, and never collapsed.
    assert minutes[f"{RECORD_FIELD_ENDPOINT_PREFIX}GET:{LOGS_QUERY}"] == 1
    assert minutes[f"{RECORD_FIELD_ENDPOINT_PREFIX}POST:{LOGS_INGEST}"] == 1
    assert minutes[f"{RECORD_FIELD_ENDPOINT_PREFIX}GET:{WHOAMI}"] == 1

    # The independent check: the daily quota counter the Lua script incremented. Same 8.
    charged = await runtime.redis.client.get("quota:daily:{demo-premium}:" + _utc_day())
    assert int(charged) == expected_cost

    # And the same number reaches the read side, which is what C11 will serve.
    snapshot = await runtime.analytics.snapshot(minutes=2, hours=1)
    assert snapshot.totals.cost == expected_cost
    assert snapshot.totals.requests == 3
    assert [(entry.user_id, entry.cost) for entry in snapshot.top_consumers] == [
        ("demo-premium", expected_cost)
    ]


async def test_a_refused_request_still_records_the_cost_it_tried_to_spend(metered):
    """A 429 is recorded at its **weighted** cost even though the script charged nothing.

    The two numbers genuinely differ here, and both are right for their own purpose. The quota
    counter must not move — a denial writes nothing, which is the property C4 exists to guarantee —
    while the analytics ``cost`` series is a measure of *demand*, and a caller hammering the 5-token
    endpoint is generating five times the load of one hammering ``/whoami`` whether or not the
    limiter lets them through. Recording refusals at cost 1 would make the endpoint that costs the
    most look like the cheapest as soon as it started being throttled — precisely backwards.
    """
    client, runtime = metered
    free = key_headers(Tier.FREE)

    statuses = [
        (await client.get(LOGS_QUERY, headers=free)).status_code for _ in range(20)
    ]
    allowed = statuses.count(200)
    denied = statuses.count(429)
    assert allowed and denied, "the burst must produce both outcomes for this to mean anything"

    minutes, _hours = await buckets(runtime)

    assert minutes[RECORD_FIELD_REQUESTS] == allowed + denied
    assert minutes[RECORD_FIELD_COST] == (allowed + denied) * COST_LOGS_QUERY
    # ...while the quota counter only moved for the requests that were actually admitted.
    charged = await runtime.redis.client.get("quota:daily:{demo-free}:" + _utc_day())
    assert int(charged) == allowed * COST_LOGS_QUERY


# =============================================================================================
# 4. The bucket a request lands in is the one the DECISION's clock names
# =============================================================================================


async def test_a_handler_that_raises_is_recorded_as_a_500(redis_settings: Settings):
    """**The failure class most worth seeing** — and the one the seam's placement almost lost.

    An unhandled handler exception never comes back as a status code here.
    ``ExceptionMiddleware`` sits *below* this middleware and only handles what it was registered
    for; everything else propagates up past us to ``ServerErrorMiddleware``, which is registered
    **outside** us and is what actually writes the 500. So ``send_wrapper`` is never called, the
    captured status is still 0, and a record placed only after a normal return never happens —
    while the request was metered, the quota counter was charged, and the client got a 500.

    Verified before the fix: 500 served, quota 1 -> 2, ``records`` unchanged, no ``status:500``
    field ever written. ``totals.requests`` silently under-reported every server error.
    """
    runtime = Runtime.build(redis_settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()

    app: FastAPI = create_app(runtime=runtime)

    @app.get(BOOM)
    async def boom() -> dict[str, str]:  # pragma: no cover - raises before returning
        raise RuntimeError("handler exploded")

    # `raise_app_exceptions=False` so the transport behaves like a real server: it lets
    # ServerErrorMiddleware turn the exception into a 500 response instead of re-raising it into
    # the test, which is the behaviour a client actually sees.
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    try:
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            response = await client.get(BOOM, headers=key_headers(Tier.FREE))

        assert response.status_code == 500
        minutes, hours = await buckets(runtime)

        # The 500 is ON the graph, under its own status, and it counts toward the totals.
        assert minutes[f"{RECORD_FIELD_STATUS_PREFIX}500"] == 1
        assert minutes[RECORD_FIELD_REQUESTS] == 1
        assert hours[f"{RECORD_FIELD_STATUS_PREFIX}500"] == 1
        # It was ALLOWED by the limiter — the handler failed afterwards — so the outcome dimension
        # says `allowed` and only the status says the request failed. Both facts are true and the
        # two dimensions are what keep them separable.
        assert minutes[f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_ALLOWED}"] == 1
        # And the request really was charged, which is what makes the missing record a discrepancy
        # rather than a rounding difference: quota moved, the dashboard did not.
        charged = await runtime.redis.client.get("quota:daily:{demo-free}:" + _utc_day())
        assert int(charged) == 1
        assert runtime.analytics.records == 1
    finally:
        try:
            await runtime.redis.client.flushdb()
        finally:
            await runtime.stop()


async def test_a_handler_exception_still_propagates(redis_settings: Settings):
    """Recording is additive: it must not swallow the exception on its way past.

    ``ServerErrorMiddleware`` has to keep seeing the original error — it is what logs the traceback
    and what a debug-mode deployment renders. An analytics hook that quietly ate a server error
    would be a far worse bug than the missing data point it was added to fix.
    """
    runtime = Runtime.build(redis_settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()

    app: FastAPI = create_app(runtime=runtime)

    @app.get(BOOM)
    async def boom() -> dict[str, str]:  # pragma: no cover - raises before returning
        raise RuntimeError("handler exploded")

    # `raise_app_exceptions=True` (the default) re-raises whatever escaped the app, so this asserts
    # on the exception object itself rather than on a status code.
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            with pytest.raises(RuntimeError, match="handler exploded"):
                await client.get(BOOM, headers=key_headers(Tier.FREE))

        # Recorded on the way past, even though the exception carried on.
        assert runtime.analytics.records == 1
    finally:
        try:
            await runtime.redis.client.flushdb()
        finally:
            await runtime.stop()


async def test_requests_land_in_the_bucket_named_by_redis_own_clock(metered):
    """The minute a request is recorded in is Redis's minute, not this process's.

    Asserted by taking the instant from ``TIME`` — the same call the decision script makes — and
    checking the bucket exists under that index. Two replicas doing this cannot disagree, which is
    the entire reason the index comes off ``LimitDecision.server_now_ms``.
    """
    client, runtime = metered

    seconds, microseconds = await runtime.redis.client.time()
    now_ms = int(seconds) * 1000 + int(microseconds) // 1000
    assert (await client.get(WHOAMI, headers=key_headers(Tier.FREE))).status_code == 200

    minute = await runtime.redis.client.hgetall(stats_minute_key(minute_index(now_ms)))
    hour = await runtime.redis.client.hgetall(stats_hour_key(hour_index(now_ms)))
    assert minute.get(RECORD_FIELD_REQUESTS.encode()) == b"1"
    assert hour.get(RECORD_FIELD_REQUESTS.encode()) == b"1"


def _utc_day() -> str:
    """Today's UTC date stamp, for the quota key the decision script wrote.

    Built through :func:`src.keys.day_string` rather than by formatting a date here, so a test that
    reads a quota counter and the code that writes one cannot disagree about what a day is called.
    """
    return day_string(datetime.now(timezone.utc))
