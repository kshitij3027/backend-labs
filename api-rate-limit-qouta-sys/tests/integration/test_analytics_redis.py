"""The analytics collector against a real ``redis:7-alpine``: the script, the TTLs, the ZSET.

``tests/unit/test_analytics.py`` proves the collector's *arithmetic* against a stub — which keys it
builds, which clock it reads, which exceptions it survives. This file proves the half a stub
structurally cannot, because a stub answers with this module's own beliefs:

* **``EXPIRE ... NX`` really does not extend a TTL.** That is a property of Redis 7's ``EXPIRE``
  flags, not of the Python around them, and it is the single assertion behind "minute buckets are
  retained for an hour" not silently meaning "an hour after traffic stops".
* **``HINCRBY`` on a shared key sums across processes.** Two collectors, two clients, two pools,
  one bucket. This is the two-replica property that the whole project is organised around, applied
  to the analytics keys.
* **``ZINCRBY`` + ``ZREVRANGE`` rank by score.** A stub's ``sorted()`` proves the test's sort, not
  Redis's.

``fakeredis`` is deliberately not used for any of it, for the same reason C4's Lua assertions are
not: it is a reimplementation whose ``EXPIRE`` flag handling, float coercion and Lua->RESP rules are
approximations, and an approximation is the wrong oracle for a script whose job is exactness.
"""

from __future__ import annotations

import asyncio
import dataclasses

import httpx
import pytest
from fastapi import FastAPI

from src.analytics import (
    ANONYMOUS_USER_ID,
    OUTCOME_ALLOWED,
    OUTCOME_DEGRADED,
    OUTCOME_DENIED,
    UNKNOWN_TIER,
    AnalyticsCollector,
)
from src.config import Settings
from src.identity import DEMO_KEY_BY_TIER
from src.keys import hour_index, minute_index, stats_hour_key, stats_minute_key, stats_top_key
from src.lua import (
    RECORD_FIELD_COST,
    RECORD_FIELD_ENDPOINT_PREFIX,
    RECORD_FIELD_OUTCOME_PREFIX,
    RECORD_FIELD_REQUESTS,
    RECORD_FIELD_STATUS_PREFIX,
    RECORD_FIELD_TIER_PREFIX,
)
from src.main import Runtime, create_app
from src.models import DenyReason, LimitDecision, QuotaPeriodState, Tier
from src.redis_client import RedisGateway

#: A port nothing listens on inside the test container, so a connect is *refused* by the kernel in
#: microseconds. Same value and same reasoning as ``tests/integration/test_degradation.py``.
DEAD_URL = "redis://127.0.0.1:6390/0"

#: The metered probe. Cost 1, so a request count and a weighted total are the same number and a
#: failed assertion says which one is wrong.
WHOAMI = "/api/v1/whoami"

#: The shipped minute retention, in seconds. Written out rather than read from ``Settings`` in the
#: TTL test: the plan names 3600 specifically, so the assertion has to fail if the default moves
#: rather than following it.
SPEC_MINUTE_TTL_SEC = 3600


# =============================================================================================
# Fixtures and helpers
# =============================================================================================


@pytest.fixture()
async def collector(gateway: RedisGateway, redis_settings: Settings) -> AnalyticsCollector:
    """A collector on the flushed session gateway. The script registers in its constructor."""
    return AnalyticsCollector(gateway, redis_settings)


def decision(**overrides) -> LimitDecision:
    """A plausible decision. ``server_now_ms`` is the field every test here actually chooses."""
    base = {
        "allowed": True,
        "reason": DenyReason.NONE,
        "tier": "free",
        "user_id": "alice",
        "endpoint": "GET:/api/v1/whoami",
        "cost": 1,
        "bucket_limit": 60,
        "bucket_remaining": 59,
        "bucket_reset_sec": 1,
        "window_limit": 60,
        "window_used": 1,
        "window_reset_sec": 59,
        "daily_limit": 1000,
        "daily_used": 1,
        "daily_reset_at": 1_786_752_000,
        "daily_state": QuotaPeriodState.ACTIVE,
        "monthly_limit": 25_000,
        "monthly_used": 1,
        "monthly_reset_at": 1_788_220_800,
        "monthly_state": QuotaPeriodState.ACTIVE,
        "retry_after_sec": 0,
        "degraded": False,
        "server_now_ms": 0,
        "latency_ms": 0.4,
    }
    base.update(overrides)
    return LimitDecision(**base)  # type: ignore[arg-type]


async def server_now_ms(gateway: RedisGateway) -> int:
    """Redis's own clock in epoch milliseconds — the same source the decision script reads.

    Used rather than ``time.time()`` so a test's bucket and the script's bucket are the same one by
    construction. The gap is normally microseconds; taking it from the server means the test does
    not have a second clock that can straddle a minute boundary.
    """
    seconds, microseconds = await gateway.client.time()
    return int(seconds) * 1000 + int(microseconds) // 1000


async def fields(gateway: RedisGateway, key: str) -> dict[str, int]:
    """One bucket's hash, decoded to ``{field: count}``."""
    raw = await gateway.client.hgetall(key)
    return {name.decode(): int(value) for name, value in raw.items()}


# =============================================================================================
# 1. One record lands in BOTH buckets, with every dimension
# =============================================================================================


async def test_a_record_lands_in_both_the_minute_and_the_hour_bucket(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """One call, six fields, two buckets — and the two carry the identical picture.

    Both granularities are written by the same ``fold`` in the script rather than by two code
    paths, which is what makes "the hour bucket is the minute buckets summed" true by construction
    instead of by a reconciliation job nobody wrote.
    """
    now_ms = await server_now_ms(gateway)

    landed = await collector.record(
        decision(server_now_ms=now_ms),
        status_code=200,
        user_id="alice",
        endpoint="GET:/api/v1/logs/query",
        tier="premium",
        cost=5,
    )

    assert landed is True
    assert collector.records == 1
    assert collector.dropped == 0

    expected = {
        RECORD_FIELD_REQUESTS: 1,
        RECORD_FIELD_COST: 5,
        f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_ALLOWED}": 1,
        f"{RECORD_FIELD_TIER_PREFIX}premium": 1,
        f"{RECORD_FIELD_ENDPOINT_PREFIX}GET:/api/v1/logs/query": 1,
        f"{RECORD_FIELD_STATUS_PREFIX}200": 1,
    }
    assert await fields(gateway, stats_minute_key(minute_index(now_ms))) == expected
    assert await fields(gateway, stats_hour_key(hour_index(now_ms))) == expected

    # The ZSET is scored by COST, not by 1 per request.
    top = await gateway.client.zrange(stats_top_key(minute_index(now_ms)), 0, -1, withscores=True)
    assert top == [(b"alice", 5.0)]


async def test_the_hour_bucket_accumulates_across_minutes(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """Three records spread over three minutes: three minute buckets, one hour bucket holding all.

    This is what makes the hour series the long-tail context line the dashboard draws behind the
    live one — and it is also why the read side folds its totals from ONE granularity: both series
    describe the same requests, so counting both would double every number.
    """
    now_ms = await server_now_ms(gateway)
    # A base instant far enough inside the hour that minus two minutes cannot cross into the
    # previous one; otherwise this would flake once an hour.
    base_ms = (hour_index(now_ms) * 3_600_000) + 30 * 60_000

    for offset in range(3):
        await collector.record(
            decision(server_now_ms=base_ms + offset * 60_000),
            status_code=200,
            user_id="alice",
            endpoint="GET:/api/v1/whoami",
            tier="free",
            cost=1,
        )

    for offset in range(3):
        minute = await fields(gateway, stats_minute_key(minute_index(base_ms + offset * 60_000)))
        assert minute[RECORD_FIELD_REQUESTS] == 1

    hour = await fields(gateway, stats_hour_key(hour_index(base_ms)))
    assert hour[RECORD_FIELD_REQUESTS] == 3
    assert hour[RECORD_FIELD_COST] == 3


# =============================================================================================
# 2. EXPIRE ... NX — the property the plan calls out
# =============================================================================================


async def test_the_minute_bucket_ttl_is_the_specs_3600_seconds(
    collector: AnalyticsCollector, gateway: RedisGateway, redis_settings: Settings
):
    """3600 s on the minute bucket, ``ANALYTICS_HOUR_TTL_SEC`` on the hour one, and the ZSET tracks
    the minute.

    The ZSET's TTL matching the minute bucket's is not cosmetic: it is a *view of* that minute, so a
    longer TTL would leave a top-consumer ranking for a minute whose totals had already expired — a
    numerator with no denominator — and a shorter one would make the list vanish while the chart
    was still drawing the minute.
    """
    now_ms = await server_now_ms(gateway)
    await collector.record(
        decision(server_now_ms=now_ms),
        status_code=200, user_id="alice", endpoint=WHOAMI, tier="free", cost=1,
    )

    minute_key = stats_minute_key(minute_index(now_ms))
    assert await gateway.client.ttl(minute_key) == SPEC_MINUTE_TTL_SEC
    assert redis_settings.analytics_minute_ttl_sec == SPEC_MINUTE_TTL_SEC
    assert await gateway.client.ttl(stats_hour_key(hour_index(now_ms))) == (
        redis_settings.analytics_hour_ttl_sec
    )
    assert await gateway.client.ttl(stats_top_key(minute_index(now_ms))) == SPEC_MINUTE_TTL_SEC


async def test_a_second_write_does_not_re_arm_the_ttl(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """**The NX property, measured.** The TTL counts down from bucket CREATION, not from last write.

    Without ``NX`` every write would reset the countdown to the full hour, so a continuously hot
    minute bucket would live an hour past its last write and "minute buckets are retained for an
    hour" would silently become "an hour after traffic stops" — unbounded retention under exactly
    the load that produces the most buckets, and a bug with no error message anywhere.

    Deliberately measured by **waiting**, which is the plan's own formulation and the only version
    that is a statement about elapsed time rather than about a TTL somebody set by hand. The
    companion test below removes the timing dependence entirely.
    """
    now_ms = await server_now_ms(gateway)
    key = stats_minute_key(minute_index(now_ms))
    write = dict(status_code=200, user_id="alice", endpoint=WHOAMI, tier="free", cost=1)

    await collector.record(decision(server_now_ms=now_ms), **write)
    first = await gateway.client.pttl(key)

    await asyncio.sleep(1.1)
    await collector.record(decision(server_now_ms=now_ms), **write)
    second = await gateway.client.pttl(key)

    # The bucket really was written twice — otherwise this would be asserting about a no-op.
    assert (await fields(gateway, key))[RECORD_FIELD_REQUESTS] == 2
    # And the clock kept running through the second write instead of restarting.
    assert second < first
    assert second <= SPEC_MINUTE_TTL_SEC * 1000 - 1000


async def test_expire_nx_leaves_an_existing_ttl_alone_whatever_its_value(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """The same property with the timing removed: an unrelated, much shorter TTL survives a write.

    ``PEXPIRE`` sets the bucket to five seconds; the next record must leave it there. Without
    ``NX`` the TTL would jump straight back to 3 600 000 ms, so the two outcomes are three orders
    of magnitude apart and no amount of scheduler jitter can confuse them. This is the assertion
    that would still be decisive on a loaded CI machine where a 1.1 s sleep is 1.9 s.
    """
    now_ms = await server_now_ms(gateway)
    key = stats_minute_key(minute_index(now_ms))
    write = dict(status_code=200, user_id="alice", endpoint=WHOAMI, tier="free", cost=1)

    await collector.record(decision(server_now_ms=now_ms), **write)
    await gateway.client.pexpire(key, 5_000)

    await collector.record(decision(server_now_ms=now_ms), **write)

    remaining = await gateway.client.pttl(key)
    assert 0 < remaining <= 5_000, "EXPIRE ... NX re-armed a TTL it was supposed to leave alone"


# =============================================================================================
# 3. Two collectors, one bucket — the two-replica property
# =============================================================================================


async def test_two_collectors_on_one_redis_sum_into_the_same_bucket(
    gateway: RedisGateway, redis_settings: Settings
):
    """**The property the whole project is about**, applied to the analytics keys.

    Two collectors, each with its own :class:`~src.redis_client.RedisGateway` — its own client and
    its own connection pool, which is what makes them a stand-in for two replicas rather than for
    two objects — recording the same instant. The bucket holds the sum.

    Both are given the *same* ``server_now_ms``, and that is the point rather than a convenience:
    it stands in for two replicas reading one ``redis.call('TIME')``. Bucketing on each process's
    own wall clock is what would put one instant into two different minutes and produce the
    permanent saw-tooth this design removes.
    """
    replica_a = AnalyticsCollector(gateway, redis_settings)
    other = RedisGateway(redis_settings)
    await other.connect()
    replica_b = AnalyticsCollector(other, redis_settings)
    assert other.client is not gateway.client

    try:
        now_ms = await server_now_ms(gateway)
        await replica_a.record(
            decision(server_now_ms=now_ms),
            status_code=200, user_id="alice", endpoint=WHOAMI, tier="free", cost=1,
        )
        await replica_b.record(
            decision(server_now_ms=now_ms),
            status_code=200, user_id="bob", endpoint=WHOAMI, tier="premium", cost=4,
        )

        bucket = await fields(gateway, stats_minute_key(minute_index(now_ms)))
    finally:
        await other.aclose()

    assert bucket[RECORD_FIELD_REQUESTS] == 2
    assert bucket[RECORD_FIELD_COST] == 5
    assert bucket[f"{RECORD_FIELD_TIER_PREFIX}free"] == 1
    assert bucket[f"{RECORD_FIELD_TIER_PREFIX}premium"] == 1

    # And a snapshot taken from either one sees both, which is what a dashboard behind a load
    # balancer actually depends on.
    snapshot = await replica_a.snapshot(minutes=2, hours=1)
    assert snapshot.totals.requests == 2
    assert snapshot.totals.cost == 5
    assert snapshot.by_tier == {"free": 1, "premium": 1}


# =============================================================================================
# 4. The outcome dimension
# =============================================================================================


async def test_a_429_is_counted_as_denied_and_a_degraded_request_as_degraded(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """The three outcomes partition the traffic, and ``degraded`` outranks the other two.

    A degraded **429** counts as ``degraded`` rather than ``denied``, because during an outage the
    question worth answering is "how much of this was still being metered authoritatively?" — and
    the refusal itself is not lost, because ``status:429`` carries it. Asserting both facts in one
    test is what keeps the taxonomy from being described one way and implemented another.
    """
    now_ms = await server_now_ms(gateway)
    common = dict(user_id="alice", endpoint=WHOAMI, tier="free", cost=1)

    await collector.record(decision(server_now_ms=now_ms), status_code=200, **common)
    await collector.record(
        decision(server_now_ms=now_ms, allowed=False, reason=DenyReason.RATE_LIMIT,
                 retry_after_sec=3),
        status_code=429, **common,
    )
    await collector.record(
        decision(server_now_ms=now_ms, degraded=True), status_code=200, **common
    )
    await collector.record(
        decision(server_now_ms=now_ms, degraded=True, allowed=False,
                 reason=DenyReason.RATE_LIMIT, retry_after_sec=3),
        status_code=429, **common,
    )
    # A 401 has no decision at all: the anonymous sentinel, counted as a refusal.
    await collector.record(
        None, status_code=401, user_id=ANONYMOUS_USER_ID, endpoint=WHOAMI,
        tier=UNKNOWN_TIER, cost=1, now_ms=now_ms,
    )

    bucket = await fields(gateway, stats_minute_key(minute_index(now_ms)))
    assert bucket[f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_ALLOWED}"] == 1
    assert bucket[f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_DENIED}"] == 2
    assert bucket[f"{RECORD_FIELD_OUTCOME_PREFIX}{OUTCOME_DEGRADED}"] == 2
    # A partition: the three sum to the request count.
    assert bucket[RECORD_FIELD_REQUESTS] == 5
    # ...and both refusals are still visible as refusals through the status dimension.
    assert bucket[f"{RECORD_FIELD_STATUS_PREFIX}429"] == 2
    assert bucket[f"{RECORD_FIELD_STATUS_PREFIX}401"] == 1
    assert bucket[f"{RECORD_FIELD_TIER_PREFIX}{UNKNOWN_TIER}"] == 1


# =============================================================================================
# 5. Top consumers rank by COST
# =============================================================================================


async def test_top_consumers_ranks_by_cost_and_not_by_request_count(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """The heavy caller is the one consuming the most **units of work**, not the most requests.

    Driven with genuinely different weights so the two rankings disagree: ``heavy`` makes three
    5-cost calls (15 units) and ``chatty`` makes ten 1-cost calls (10 units). Ranked by request
    count, ``chatty`` would be top and an operator would call the wrong client. That is the whole
    reason the ZSET is scored with ``ZINCRBY cost`` rather than ``ZINCRBY 1``.
    """
    now_ms = await server_now_ms(gateway)

    for _ in range(3):
        await collector.record(
            decision(server_now_ms=now_ms),
            status_code=200, user_id="heavy", endpoint="GET:/api/v1/logs/query",
            tier="premium", cost=5,
        )
    for _ in range(10):
        await collector.record(
            decision(server_now_ms=now_ms),
            status_code=200, user_id="chatty", endpoint=WHOAMI, tier="free", cost=1,
        )

    snapshot = await collector.snapshot(minutes=1, hours=1)

    assert [(entry.user_id, entry.cost) for entry in snapshot.top_consumers] == [
        ("heavy", 15),
        ("chatty", 10),
    ]
    # The request count really does rank the other way round — otherwise the test proves nothing.
    assert snapshot.by_endpoint == {"GET:/api/v1/logs/query": 3, WHOAMI: 10}
    assert snapshot.totals.requests == 13
    assert snapshot.totals.cost == 25


# =============================================================================================
# 6. The read side, against real keys
# =============================================================================================


async def test_snapshot_reads_back_what_was_written_and_never_scans(
    collector: AnalyticsCollector, gateway: RedisGateway
):
    """A round trip through real keys, with ``MONITOR``-grade proof that nothing scanned.

    ``INFO commandstats`` is the server's own tally of what it executed, so this is not the
    collector reporting on itself: a ``SCAN`` or a ``KEYS`` issued by anyone during the snapshot
    would appear there. The counters are reset first so the window is exactly this test's.
    """
    now_ms = await server_now_ms(gateway)
    for index in range(4):
        await collector.record(
            decision(server_now_ms=now_ms),
            status_code=200 if index else 429,
            user_id=f"user-{index}", endpoint=WHOAMI, tier="free", cost=index + 1,
        )

    await gateway.client.config_resetstat()
    snapshot = await collector.snapshot(minutes=5, hours=2)
    stats = await gateway.client.info("commandstats")

    assert snapshot.totals.requests == 4
    assert snapshot.totals.cost == 1 + 2 + 3 + 4
    assert snapshot.by_status == {"200": 3, "429": 1}
    assert snapshot.buckets_read == 7
    assert snapshot.dropped == 0
    assert snapshot.window.minutes_covered == 5
    assert snapshot.window.hours_covered == 2
    # The newest minute bucket is the last point of the series, and it is the one with traffic.
    assert snapshot.per_minute[-1].index == minute_index(now_ms)
    assert snapshot.per_minute[-1].requests == 4

    # The server's own record of what it ran: HGETALL and ZREVRANGE, no SCAN and no KEYS.
    issued = {name.removeprefix("cmdstat_").upper() for name in stats}
    assert "SCAN" not in issued
    assert "KEYS" not in issued
    assert {"HGETALL", "ZREVRANGE"} <= issued


async def test_a_window_with_no_traffic_is_zeros_rather_than_an_error(
    collector: AnalyticsCollector,
):
    """Buckets that were never written come back as empty hashes and fold to zeros.

    That is the whole reason the read side can compute key names instead of discovering them: a
    missing bucket is not an error condition to be detected, it is an empty answer that costs one
    pipelined ``HGETALL``.
    """
    snapshot = await collector.snapshot(minutes=10, hours=3)

    assert snapshot.totals.requests == 0
    assert snapshot.totals.cost == 0
    assert len(snapshot.per_minute) == 10
    assert len(snapshot.per_hour) == 3
    assert all(bucket.requests == 0 for bucket in snapshot.per_minute)
    assert snapshot.top_consumers == []
    assert snapshot.by_status == {}


# =============================================================================================
# 7. An analytics failure never fails a request
# =============================================================================================


@pytest.fixture()
async def app_with_broken_analytics(redis_settings: Settings):
    """A real app on a real Redis, whose **analytics collector alone** points at a dead port.

    Built with :func:`dataclasses.replace` off a real ``Runtime.build``, so the limiter, the
    identity resolver and the tier registry all keep talking to the live server and only the
    analytics write fails. That isolation is the whole design of the fixture: pointing the *whole*
    runtime at a dead Redis would exercise C8's degradation and prove nothing about analytics.
    """
    runtime = Runtime.build(redis_settings)
    await runtime.redis.connect()
    await runtime.redis.client.flushdb()
    await runtime.start()

    broken = RedisGateway(redis_settings.model_copy(update={"redis_url": DEAD_URL}))
    await broken.connect()
    runtime = dataclasses.replace(
        runtime, analytics=AnalyticsCollector(broken, redis_settings)
    )

    app: FastAPI = create_app(runtime=runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            await broken.aclose()
            try:
                await runtime.redis.client.flushdb()
            finally:
                await runtime.stop()


async def test_an_analytics_outage_never_changes_a_response(app_with_broken_analytics):
    """**The rule this module exists under**, asserted through the whole HTTP stack.

    Every analytics write fails — connection refused, on every request — and the caller cannot
    tell. Same status, same body, same headers as a healthy replica would produce, because the
    record fires after the response is already on the wire and swallows whatever it hits.

    The counters are checked too, because "swallowed" must not mean "invisible": a collector that
    silently recorded nothing and one that is working look identical from outside, which is exactly
    why :attr:`~src.analytics.AnalyticsCollector.dropped` and ``last_error`` exist.
    """
    client, runtime = app_with_broken_analytics
    headers = {"X-API-Key": DEMO_KEY_BY_TIER[Tier.FREE]}

    responses = [await client.get(WHOAMI, headers=headers) for _ in range(5)]

    assert [response.status_code for response in responses] == [200] * 5
    assert all(response.json()["user_id"] == "demo-free" for response in responses)
    # The limiter was working the whole time — the headers come from a real decision on the live
    # store — so this really is an analytics-only failure.
    assert all("X-RateLimit-Remaining" in response.headers for response in responses)
    assert all("X-RateLimit-Degraded" not in response.headers for response in responses)

    assert runtime.analytics.records == 0
    assert runtime.analytics.dropped == 5
    assert runtime.analytics.errors == 5
    assert runtime.analytics.last_error is not None


async def test_a_401_is_still_served_normally_when_analytics_is_down(
    app_with_broken_analytics,
):
    """The refusal paths are covered by the same rule, and they are the ones that record most.

    A 401 fires an analytics record too (under the anonymous sentinel), so a collector that raised
    would break the *rejection* path — the one an unauthenticated flood consists entirely of, and
    therefore the one where a crash would be most expensive.
    """
    client, runtime = app_with_broken_analytics

    unauthorised = await client.get(WHOAMI)
    bad_key = await client.get(WHOAMI, headers={"X-API-Key": "nope-not-a-real-key"})

    assert unauthorised.status_code == 401
    assert bad_key.status_code == 401
    assert "WWW-Authenticate" in unauthorised.headers
    assert runtime.analytics.dropped == 2
