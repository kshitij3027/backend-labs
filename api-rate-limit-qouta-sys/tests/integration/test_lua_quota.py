"""Gates 3 and 4 — the daily and monthly quota counters — against a REAL ``redis:7-alpine``.

The headline test in this file is
:func:`test_a_quota_rejection_does_not_spend_a_token`. It is the one that proves the whole reason
there is a single script rather than two:

    A caller refused for being out of quota must not have been charged a token on the way to being
    refused.

With two round trips — "check the rate, then check the quota" — that property is unobtainable.
Whichever check runs first has already mutated state by the time the second one refuses, and
undoing it needs a compensating write that can itself fail, on a caller who was never served. The
cost is invisible in aggregate and vicious in the specific case it hits: a client that has
exhausted its daily allowance keeps burning burst capacity on requests it is not being given, so
when the quota rolls over at midnight it discovers it is *also* rate limited.

Everything else here is about arithmetic that has to be exact — an ``EXPIREAT`` that lands on the
right UTC instant, a counter that moves by the request's weight rather than by one, and the
``limit <= 0`` sentinel that means "unlimited" rather than "you have nothing left".
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

from src.keys import day_expire_at, month_expire_at
from src.models import UNLIMITED, QuotaPeriodState
from tests.integration.conftest import (
    Reply,
    ScriptDriver,
    TierRow,
    moment_of,
    tokens_of,
)

#: Seconds in a UTC day. A period boundary is a whole number of these since the epoch — that is
#: what "midnight UTC" means numerically, and it is the cheapest assertion that catches an expiry
#: landing at the right *duration* away but the wrong *instant* (a relative TTL wearing an
#: absolute one's clothes).
SECONDS_PER_DAY = 86_400

#: One fixed instant and the two boundaries it implies, as **literals**. Every other assertion in
#: this file derives its expectation from the calendar at run time, which is necessary (the tests
#: run on whatever day CI runs on) but leaves nothing absolute pinned. These three lines are the
#: absolute statement: 2026-08-12T00:00:00Z is 1786492800 and 2026-09-01T00:00:00Z is 1788220800,
#: full stop, and no helper in ``src/`` participates in producing them.
REFERENCE_MOMENT = datetime(2026, 8, 11, 13, 0, tzinfo=timezone.utc)
REFERENCE_DAY_BOUNDARY = 1_786_492_800
REFERENCE_MONTH_BOUNDARY = 1_788_220_800

#: rpm/burst big enough that gates 1 and 2 can never be the reason for anything in this file.
ROOMY_RATE = 100_000

#: Tolerance, in seconds, on a TTL read back from the server. The quota expiry is computed in
#: Python from a frozen ``now_ms`` while the TTL is computed by Redis from its own clock, and the
#: two are the same clock only to within however long the test took to get here.
TTL_SLOP_SEC = 10


def tiers(*, daily: int, monthly: int) -> tuple[TierRow, ...]:
    return (
        TierRow(
            name="free", rpm=ROOMY_RATE, burst=ROOMY_RATE, daily=daily, monthly=monthly
        ),
    )


# ---------------------------------------------------------------------------------------------
# Daily
# ---------------------------------------------------------------------------------------------


async def test_the_daily_quota_admits_exactly_its_limit_then_denies(
    driver: ScriptDriver, now_ms: int
):
    table = tiers(daily=3, monthly=1_000)

    allowed = await driver.drain("day", attempts=6, now_ms=now_ms, tiers=table)
    reply = await driver.call("day", now_ms=now_ms, tiers=table)

    assert allowed == 3
    assert reply.allowed is False
    assert reply.reason == "quota_daily"
    assert reply.decision.daily_limit == 3
    assert reply.decision.daily_used == 3
    assert reply.decision.daily_remaining == 0


async def test_remaining_counts_down_as_the_quota_is_spent(driver: ScriptDriver, now_ms: int):
    """Reported on every response, not only on rejection — a client cannot pace itself off a
    number it receives only after it has already been refused."""
    table = tiers(daily=5, monthly=1_000)

    seen = []
    for _ in range(3):
        seen.append((await driver.call("countdown", now_ms=now_ms, tiers=table)).decision)

    assert [decision.daily_used for decision in seen] == [1, 2, 3]
    assert [decision.daily_remaining for decision in seen] == [4, 3, 2]


async def test_an_arbitrary_cost_moves_the_counter_by_that_amount(
    driver: ScriptDriver, gateway, now_ms: int
):
    """``INCRBY cost``, not ``INCR``. A ``/logs/query`` costs five requests' worth of allowance."""
    table = tiers(daily=100, monthly=1_000)

    reply = await driver.call("weighted", now_ms=now_ms, cost=5, tiers=table)

    assert reply.decision.daily_used == 5
    assert reply.decision.daily_remaining == 95
    assert await gateway.client.get(driver.daily("weighted", now_ms)) == b"5"


async def test_a_cost_that_would_overshoot_the_limit_is_refused_rather_than_clipped(
    driver: ScriptDriver, gateway, now_ms: int
):
    """A request is all-or-nothing. Admitting it "up to the limit" would serve work nobody paid for
    and leave the counter unable to say what actually happened."""
    table = tiers(daily=10, monthly=1_000)
    await driver.call("overshoot", now_ms=now_ms, cost=8, tiers=table)

    reply = await driver.call("overshoot", now_ms=now_ms, cost=5, tiers=table)

    assert reply.allowed is False
    assert reply.reason == "quota_daily"
    assert await gateway.client.get(driver.daily("overshoot", now_ms)) == b"8"


# ---------------------------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------------------------


async def test_the_daily_counter_expires_at_the_next_utc_midnight(
    driver: ScriptDriver, gateway, now_ms: int
):
    """``EXPIREAT`` an absolute instant, not ``EXPIRE`` a duration.

    A relative 86 400 s TTL applied on first write would keep a counter created at 18:00 alive
    until 18:00 the *next* day — so it would still be there, half-spent, when the new day's key was
    created, and the allowance would never actually reset for anyone whose usage straddles
    midnight. An absolute instant is also idempotent, so it can be reissued on every request with
    no "did I already set this?" bookkeeping and no INCR/EXPIRE race between replicas.
    """
    await driver.call("expiry", now_ms=now_ms, tiers=tiers(daily=100, monthly=1_000))

    # EXPIRETIME reads back the ABSOLUTE instant Redis stored. Reading the TTL and comparing it to
    # `day_expire_at(...)` — the very helper that produced the ARGV — would only prove pass-through:
    # the same wrong boundary on both sides of the assertion agrees with itself.
    expire_at = await gateway.client.expiretime(driver.daily("expiry", now_ms))

    # Derived here by plain calendar arithmetic, so `src.keys` is not on both sides of this either.
    tomorrow = moment_of(now_ms).date() + timedelta(days=1)
    assert datetime.fromtimestamp(expire_at, tz=timezone.utc) == datetime.combine(
        tomorrow, time.min, tzinfo=timezone.utc
    )
    # A UTC midnight is an exact multiple of a day since the epoch. This is what separates an
    # absolute EXPIREAT from a relative `EXPIRE 86400` applied at some arbitrary time of day.
    assert expire_at % SECONDS_PER_DAY == 0

    ttl = await gateway.client.ttl(driver.daily("expiry", now_ms))
    assert 0 < ttl <= SECONDS_PER_DAY


async def test_the_monthly_counter_expires_on_the_first_of_next_utc_month(
    driver: ScriptDriver, gateway, now_ms: int
):
    await driver.call("expiry", now_ms=now_ms, tiers=tiers(daily=100, monthly=1_000))

    expire_at = await gateway.client.expiretime(driver.monthly("expiry", now_ms))

    today = moment_of(now_ms).date()
    year, month = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)
    boundary = datetime.fromtimestamp(expire_at, tz=timezone.utc)
    assert boundary == datetime(year, month, 1, tzinfo=timezone.utc)
    assert boundary.day == 1 and boundary.hour == 0 and boundary.minute == 0
    assert expire_at % SECONDS_PER_DAY == 0

    ttl = await gateway.client.ttl(driver.monthly("expiry", now_ms))
    assert 0 < ttl <= 31 * SECONDS_PER_DAY


def test_the_period_boundaries_are_absolute_utc_midnights():
    """One instant, two literal answers — the absolute statement the calendar-derived tests lack.

    Every other expiry assertion in this file has to derive its expectation at run time, because
    the suite runs on whatever day CI runs on. That is correct and it is also why nothing else here
    can catch a systematic error: an off-by-a-timezone in ``src.keys`` would shift both sides.

    ``month_expire_at`` is the one worth pinning hardest. The classic implementation adds
    ``timedelta(days=31)``, which from 31 January lands on 3 March — skipping February's key
    entirely — and from any 30-day month lands on the 2nd rather than the 1st.
    """
    assert day_expire_at(REFERENCE_MOMENT) == REFERENCE_DAY_BOUNDARY
    assert month_expire_at(REFERENCE_MOMENT) == REFERENCE_MONTH_BOUNDARY

    assert datetime.fromtimestamp(REFERENCE_DAY_BOUNDARY, tz=timezone.utc) == datetime(
        2026, 8, 12, tzinfo=timezone.utc
    )
    assert datetime.fromtimestamp(REFERENCE_MONTH_BOUNDARY, tz=timezone.utc) == datetime(
        2026, 9, 1, tzinfo=timezone.utc
    )
    assert REFERENCE_DAY_BOUNDARY % SECONDS_PER_DAY == 0
    assert REFERENCE_MONTH_BOUNDARY % SECONDS_PER_DAY == 0


async def test_a_period_with_no_boundary_is_neither_read_nor_written(
    driver: ScriptDriver, gateway, now_ms: int
):
    """``QUOTA_DAILY_ENABLED=false`` reaches the script as an ``EXPIREAT`` of 0.

    A period with no boundary is not a period. The counter is not read, not incremented, and
    reported as ``limit = 0`` — which :class:`~src.models.LimitDecision` already renders as
    ``UNLIMITED`` and already suppresses the ``X-Quota-*`` headers for.
    """
    # daily=1 would refuse the second request if the period were live; it is not.
    table = tiers(daily=1, monthly=1_000)
    reply = await driver.call("off", now_ms=now_ms, tiers=table, daily_expire_at=0)
    second = await driver.call("off", now_ms=now_ms, tiers=table, daily_expire_at=0)

    assert reply.allowed is True
    assert second.allowed is True, "a disabled period must not be able to refuse anything"
    assert second.decision.daily_limit == 0
    assert second.decision.daily_used == 0
    assert second.decision.daily_remaining == UNLIMITED
    # `unenforced`, not `reset`. `reset` is a CLAIM that a period boundary just rolled over, and
    # this period has no boundary at all — a client could reasonably render "your quota just
    # refreshed" off it. It is also the same condition that produces the UNLIMITED above and that
    # suppresses the X-Quota-* headers, so all three agree.
    assert second.decision.daily_state is QuotaPeriodState.UNENFORCED
    assert "X-Quota-Limit" not in second.decision.headers()
    assert await gateway.client.exists(driver.daily("off", now_ms)) == 0
    # The monthly period is untouched by the daily switch and is still counting.
    assert await gateway.client.get(driver.monthly("off", now_ms)) == b"2"


# ---------------------------------------------------------------------------------------------
# Period state
# ---------------------------------------------------------------------------------------------


async def test_period_state_moves_reset_then_active_then_exhausted(
    driver: ScriptDriver, now_ms: int
):
    """``reset`` is not the same fact as "you have plenty left".

    It says the period rolled over — which is what lets a client tell "1000 left because it is a
    new day" from "1000 left because you have not called us this month".
    """
    table = tiers(daily=3, monthly=1_000)

    states = []
    for _ in range(4):
        states.append((await driver.call("states", now_ms=now_ms, tiers=table)).decision)

    assert [decision.daily_state.value for decision in states] == [
        "reset",
        "active",
        "exhausted",
        # The fourth was refused, and the period it was refused by is still exhausted.
        "exhausted",
    ]
    assert states[-1].allowed is False


async def test_a_first_request_that_consumes_the_whole_allowance_is_exhausted_not_reset(
    driver: ScriptDriver, now_ms: int
):
    """``exhausted`` is checked first, and this is why: both facts are true and only one is useful.

    Reporting ``reset`` here would tell a client the period had just rolled over — on the very
    response that used the last of it.
    """
    reply = await driver.call(
        "allatonce", now_ms=now_ms, cost=4, tiers=tiers(daily=4, monthly=1_000)
    )

    assert reply.allowed is True
    assert reply.decision.daily_state.value == "exhausted"


# ---------------------------------------------------------------------------------------------
# The cross-gate property this whole design exists for
# ---------------------------------------------------------------------------------------------


async def test_a_quota_rejection_does_not_spend_a_token(
    driver: ScriptDriver, gateway, now_ms: int
):
    """**The single most important test in C4.**

    All four gates are read and evaluated before *anything* is written, so a request refused by
    gate 3 leaves gates 1 and 2 exactly as it found them. There is no ordering of two separate
    round trips that achieves this without a distributed compensating write.

    The bucket hash is captured byte for byte before and after, because "the token count is the
    same" is not enough on its own — the ``ts`` field moving would mean the bucket was persisted,
    which is the other half of "a denial writes nothing".
    """
    table = tiers(daily=2, monthly=1_000)
    bucket = driver.bucket("crossgate")
    await driver.drain("crossgate", attempts=2, now_ms=now_ms, tiers=table)

    before = await gateway.client.hgetall(bucket)
    assert tokens_of(before[b"t"]) == ROOMY_RATE - 2

    reply: Reply = await driver.call("crossgate", now_ms=now_ms, tiers=table)

    assert reply.allowed is False
    assert reply.reason == "quota_daily"
    assert await gateway.client.hgetall(bucket) == before
    # And the quota counter it was refused by did not move either.
    assert await gateway.client.get(driver.daily("crossgate", now_ms)) == b"2"


async def test_a_quota_rejection_does_not_move_the_sliding_window_either(
    driver: ScriptDriver, gateway, now_ms: int
):
    """Same property, gate 2's counter. All four, or none."""
    table = tiers(daily=1, monthly=1_000)
    await driver.call("swquiet", now_ms=now_ms, tiers=table)
    window = driver.window("swquiet", now_ms)
    assert await gateway.client.get(window) == b"1"

    reply = await driver.call("swquiet", now_ms=now_ms, tiers=table)

    assert reply.allowed is False
    assert await gateway.client.get(window) == b"1"


# ---------------------------------------------------------------------------------------------
# Unlimited, and period independence
# ---------------------------------------------------------------------------------------------


async def test_a_non_positive_limit_means_unlimited(driver: ScriptDriver, gateway, now_ms: int):
    """The escape hatch an enterprise tier needs, and the encoding ``src.models`` already uses.

    ``0`` would be an unusable encoding for it, because "unlimited" and "you have nothing left" are
    opposite facts: a client pacing itself off a ``0`` would stop calling an endpoint it has
    infinite allowance on. Hence ``-1``.
    """
    table = tiers(daily=0, monthly=0)

    allowed = await driver.drain("unlimited", attempts=25, now_ms=now_ms, tiers=table)
    reply = await driver.call("unlimited", now_ms=now_ms, tiers=table)

    assert allowed == 25
    assert reply.allowed is True
    assert reply.decision.daily_limit == 0
    assert reply.decision.daily_remaining == UNLIMITED
    assert reply.decision.monthly_remaining == UNLIMITED
    # A tier with no ceiling is `unenforced` too, even though the counter behind it is real and
    # moving: there is no limit for `exhausted` to be measured against, and `reset`/`active` would
    # describe a period nobody is enforcing as though someone were.
    assert reply.decision.daily_state is QuotaPeriodState.UNENFORCED
    assert reply.decision.monthly_state is QuotaPeriodState.UNENFORCED
    # Unenforced is not the same as uncounted: the counter still records usage, which is what the
    # admin usage endpoint and the E2E verifier read.
    assert await gateway.client.get(driver.daily("unlimited", now_ms)) == b"26"


async def test_exhausting_the_daily_quota_does_not_exhaust_the_monthly_one(
    driver: ScriptDriver, now_ms: int
):
    """Two independent periods. Monthly is daily x 25, so it must still have room on day one."""
    table = tiers(daily=3, monthly=75)

    await driver.drain("periods", attempts=3, now_ms=now_ms, tiers=table)
    reply = await driver.call("periods", now_ms=now_ms, tiers=table)

    assert reply.reason == "quota_daily"
    assert reply.decision.monthly_used == 3
    assert reply.decision.monthly_remaining == 72
    assert reply.decision.monthly_state.value == "active"
    assert reply.decision.daily_state.value == "exhausted"


async def test_the_monthly_gate_can_refuse_on_its_own(driver: ScriptDriver, now_ms: int):
    table = tiers(daily=1_000, monthly=2)

    await driver.drain("monthonly", attempts=2, now_ms=now_ms, tiers=table)
    reply = await driver.call("monthonly", now_ms=now_ms, tiers=table)

    assert reply.allowed is False
    assert reply.reason == "quota_monthly"
    assert reply.decision.daily_state.value == "active"
    assert reply.decision.monthly_state.value == "exhausted"


async def test_retry_after_is_the_furthest_wall_not_the_nearest(
    driver: ScriptDriver, now_ms: int
):
    """**The rule that keeps a 429 from becoming a retry loop.**

    A caller who is both rate limited (seconds away) and out of monthly quota (weeks away) must be
    told about the *monthly* wall. Reporting the nearest one would send them back in three seconds
    to be refused again, and again, for the rest of the month — one wasted request per retry, all
    of them served by the limiter it is already overwhelming.
    """
    # burst 1 so gate 1 also refuses, monthly 1 so gate 4 does too.
    table = (TierRow(name="free", rpm=100_000, burst=1, daily=1_000, monthly=1),)
    await driver.call("furthest", now_ms=now_ms, tiers=table)

    reply = await driver.call("furthest", now_ms=now_ms, tiers=table)

    assert reply.allowed is False
    assert reply.reason == "quota_monthly"
    # The monthly wall, not the ~1 ms the drained bucket would have advertised. Compared against
    # the computed boundary rather than against a fixed "> a day", which would be false for a test
    # run on the last evening of a month — and a limiter test that fails once a month is a limiter
    # test people learn to re-run.
    monthly_wall = month_expire_at(moment_of(now_ms)) - now_ms // 1000
    assert abs(reply.decision.retry_after_sec - monthly_wall) <= TTL_SLOP_SEC
    assert reply.decision.retry_after_sec > 1
