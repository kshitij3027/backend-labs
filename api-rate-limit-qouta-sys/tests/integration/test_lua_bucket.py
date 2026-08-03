"""Gate 1 — the token bucket — asserted against a REAL ``redis:7-alpine``.

.. rubric:: Why none of this is a unit test

``fakeredis[lua]`` is a reimplementation. Its ``TIME``, its number-to-string coercion and its
Lua->RESP conversion rules are *approximations of* Redis's, and the entire job of this script is
exactness: micro-token arithmetic that must not drift, a millisecond clock that must be the
server's, and a 19-element reply whose every element must survive truncation without becoming a
nil. Proving "the bucket admits exactly 5" against an approximation would produce a green suite
that says nothing about the thing that ships — and would be believed, which is worse than having
no test at all.

.. rubric:: The sliding window is switched off in this file

Every test here fixes a specific ``burst`` and a specific ``rpm``, and ``rpm`` is *also* the
account-wide window's ceiling. A bucket test that wanted a slow refill (``rpm=1``) would otherwise
be silently asserting about gate 2 refusing at one request per minute. Gate 2 has its own file;
here it is disarmed so that every count in these assertions is attributable to the bucket.

.. rubric:: The clock is frozen, not slept for

Every call carries an explicit ``now_ms``. Refill is a function of elapsed milliseconds, so
"advance the clock by exactly 3 seconds" is a parameter, not a ``sleep`` — which makes these tests
instant, deterministic, and able to assert an *exact* token count rather than a range wide enough
to survive CI scheduling jitter.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.integration.conftest import (
    DEFAULT_BUCKET_TTL_MS,
    DEFAULT_ENDPOINT,
    Reply,
    ScriptDriver,
    TierRow,
    tokens_of,
)

#: Quota ceilings big enough that gates 3 and 4 can never be the reason for anything in this file.
ROOMY_DAILY = 10_000_000
ROOMY_MONTHLY = 100_000_000

#: An rpm high enough that the refill is irrelevant at a frozen clock, used by the tests that are
#: about capacity rather than about rate.
FAST = 100_000


def tiers(*, burst: int, rpm: int) -> tuple[TierRow, ...]:
    """A one-row tier table: exactly the two numbers gate 1 is made of."""
    return (
        TierRow(
            name="free", rpm=rpm, burst=burst, daily=ROOMY_DAILY, monthly=ROOMY_MONTHLY
        ),
    )


async def call(driver: ScriptDriver, user: str, **kwargs: Any) -> Reply:
    """One decision with gate 2 disarmed. See the module docstring."""
    kwargs.setdefault("sw_enabled", False)
    return await driver.call(user, **kwargs)


async def drain(driver: ScriptDriver, user: str, *, attempts: int, **kwargs: Any) -> int:
    kwargs.setdefault("sw_enabled", False)
    return await driver.drain(user, attempts=attempts, **kwargs)


# ---------------------------------------------------------------------------------------------
# Capacity
# ---------------------------------------------------------------------------------------------


async def test_a_bucket_admits_exactly_its_capacity_and_then_denies(
    driver: ScriptDriver, now_ms: int
):
    """**Exactly** five, asserted as equality rather than as ``<= 5``.

    ``allowed <= capacity`` is passed trivially by a limiter that denies *everything*, which is a
    total outage wearing a passing test's clothes. ``allowed > 0`` alone is passed by one that
    allows everything. Only equality distinguishes a working limiter from both failures, so it is
    the assertion this file leads with.
    """
    capacity = 5

    allowed = await drain(
        driver, "cap", attempts=capacity + 3, now_ms=now_ms, tiers=tiers(burst=capacity, rpm=FAST)
    )

    assert allowed > 0
    assert allowed == capacity


async def test_the_denial_names_the_bucket_and_reports_nothing_left(
    driver: ScriptDriver, now_ms: int
):
    await drain(driver, "named", attempts=2, now_ms=now_ms, tiers=tiers(burst=2, rpm=FAST))

    reply = await call(driver, "named", now_ms=now_ms, tiers=tiers(burst=2, rpm=FAST))

    assert reply.allowed is False
    assert reply.reason == "rate_limit"
    assert reply.decision.bucket_remaining == 0
    assert reply.decision.bucket_limit == 2
    # >= 1 always. A `Retry-After: 0` is a retry storm the limiter manufactured itself.
    assert reply.decision.retry_after_sec >= 1


async def test_an_unseen_bucket_starts_full(driver: ScriptDriver, gateway, now_ms: int):
    """The only defensible starting state.

    Starting empty would refuse a first-time caller for a full minute for the crime of arriving;
    any other value is a number nobody can explain to them.
    """
    key = driver.bucket("fresh")
    assert await gateway.client.exists(key) == 0

    reply = await call(driver, "fresh", now_ms=now_ms, tiers=tiers(burst=7, rpm=FAST))

    assert reply.allowed is True
    assert reply.decision.bucket_remaining == 6
    assert tokens_of(await gateway.client.hget(key, "t")) == 6


# ---------------------------------------------------------------------------------------------
# Refill
# ---------------------------------------------------------------------------------------------


async def test_the_bucket_refills_at_the_configured_rate(driver: ScriptDriver, now_ms: int):
    """rpm=60 is one token per second, so three seconds is three tokens. Exactly three."""
    table = tiers(burst=10, rpm=60)
    drained = await drain(driver, "refill", attempts=10, now_ms=now_ms, tiers=table)
    assert drained == 10
    assert (await call(driver, "refill", now_ms=now_ms, tiers=table)).allowed is False

    reply = await call(driver, "refill", now_ms=now_ms + 3_000, tiers=table)

    assert reply.allowed is True
    # Three tokens accrued, one spent by this very request.
    assert reply.decision.bucket_remaining == 2


async def test_a_partial_millisecond_of_refill_does_not_round_up_into_a_free_token(
    driver: ScriptDriver, now_ms: int
):
    """Micro-tokens exist for this. At 1 token/sec, 999 ms is 0.999 tokens — not 1.

    Rounding a fractional refill up is how a bucket quietly becomes bigger than its capacity under
    a client that paces itself just under the limit: every request would round its own fraction up
    and the caller would gain a token per request forever.
    """
    table = tiers(burst=4, rpm=60)
    await drain(driver, "partial", attempts=4, now_ms=now_ms, tiers=table)

    reply = await call(driver, "partial", now_ms=now_ms + 999, tiers=table)

    assert reply.allowed is False


async def test_a_long_idle_clamps_at_capacity(driver: ScriptDriver, now_ms: int):
    """A day of silence must produce a full bucket, not a day's worth of tokens."""
    table = tiers(burst=10, rpm=60)
    await drain(driver, "idle", attempts=10, now_ms=now_ms, tiers=table)

    one_day_ms = 86_400_000
    reply = await call(driver, "idle", now_ms=now_ms + one_day_ms, tiers=table)

    assert reply.allowed is True
    assert reply.decision.bucket_remaining == 9


# ---------------------------------------------------------------------------------------------
# TTL
# ---------------------------------------------------------------------------------------------


async def test_a_call_always_leaves_a_ttl_on_the_bucket(
    driver: ScriptDriver, gateway, now_ms: int
):
    """``PTTL > 0`` — the assertion that catches the documented redis.io-example bug.

    The canonical token-bucket example sets no expiry at all, so every ``(user, endpoint)`` pair a
    caller ever touches becomes a permanent hash. The key space is bounded per user, but the number
    of users is not, and a store whose memory only ever grows is a store that eventually starts
    evicting the counters this service depends on.
    """
    await call(driver, "ttl", now_ms=now_ms, tiers=tiers(burst=5, rpm=FAST))

    pttl = await gateway.client.pttl(driver.bucket("ttl"))

    assert pttl > 0
    assert pttl <= DEFAULT_BUCKET_TTL_MS


async def test_the_ttl_floor_is_whatever_argv_2_carried(
    driver: ScriptDriver, gateway, now_ms: int
):
    """The floor is the value the caller sent, not a constant compiled into the script.

    ``0 < pttl <= 3_600_000`` on its own is satisfied by a script that ignores ARGV[2] entirely and
    hard-codes an hour — which is exactly the shape of the bug ``BUCKET_TTL_SEC`` exists to make
    configurable. Two floors, one shallow deficit, and the TTL follows the argument.
    """
    table = tiers(burst=5, rpm=FAST)

    await call(driver, "ttl-default", now_ms=now_ms, tiers=table, bucket_ttl_ms=DEFAULT_BUCKET_TTL_MS)
    await call(driver, "ttl-small", now_ms=now_ms, tiers=table, bucket_ttl_ms=1_000)

    # One token of deficit at 100 000 rpm is a 1 ms refill, so in BOTH calls the floor is what wins.
    default_pttl = await gateway.client.pttl(driver.bucket("ttl-default"))
    small_pttl = await gateway.client.pttl(driver.bucket("ttl-small"))

    assert 3_599_000 < default_pttl <= DEFAULT_BUCKET_TTL_MS
    assert 900 < small_pttl <= 1_000


async def test_a_large_refill_deficit_outranks_a_small_ttl_floor(
    driver: ScriptDriver, gateway, now_ms: int
):
    """The other direction of ``max(floor, time-to-refill)``: the deficit wins by three orders."""
    await call(
        driver, "deficit", now_ms=now_ms, cost=100, tiers=tiers(burst=100, rpm=1), bucket_ttl_ms=1_000
    )

    pttl = await gateway.client.pttl(driver.bucket("deficit"))

    assert pttl > 1_000
    # 100 tokens at 1 per minute.
    assert 5_900_000 < pttl <= 6_000_000


async def test_every_value_the_script_writes_is_a_plain_integer_string(
    driver: ScriptDriver, gateway, now_ms: int
):
    """No scientific notation anywhere the script writes — see ``int_arg`` in ``src/lua.py``.

    Redis 7 already renders an integral Lua number as a plain integer (a ``num == (long long)num``
    fast path), so on the server this project ships against nothing here is at risk. Redis <= 6.2
    had only the shortest-round-trip float path, which renders 1750000000000 as ``1.75e+12``. The
    values still round-trip through ``tonumber()``, so the cost is a ``MONITOR`` trace nobody can
    read and a decimal string arriving at ``INCRBY``.

    The script formats its own arguments so the behaviour does not depend on the server build, and
    this is the assertion that would notice if that stopped happening: ``ts`` is the millisecond
    clock, which is the one value large enough to trip the float path.
    """
    table = tiers(burst=50, rpm=FAST)

    await driver.call("plain", now_ms=now_ms, cost=3, tiers=table)

    stored = await gateway.client.hgetall(driver.bucket("plain"))
    assert stored[b"ts"] == str(now_ms).encode()
    assert stored[b"t"].isdigit()
    assert int(stored[b"t"]) == 47 * 1_000_000
    # `cost` reaches all three INCRBYs through the same formatter.
    assert await gateway.client.get(driver.daily("plain", now_ms)) == b"3"
    assert await gateway.client.get(driver.monthly("plain", now_ms)) == b"3"
    assert await gateway.client.get(driver.window("plain", now_ms)) == b"3"


async def test_the_ttl_is_level_aware_and_never_gifts_a_full_bucket(
    driver: ScriptDriver, gateway, now_ms: int
):
    """**An expiry must never silently hand a caller a full bucket.**

    A drained bucket on a slow tier takes longer to refill than a flat 1-hour TTL: rpm=1 with a
    capacity of 100 needs 100 minutes. With a flat TTL the key would be deleted at 60 minutes and
    recreated *full* by the next request — an unlimited allowance available to anyone willing to
    pause. So the TTL is ``max(floor, time-to-refill)``, and here that is provably above the floor.
    """
    table = tiers(burst=100, rpm=1)

    reply = await call(driver, "slow", now_ms=now_ms, cost=100, tiers=table)

    assert reply.allowed is True
    assert reply.decision.bucket_remaining == 0
    pttl = await gateway.client.pttl(driver.bucket("slow"))
    # 100 tokens at 1 per minute = 6 000 000 ms, which is well past the 3 600 000 ms floor.
    assert pttl > DEFAULT_BUCKET_TTL_MS
    assert 5_900_000 < pttl <= 6_000_000


# ---------------------------------------------------------------------------------------------
# A denial writes NOTHING
# ---------------------------------------------------------------------------------------------


async def test_a_denied_request_writes_nothing_at_all(
    driver: ScriptDriver, gateway, now_ms: int
):
    """**The write-amplification property**, captured before and after.

    Refill is linear and clamped, so recomputing it from the older ``ts`` on the next request
    yields the identical number — persisting it on a refusal buys nothing and costs a write. That
    matters most under exactly the traffic you least want to amplify: a client in a retry loop
    against a limit it has already hit produces a flood of *denials*, and every one of them must
    cost the single-threaded server zero writes.

    The TTL is stamped to a distinctive 5 seconds first. A ``PEXPIRE`` reissued by the denied call
    would reset it to the 3 600 000 ms floor, which no amount of elapsed time between two
    assertions could imitate.
    """
    key = driver.bucket("quiet")
    table = tiers(burst=2, rpm=FAST)
    await drain(driver, "quiet", attempts=2, now_ms=now_ms, tiers=table)
    await gateway.client.pexpire(key, 5_000)

    before_hash = await gateway.client.hgetall(key)
    before_ttl = await gateway.client.pttl(key)

    # The SAME instant, deliberately: at rpm=100000 a single millisecond is worth 1.67 tokens, so
    # advancing the clock even by 1 ms here would be testing the refill rather than the denial.
    reply = await call(driver, "quiet", now_ms=now_ms, tiers=table)

    assert reply.allowed is False
    assert await gateway.client.hgetall(key) == before_hash
    after_ttl = await gateway.client.pttl(key)
    # A TTL can only tick DOWN on its own. Anything at or above the floor means PEXPIRE ran.
    assert after_ttl <= before_ttl
    assert after_ttl < DEFAULT_BUCKET_TTL_MS


# ---------------------------------------------------------------------------------------------
# Clock skew
# ---------------------------------------------------------------------------------------------


async def test_a_backwards_clock_neither_refills_nor_penalises(
    driver: ScriptDriver, now_ms: int
):
    """An NTP step backwards is the operator's problem, and must not become the caller's.

    Negative elapsed time is clamped to zero: no refill (the tokens did not accrue) and no penalty
    (the caller did nothing wrong). The alternative — letting the negative through the refill
    arithmetic — would *subtract* tokens the caller never spent.
    """
    table = tiers(burst=10, rpm=60)
    await call(driver, "ntp", now_ms=now_ms, cost=4, tiers=table)

    reply = await call(driver, "ntp", now_ms=now_ms - 5_000, tiers=table)

    assert reply.allowed is True
    # 6 left, minus this request. No refill was credited for time that ran backwards.
    assert reply.decision.bucket_remaining == 5


async def test_a_timestamp_far_in_the_future_resets_rather_than_freezing_the_caller(
    driver: ScriptDriver, now_ms: int
):
    """A stamp more than one full refill period ahead is not a bucket we can reason about.

    Honouring it would freeze this principal out for the entire duration of the skew — potentially
    hours, on a bucket that is supposed to recover in seconds — with no way for them to tell that
    from a limiter that had simply stopped working. Resetting to full is the state we can defend.
    """
    # capacity 10 at 60 rpm refills fully in 10 000 ms; the stamp below is 20 000 ms ahead.
    table = tiers(burst=10, rpm=60)
    await drain(driver, "skew", attempts=10, now_ms=now_ms, tiers=table)

    reply = await call(driver, "skew", now_ms=now_ms - 20_000, tiers=table)

    assert reply.allowed is True
    assert reply.decision.bucket_remaining == 9


# ---------------------------------------------------------------------------------------------
# Weighted cost
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("cost", [1, 2, 5])
async def test_cost_is_consumed_proportionally(driver: ScriptDriver, now_ms: int, cost: int):
    """The in-scope bonus: ``/logs/query`` costs 5 tokens where ``/whoami`` costs 1."""
    table = tiers(burst=20, rpm=FAST)

    first = await call(driver, f"cost{cost}", now_ms=now_ms, cost=cost, tiers=table)
    second = await call(driver, f"cost{cost}", now_ms=now_ms, cost=cost, tiers=table)

    assert first.decision.bucket_remaining == 20 - cost
    assert second.decision.bucket_remaining == 20 - 2 * cost


async def test_a_request_costing_more_than_capacity_can_never_be_allowed(
    driver: ScriptDriver, gateway, now_ms: int
):
    """Not even against a completely full bucket — and it must not half-spend one trying."""
    reply = await call(driver, "toobig", now_ms=now_ms, cost=6, tiers=tiers(burst=5, rpm=FAST))

    assert reply.allowed is False
    assert reply.reason == "rate_limit"
    assert reply.decision.bucket_remaining == 5
    # A denial writes nothing, so the bucket was never even created.
    assert await gateway.client.exists(driver.bucket("toobig")) == 0


async def test_two_endpoints_are_two_independent_buckets(driver: ScriptDriver, now_ms: int):
    """The bucket is per ``(user, endpoint)`` — that is the spec's literal key shape.

    The account-wide ceiling is gate 2's job precisely because these are independent; without it,
    five endpoints would multiply a 60-per-minute tier by five.
    """
    table = tiers(burst=2, rpm=FAST)
    await drain(driver, "two", attempts=2, now_ms=now_ms, tiers=table, endpoint=DEFAULT_ENDPOINT)

    same = await call(driver, "two", now_ms=now_ms, tiers=table, endpoint=DEFAULT_ENDPOINT)
    other = await call(
        driver, "two", now_ms=now_ms, tiers=table, endpoint="GET:/api/v1/logs/query"
    )

    assert same.allowed is False
    assert other.allowed is True
