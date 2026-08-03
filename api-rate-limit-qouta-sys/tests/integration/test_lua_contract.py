"""The producer/consumer contract between the Lua script and :class:`~src.models.LimitDecision`.

C3 shipped :meth:`~src.models.LimitDecision.from_lua` and pinned
:data:`~src.models.LUA_REPLY_FIELDS` — and its verifier said explicitly that it could not check the
half that matters, because there was no producer yet. This file is that check: a **live** reply
from a **real** Redis, decoded by the **real** decoder.

Four things are asserted here that nothing else in the suite can assert:

1. **Arity and order.** Lua->RESP truncates numbers *and stops at the first nil*, so a reply with a
   nullable slot does not arrive as a null — it arrives *shorter*, and every field after the gap
   shifts one place left into a decoder that would happily read a quota counter as ``allowed``.
2. **``NOSCRIPT`` recovery.** ``SCRIPT FLUSH`` and re-run. Redis's script cache is volatile: empty
   after a restart, emptied by ``SCRIPT FLUSH``, and never populated at all on a replica promoted
   by a failover, because script loads are not replicated. Only ``register_script`` survives that;
   a hand-rolled ``EVALSHA`` 500s from the instant of the failover until someone redeploys.
3. **Tier resolution is instant.** ``user -> tier`` is read *inside* the script, so an admin
   reassignment takes effect on the very next request on every replica, with no cache to
   invalidate. That is the half of hot reload that is free, and it is a property of *where* the
   HGET lives.
4. **Atomicity.** The concurrency test fires ``capacity + 40`` calls through independent
   connections and asserts **exactly** ``capacity`` were allowed. A read-modify-write race would
   let more through, and nothing sequential can tell the two implementations apart.
"""

from __future__ import annotations

import asyncio
import time

import pytest
import redis.exceptions

from src.config import TierConfig
from src.keys import user_key
from src.limiter import Limiter
from src.models import LUA_REPLY_ARITY, LUA_REPLY_FIELDS, DenyReason, LimitDecision
from src.redis_client import BackingStoreUnavailable, BreakerState, RedisGateway
from src.tiers import TierRegistry
from tests.integration.conftest import (
    DEFAULT_WINDOW_MS,
    ROOMY,
    ScriptDriver,
    TierRow,
    field,
    tier_tail,
)

#: Tolerance when comparing Redis's ``TIME`` against this process's wall clock. Both containers
#: read the same kernel clock, so the real gap is sub-millisecond; the slop is for a loaded CI box
#: scheduling the two reads far apart, not for a genuine disagreement.
CLOCK_SLOP_MS = 2_000

#: Seconds in a UTC day — a period boundary is a whole number of these since the epoch.
SECONDS_PER_DAY = 86_400

#: A small, exactly-countable capacity for the concurrency proof, plus enough surplus callers that
#: a racy implementation has room to let extras through.
SWARM_CAPACITY = 20
SWARM_EXTRA = 40

#: Independent :class:`~src.redis_client.RedisGateway` objects, each with its OWN pool — the same
#: relationship two API replicas have with one Redis.
SWARM_CONNECTIONS = 4

#: Two tiers whose numbers cannot be confused for one another.
FREE = TierRow(name="free", rpm=60, burst=5, daily=1_000, monthly=25_000)
PREMIUM = TierRow(name="premium", rpm=300, burst=50, daily=50_000, monthly=1_250_000)
TWO_TIERS = (FREE, PREMIUM)


# ---------------------------------------------------------------------------------------------
# The 19-element reply
# ---------------------------------------------------------------------------------------------


async def test_the_reply_is_exactly_the_documented_arity_and_order(
    driver: ScriptDriver, now_ms: int
):
    reply = await driver.call("shape", now_ms=now_ms)

    assert len(reply.raw) == LUA_REPLY_ARITY == 19
    assert field(reply.raw, "allowed") == 1
    assert field(reply.raw, "reason") == b"ok"
    assert field(reply.raw, "tier") == b"free"
    assert field(reply.raw, "bucket_limit") == ROOMY.burst
    assert field(reply.raw, "bucket_remaining") == ROOMY.burst - 1
    assert field(reply.raw, "window_limit") == ROOMY.rpm
    assert field(reply.raw, "window_used") == 1
    assert field(reply.raw, "daily_limit") == ROOMY.daily
    assert field(reply.raw, "daily_used") == 1
    assert field(reply.raw, "daily_state") == b"reset"
    assert field(reply.raw, "retry_ms") == 0
    # The clock the decision was actually made against — the server's, or the test's override.
    assert field(reply.raw, "now_ms") == now_ms


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [
        ("allowed", {}),
        ("window-off", {"sw_enabled": False}),
        ("quotas-off", {"daily_expire_at": 0, "monthly_expire_at": 0}),
        ("all-optional-off", {"sw_enabled": False, "daily_expire_at": 0}),
    ],
)
async def test_no_element_is_ever_nil(driver: ScriptDriver, now_ms: int, label: str, kwargs):
    """Every branch that *skips* work still has to fill its slots.

    This is where a nil would actually come from: a disabled gate leaving its counter unset. And it
    would not surface as a ``None`` — Lua->RESP stops at the first nil, so the reply would simply
    be shorter and every later field would decode as the wrong one.
    """
    reply = await driver.call(f"nil-{label}", now_ms=now_ms, **kwargs)

    assert len(reply.raw) == LUA_REPLY_ARITY
    assert all(element is not None for element in reply.raw)
    assert all(isinstance(element, (int, bytes)) for element in reply.raw)


async def test_the_reply_reports_each_gates_own_reset_instant(
    driver: ScriptDriver, gateway: RedisGateway, now_ms: int
):
    """The four time fields, asserted against live values instead of merely being non-nil.

    They are the fields a client paces itself off, and until now nothing here checked that any of
    them carried a *meaningful* number — a script that returned 0 for all four would have passed
    every other test in this file.
    """
    reply = await driver.call("resets", now_ms=now_ms)

    # One token short of capacity, refilling at ROOMY.rpm per minute:
    #   ceil(1e6 micro-tokens * 60 000 ms / (100 000 rpm * 1e6)) == 1 ms
    assert field(reply.raw, "bucket_reset_ms") == 1

    # Whatever is left of the window the frozen clock landed in.
    assert field(reply.raw, "window_reset_ms") == DEFAULT_WINDOW_MS - (now_ms % DEFAULT_WINDOW_MS)
    assert 0 < field(reply.raw, "window_reset_ms") <= DEFAULT_WINDOW_MS

    # The two quota expiries echo ARGV[6]/ARGV[7], are absolute UTC midnights, and are in the
    # future — an EXPIREAT in the past deletes the counter on the spot, handing the caller a fresh
    # allowance every time it is created.
    daily = field(reply.raw, "daily_expire_at")
    monthly = field(reply.raw, "monthly_expire_at")
    assert daily % SECONDS_PER_DAY == 0
    assert monthly % SECONDS_PER_DAY == 0
    assert daily > now_ms // 1000
    assert monthly >= daily

    # ...and they are the instants Redis actually stamped, not just the ones we reported back.
    assert await gateway.client.expiretime(driver.daily("resets", now_ms)) == daily
    assert await gateway.client.expiretime(driver.monthly("resets", now_ms)) == monthly


async def test_a_denied_reply_is_the_same_shape_as_an_allowed_one(
    driver: ScriptDriver, now_ms: int
):
    table = (TierRow(name="free", rpm=60, burst=1, daily=1_000, monthly=25_000),)
    await driver.call("denyshape", now_ms=now_ms, tiers=table)

    reply = await driver.call("denyshape", now_ms=now_ms, tiers=table)

    assert field(reply.raw, "allowed") == 0
    assert len(reply.raw) == LUA_REPLY_ARITY
    assert all(element is not None for element in reply.raw)
    assert field(reply.raw, "retry_ms") >= 1


async def test_a_live_reply_round_trips_through_the_real_decoder(
    driver: ScriptDriver, now_ms: int
):
    """The producer/consumer pair, closed. C3's verifier could only test one end of this."""
    reply = await driver.call("roundtrip", now_ms=now_ms)

    decoded = LimitDecision.from_lua(
        reply.raw, user_id="roundtrip", endpoint="GET:/api/v1/whoami", cost=1, latency_ms=0.0
    )

    # Decoding the same reply twice yields the same decision: the decode is a pure function of the
    # reply plus the caller's own inputs, with nothing read from the ambient environment.
    assert decoded == reply.decision
    # ...and `latency_ms` is the one field that comes from the caller rather than from the script,
    # which is why it is the one that has to differ when the caller says so.
    assert (
        LimitDecision.from_lua(
            reply.raw,
            user_id="roundtrip",
            endpoint="GET:/api/v1/whoami",
            cost=1,
            latency_ms=1.5,
        ).latency_ms
        == 1.5
    )
    assert decoded.reason is DenyReason.NONE
    assert decoded.server_now_ms == now_ms
    assert decoded.degraded is False
    # Every quantity the middleware will put on the wire, from one round trip.
    headers = decoded.headers()
    assert headers["X-RateLimit-Limit"] == str(ROOMY.rpm)
    assert headers["X-Quota-Limit"] == str(ROOMY.daily)
    assert "Retry-After" not in headers


# ---------------------------------------------------------------------------------------------
# NOSCRIPT recovery
# ---------------------------------------------------------------------------------------------


async def test_a_script_flush_does_not_break_the_next_call(
    driver: ScriptDriver, gateway: RedisGateway, now_ms: int
):
    """**The test that proves ``register_script`` was used rather than a hand-rolled ``EVALSHA``.**

    The case that actually bites is not an operator running ``SCRIPT FLUSH``: it is a failover.
    Script loads are not replicated, so a promoted replica has never seen this script, and a
    hand-rolled ``EVALSHA`` would 500 every single request from that instant until a redeploy —
    during exactly the event the limiter was supposed to ride out.
    """
    before = await driver.call("flush", now_ms=now_ms)
    assert before.allowed is True

    await gateway.client.script_flush()

    after = await driver.call("flush", now_ms=now_ms)

    assert after.allowed is True
    assert after.decision.bucket_remaining == before.decision.bucket_remaining - 1
    # The recovery is transparent: a cache flush is not a failure and must not move the breaker.
    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.errors == 0


# ---------------------------------------------------------------------------------------------
# Tier resolution
# ---------------------------------------------------------------------------------------------


async def test_an_unknown_user_gets_the_default_tier(driver: ScriptDriver, now_ms: int):
    """No ``user:{id}`` record at all — a JWT-authenticated principal we have never seen.

    ``DEFAULT_TIER`` is the most restrictive tier, deliberately: an unknown caller must not inherit
    the best plan, and "no tier found" must never read as "no limits".
    """
    reply = await driver.call("stranger", now_ms=now_ms, tiers=TWO_TIERS, default_tier="free")

    assert reply.decision.tier == "free"
    assert reply.decision.bucket_limit == FREE.burst


async def test_a_tier_change_takes_effect_on_the_very_next_call(
    driver: ScriptDriver, gateway: RedisGateway, now_ms: int
):
    """**The half of hot reload that is instant, and the reason the HGET is inside the script.**

    No restart, no cache flush, no pub/sub message that a reconnecting replica could miss, and no
    TTL to wait out. ``TIER_CACHE_TTL_SEC`` bounds how long it takes a change to what a tier
    *means* to propagate; it never bounds *who is on which tier*, because that is read from the
    store on every request by every replica.
    """
    before = await driver.call("promoted", now_ms=now_ms, tiers=TWO_TIERS)
    assert before.decision.tier == "free"

    await gateway.client.hset(user_key("promoted"), "tier", "premium")
    after = await driver.call("promoted", now_ms=now_ms, tiers=TWO_TIERS)

    assert after.decision.tier == "premium"
    assert after.decision.bucket_limit == PREMIUM.burst
    assert after.decision.window_limit == PREMIUM.rpm
    assert after.decision.daily_limit == PREMIUM.daily


async def test_a_tier_absent_from_the_argv_table_falls_back_to_the_default(
    driver: ScriptDriver, gateway: RedisGateway, now_ms: int
):
    """An operator can delete a tier from ``config:tiers`` while principals still reference it.

    The safe reading of "this tier no longer exists" is the most restrictive tier. Reading it as
    "no limits apply" would make deleting a row from a config hash an unmetered-access grant.
    """
    await gateway.client.hset(user_key("orphan"), "tier", "platinum")

    reply = await driver.call("orphan", now_ms=now_ms, tiers=TWO_TIERS, default_tier="free")

    assert reply.decision.tier == "free"
    assert reply.decision.bucket_limit == FREE.burst


async def test_no_tier_table_at_all_is_an_error_reply_rather_than_a_silent_fail_open(
    driver: ScriptDriver, gateway: RedisGateway, now_ms: int
):
    """Unreachable through the shipped config — and it must stay loud if it is ever reached.

    With no tier there are no limits, and "no limits found" is indistinguishable from "unlimited"
    at the point the decision is made: the entire enforcement layer would switch itself off with no
    error anywhere. An error reply becomes a ``ResponseError``, which the gateway classifies as a
    bug in this service rather than as an outage — so it surfaces as a 500 instead of reaching
    C8's fail-open path, and the breaker is left alone.
    """
    with pytest.raises(redis.exceptions.ResponseError) as caught:
        await driver.call("notiers", now_ms=now_ms, tiers=())

    assert "no tier configuration" in str(caught.value)
    assert not isinstance(caught.value, BackingStoreUnavailable)
    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.breaker.consecutive_failures == 0
    assert gateway.degraded_since is None


def test_the_empty_tier_table_really_is_empty():
    """Guards the test above from passing for the wrong reason (a malformed tail, not an empty one)."""
    assert tier_tail() == ("0",)


# ---------------------------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------------------------


async def test_exactly_capacity_is_admitted_under_concurrency(
    redis_settings, gateway: RedisGateway, now_ms: int
):
    """**The atomicity proof, and the reason the whole decision is one script.**

    ``capacity + 40`` calls, fired concurrently, spread across four independent
    :class:`~src.redis_client.RedisGateway` objects each with its own connection pool — the same
    relationship two API replicas have with one Redis. A read-modify-write split across two round
    trips would interleave and let extra requests through; a single ``EVALSHA`` runs to completion
    on Redis's single thread before the next one starts.

    ``== capacity``, never ``<= capacity``: a limiter that denied everything would pass ``<=``
    trivially, and that failure looks identical to a working one in every aggregate metric.

    The clock is frozen with an override so no request can be admitted by a refill that happened
    to land mid-swarm — the count then has exactly one explanation.
    """
    table = (
        TierRow(
            name="free",
            rpm=100_000,  # gate 2 must not be what is being measured
            burst=SWARM_CAPACITY,
            daily=1_000_000,
            monthly=10_000_000,
        ),
    )
    gateways = [RedisGateway(redis_settings) for _ in range(SWARM_CONNECTIONS)]
    for instance in gateways:
        await instance.connect()
    drivers = [ScriptDriver(instance) for instance in gateways]

    try:
        total = SWARM_CAPACITY + SWARM_EXTRA
        replies = await asyncio.gather(
            *(
                drivers[index % SWARM_CONNECTIONS].call("swarm", now_ms=now_ms, tiers=table)
                for index in range(total)
            )
        )
    finally:
        for instance in gateways:
            await instance.aclose()

    allowed = sum(1 for reply in replies if reply.allowed)
    assert allowed == SWARM_CAPACITY, (
        f"{total} concurrent calls over {SWARM_CONNECTIONS} independent connections admitted "
        f"{allowed} against a capacity of {SWARM_CAPACITY} — anything but exactly the capacity "
        "means the read-modify-write was not atomic"
    )
    assert len(replies) - allowed == SWARM_EXTRA
    # And the counter agrees from outside the script.
    stored = await gateway.client.hget(drivers[0].bucket("swarm"), "t")
    assert int(stored) == 0


# ---------------------------------------------------------------------------------------------
# The real Limiter, end to end
# ---------------------------------------------------------------------------------------------


async def test_the_shipped_limiter_produces_a_decodable_decision_from_the_shipped_config(
    redis_settings, gateway: RedisGateway
):
    """No driver, no hand-built ARGV: the real :class:`~src.limiter.Limiter` over the real registry.

    This is the one test where the KEYS the limiter builds, the ARGV tail the registry renders, the
    script that reads them and the decoder that consumes the reply are all the shipped
    implementations at once. Each half is asserted more cheaply elsewhere; only here can they be
    caught disagreeing.
    """
    registry = TierRegistry(redis_settings, gateway)
    await registry.start()
    limiter = Limiter(gateway, registry, redis_settings)

    # Brackets around the call, so `server_now_ms` can be pinned to real elapsed time rather than
    # merely to "> 0". This is the ONLY path in the suite that takes the `redis.call('TIME')`
    # branch — every other integration test freezes the clock with an override — so a stuck, wrong
    # or stubbed clock source would otherwise be invisible here.
    before_ms = int(time.time() * 1000)
    try:
        decision = await limiter.check("wired", "GET:/api/v1/logs/query", 5)
    finally:
        after_ms = int(time.time() * 1000)
        await registry.stop()

    free = redis_settings.tier_limits[redis_settings.default_tier]
    assert decision.allowed is True
    assert decision.tier == redis_settings.default_tier
    assert decision.user_id == "wired"
    assert decision.endpoint == "GET:/api/v1/logs/query"
    assert decision.cost == 5
    assert decision.bucket_limit == free.burst
    assert decision.bucket_remaining == free.burst - 5
    assert decision.window_limit == free.rate_limit_per_min
    assert decision.window_used == 5
    assert decision.daily_limit == free.daily_quota
    assert decision.daily_used == 5
    assert decision.monthly_limit == free.monthly_quota
    assert decision.retry_after_sec == 0
    # The clock is the SERVER's, and the limiter never had to know what it was — but it does have
    # to be a real clock. Bracketed by this process's own wall clock, so a `TIME` that was stuck,
    # in the wrong unit (seconds rather than milliseconds is the obvious slip) or coming from
    # somewhere other than the server would fail here.
    assert before_ms - CLOCK_SLOP_MS <= decision.server_now_ms <= after_ms + CLOCK_SLOP_MS
    assert decision.latency_ms > 0.0
    assert limiter.checks == 1


@pytest.mark.parametrize(
    ("burst", "rpm", "expected_reason"),
    [
        (5, 60, DenyReason.RATE_LIMIT),
        (60, 5, DenyReason.SLIDING_WINDOW),
    ],
    ids=["bucket-binds-first", "window-binds-first"],
)
async def test_the_shipped_limiter_names_the_gate_that_actually_refused(
    redis_settings, gateway: RedisGateway, now_ms: int, burst: int, rpm: int, expected_reason
):
    """**The two rate gates have to be separable, and the shipped tier table cannot separate them.**

    Every shipped tier has ``burst == rpm`` on purpose (sizing the bucket at exactly one minute of
    tokens makes the two gates agree on what "60 requests per minute" means). The consequence for
    testing is that they refuse at the *same* count, so a test driven off the shipped ``free`` tier
    can only assert ``reason in {rate_limit, sliding_window}`` — which is satisfied by a script that
    always reports the same one of them, i.e. by a script whose ``reason`` field is decorative.

    So the tier table is overridden to make ``burst != rpm`` in both directions. Five allowed in
    both cases, and the reason names the gate that was actually the binding one.
    """
    tier = TierConfig(
        name="free", rate_limit_per_min=rpm, burst=burst, daily_quota=1_000, monthly_quota=25_000
    )
    settings = redis_settings.model_copy(
        update={"tier_limits": {"free": tier}, "allow_clock_override": True}
    )
    registry = TierRegistry(settings, gateway)
    await registry.start()
    limiter = Limiter(gateway, registry, settings)
    user = f"gate-{burst}-{rpm}"

    try:
        allowed = 0
        for _ in range(10):
            decision = await limiter.check(user, "GET:/api/v1/whoami", 1, now_ms_override=now_ms)
            if decision.allowed:
                allowed += 1
        final = await limiter.check(user, "GET:/api/v1/whoami", 1, now_ms_override=now_ms)
    finally:
        await registry.stop()

    assert allowed == min(burst, rpm) == 5
    assert final.allowed is False
    assert final.reason is expected_reason
    assert final.retry_after_sec >= 1
    body = final.error_body()
    assert body["error"] == "Rate limit exceeded"
    assert body["reason"] == expected_reason.value
    assert body["remaining"] == 0
    assert final.headers()["Retry-After"] == str(final.retry_after_sec)
    assert int(final.headers()["X-RateLimit-Reset"]) >= 1


async def test_the_shipped_limiter_refuses_once_the_tier_is_spent(
    redis_settings, gateway: RedisGateway, now_ms: int
):
    """The **shipped** table, driven to a denial, so the 429 path is decoded from a live reply too.

    Kept alongside the parametrised test above because this one asserts the thing that is actually
    true of the shipped configuration: ``burst == rpm``, so the ceiling is that one number rather
    than the sum of the two gates.

    The clock override is enabled here and used, which is also the only place the limiter's
    ``ALLOW_CLOCK_OVERRIDE`` seam is exercised against a real server. Without it, the tier's 60
    tokens per minute would refill during the loop and "exactly 60" would become "60 plus however
    long CI took".
    """
    settings = redis_settings.model_copy(update={"allow_clock_override": True})
    registry = TierRegistry(settings, gateway)
    await registry.start()
    limiter = Limiter(gateway, registry, settings)
    free = settings.tier_limits[settings.default_tier]
    assert free.burst == free.rate_limit_per_min, "the shipped tiers size the bucket at one minute"

    try:
        allowed = 0
        for _ in range(free.burst + 5):
            decision = await limiter.check(
                "spent", "GET:/api/v1/whoami", 1, now_ms_override=now_ms
            )
            if decision.allowed:
                allowed += 1
        final = await limiter.check("spent", "GET:/api/v1/whoami", 1, now_ms_override=now_ms)
    finally:
        await registry.stop()

    assert allowed == free.burst
    assert final.allowed is False

    # BOTH rate gates refuse at exactly this count, because the shipped tier sizes them the same.
    assert final.bucket_remaining == 0
    assert final.window_used >= free.rate_limit_per_min

    # And the reported reason is the **window**, not the bucket — which is the max-retry rule
    # doing its job on the shipped configuration rather than in a contrived one. The drained
    # bucket recovers one token per second, so it would have advertised ~1 s; the account-wide
    # window has to wait out the rest of the minute plus the decay of what was just spent, which
    # is tens of seconds. Reporting the nearest wall would send this caller back in a second to be
    # refused again, and again, for the rest of the window.
    assert final.reason is DenyReason.SLIDING_WINDOW
    assert final.retry_after_sec > 1
    assert final.retry_after_sec <= 2 * 60
    body = final.error_body()
    assert body["error"] == "Rate limit exceeded"
    assert body["remaining"] == 0
    assert final.headers()["Retry-After"] == str(final.retry_after_sec)


async def test_the_reply_field_names_are_read_from_the_models_contract():
    """A guard on this file's own helper: every assertion above indexes reply slots BY NAME."""
    assert len(LUA_REPLY_FIELDS) == LUA_REPLY_ARITY
    assert LUA_REPLY_FIELDS[0] == "allowed"
    assert LUA_REPLY_FIELDS[-1] == "now_ms"
