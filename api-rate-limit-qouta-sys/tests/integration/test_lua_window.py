"""Gate 2 — the account-wide weighted sliding window — against a REAL ``redis:7-alpine``.

.. rubric:: Why this gate exists at all

The token bucket is per ``(user, endpoint)``, which satisfies the spec's literal key
``rate_limit:{user_id}:{endpoint}`` and satisfies nothing else: five endpoints at 60 tokens each is
300 requests a minute for a tier that is advertised as 60. This gate is the account-wide ceiling
that makes "free tier limited after ~60 req/min" true across *all* endpoints.

.. rubric:: Why weighted, and not a plain fixed window

:func:`test_a_boundary_burst_does_not_admit_twice_the_limit` is the test that documents the choice.
A fixed per-minute counter admits ``limit`` requests at 11:59:59 and another ``limit`` at 12:00:00
— 2x the advertised rate inside two seconds, forever, available to anyone who notices. The
Cloudflare weighted counter:

    used = ceil(prev * ((W - into) / W) + curr)

still counts almost all of the previous window one second into the current one, so that burst
simply does not happen. Two counters and two ``GET``s, versus a ZSET request *log* whose per-call
trim is O(n) on a single-threaded server — a 0.25 ms limiter that becomes a 20 ms one under load.

Every test below fixes ``now_ms`` explicitly, and several align it to a window boundary, because
the whole subject is where a request falls *within* a window.
"""

from __future__ import annotations

from tests.integration.conftest import (
    DEFAULT_WINDOW_MS,
    ScriptDriver,
    TierRow,
)

#: burst/quota ceilings big enough that gates 1, 3 and 4 can never refuse anything in this file.
ROOMY_BURST = 100_000
ROOMY_DAILY = 10_000_000
ROOMY_MONTHLY = 100_000_000


def tiers(*, rpm: int) -> tuple[TierRow, ...]:
    """A one-row table whose only meaningful number is the window's per-minute ceiling."""
    return (
        TierRow(
            name="free",
            rpm=rpm,
            burst=ROOMY_BURST,
            daily=ROOMY_DAILY,
            monthly=ROOMY_MONTHLY,
        ),
    )


def aligned(now_ms: int) -> int:
    """The start of the window containing ``now_ms``.

    Tests that talk about "the end of window k" need an origin, and deriving it from the real clock
    (rather than from a hard-coded epoch) keeps every ``EXPIREAT`` this drives in the future
    relative to the server's own clock — see the ``now_ms`` fixture.
    """
    return (now_ms // DEFAULT_WINDOW_MS) * DEFAULT_WINDOW_MS


# ---------------------------------------------------------------------------------------------
# The formula
# ---------------------------------------------------------------------------------------------


async def test_the_weighted_formula_matches_a_hand_computed_value(
    driver: ScriptDriver, gateway, now_ms: int
):
    """prev=8, curr=3, a quarter of the way into the window: ``ceil(8 * 0.75 + 3) == 9``.

    Seeded directly into the two counters rather than produced by driving traffic, so the assertion
    is against an arithmetic statement a reader can check by hand instead of against whatever a
    previous loop happened to leave behind.
    """
    base = aligned(now_ms)
    index = base // DEFAULT_WINDOW_MS
    await gateway.client.set(driver.window("weights", base), 3)
    await gateway.client.set(driver.window("weights", base - DEFAULT_WINDOW_MS), 8)
    assert driver.window("weights", base).endswith(f":{index}")

    # A quarter into the window: the previous one still counts for three quarters.
    reply = await driver.call("weights", now_ms=base + 15_000, tiers=tiers(rpm=20))

    assert reply.allowed is True
    assert reply.decision.window_limit == 20
    # ceil(8 * 0.75 + 3) = 9 before this request, + 1 for this request itself.
    assert reply.decision.window_used == 10


async def test_the_previous_window_stops_counting_once_it_has_fully_decayed(
    driver: ScriptDriver, gateway, now_ms: int
):
    """At the very end of a window the previous one contributes ~nothing — that is the "sliding"."""
    base = aligned(now_ms)
    await gateway.client.set(driver.window("decay", base - DEFAULT_WINDOW_MS), 40)

    early = await driver.call("decay", now_ms=base + 1, tiers=tiers(rpm=1_000))
    late = await driver.call("decay", now_ms=base + 59_999, tiers=tiers(rpm=1_000))

    # Just after the boundary the previous window is still worth ~all of its 40.
    assert early.decision.window_used >= 40
    # Just before the next one it is worth ~none of them; only the two requests above remain.
    assert late.decision.window_used <= 3


# ---------------------------------------------------------------------------------------------
# The boundary burst — the test that justifies the algorithm
# ---------------------------------------------------------------------------------------------


async def test_a_boundary_burst_does_not_admit_twice_the_limit(
    driver: ScriptDriver, now_ms: int
):
    """**A fixed window would admit 2x the limit in two seconds. This one admits zero extra.**

    Ten requests at the last second of window *k*, then ten more at the first second of *k+1*. A
    plain per-minute counter resets at the boundary and would hand out all twenty. The weighted
    counter still values window *k* at 59/60 one second in, so the account is already at its
    ceiling and every one of the second ten is refused.
    """
    limit = 10
    table = tiers(rpm=limit)
    base = aligned(now_ms)

    end_of_k = await driver.drain(
        "burst", attempts=limit + 2, now_ms=base + 59_000, tiers=table
    )
    start_of_next = await driver.drain(
        "burst", attempts=limit, now_ms=base + DEFAULT_WINDOW_MS + 1_000, tiers=table
    )

    assert end_of_k == limit
    assert start_of_next == 0, (
        "a fixed window would have reset here and admitted a second full limit"
    )
    assert end_of_k + start_of_next < 2 * limit


async def test_headroom_returns_gradually_as_the_previous_window_decays(
    driver: ScriptDriver, gateway, now_ms: int
):
    """The other half of the same property: the gate loosens continuously, it does not snap open.

    Halfway into the window a previous window of 10 is worth 5, so a limit of 10 has exactly five
    requests of headroom — not zero, and not ten.
    """
    base = aligned(now_ms)
    await gateway.client.set(driver.window("gradual", base - DEFAULT_WINDOW_MS), 10)

    allowed = await driver.drain("gradual", attempts=10, now_ms=base + 30_000, tiers=tiers(rpm=10))

    assert allowed == 5


async def test_a_window_denial_names_the_window_and_advertises_a_bounded_retry(
    driver: ScriptDriver, now_ms: int
):
    base = aligned(now_ms)
    table = tiers(rpm=4)
    await driver.drain("named", attempts=4, now_ms=base + 10_000, tiers=table)

    reply = await driver.call("named", now_ms=base + 10_000, tiers=table)

    assert reply.allowed is False
    assert reply.reason == "sliding_window"
    assert reply.decision.window_limit == 4
    assert reply.decision.retry_after_sec >= 1
    # The advice never exceeds two windows: past that the counter it is waiting on no longer
    # exists, so a longer number would be a promise the algorithm cannot keep.
    assert reply.decision.retry_after_sec <= 2 * DEFAULT_WINDOW_MS // 1000


# ---------------------------------------------------------------------------------------------
# Keys, TTLs and the switch
# ---------------------------------------------------------------------------------------------


async def test_the_window_counter_outlives_its_own_window(
    driver: ScriptDriver, gateway, now_ms: int
):
    """``PEXPIRE 2 x W``, not ``W``.

    The previous window has to survive into the current one, or there is nothing to weight — and a
    window with nothing to weight is a fixed window, which is the exact bug the algorithm above
    exists to not have.
    """
    base = aligned(now_ms)

    await driver.call("ttl", now_ms=base + 1_000, tiers=tiers(rpm=100))

    pttl = await gateway.client.pttl(driver.window("ttl", base))

    assert pttl > DEFAULT_WINDOW_MS
    assert pttl <= 2 * DEFAULT_WINDOW_MS


async def test_the_window_keys_are_derived_inside_the_script_from_the_shared_clock(
    driver: ScriptDriver, gateway, now_ms: int
):
    """The script is handed a PREFIX, and computes the index itself.

    Passing a Python-computed index as a KEY would reintroduce the second clock this whole design
    removed: a replica running 40 seconds fast would write into a window the others are not reading
    yet, and the account-wide gate would silently become a per-replica gate. The keys it produces
    still carry the same ``{user}`` hash tag as KEYS[1], so deriving them here is provably one slot.
    """
    base = aligned(now_ms)

    await driver.call("derived", now_ms=base + 5_000, tiers=tiers(rpm=100))
    await driver.call("derived", now_ms=base + DEFAULT_WINDOW_MS + 5_000, tiers=tiers(rpm=100))

    this_window = driver.window("derived", base)
    next_window = driver.window("derived", base + DEFAULT_WINDOW_MS)
    assert await gateway.client.get(this_window) == b"1"
    assert await gateway.client.get(next_window) == b"1"
    assert "{derived}" in this_window and "{derived}" in next_window


async def test_the_gate_can_be_switched_off_entirely(
    driver: ScriptDriver, gateway, now_ms: int
):
    """``SLIDING_WINDOW_ENABLED=false`` must not merely raise the ceiling — it must not count.

    A counter that kept ticking while the gate was off would come back armed and already exhausted
    the moment an operator switched it on again, which is the worst possible moment for a limit to
    fire.
    """
    base = aligned(now_ms)
    table = tiers(rpm=5)

    allowed = await driver.drain(
        "off", attempts=20, now_ms=base + 1_000, tiers=table, sw_enabled=False
    )

    assert allowed == 20
    assert await gateway.client.exists(driver.window("off", base)) == 0


async def test_a_disabled_gate_still_advertises_a_reset_a_client_can_act_on(
    driver: ScriptDriver, now_ms: int
):
    """**The header-level consequence of switching this gate off, which the test above missed.**

    With the gate disabled the script still reports the tier's per-minute number as
    ``window_limit`` — that is what the caller's plan says — but has no window to report a recovery
    for, so ``window_reset_ms`` is 0. Emitted naively that is
    ``Limit: 60, Remaining: 0, Reset: 0`` on a **denied** response: a client pacing off that pair
    retries immediately, is refused by the bucket whose real recovery was three seconds away, and
    loops. The same retry storm ``Retry-After: 0`` would cause, arriving through a different
    header.

    It bites specifically because ``SLIDING_WINDOW_ENABLED`` is an *operability* switch, so it gets
    flipped during an incident — exactly when a client-side retry storm is least affordable. The
    script keeps reporting raw per-gate facts; ``LimitDecision.headers()`` owns turning them into
    advice.
    """
    base = aligned(now_ms)
    # burst 3 at 60 rpm: a drained bucket recovers in exactly 3 000 ms.
    table = (
        TierRow(name="free", rpm=60, burst=3, daily=ROOMY_DAILY, monthly=ROOMY_MONTHLY),
    )
    await driver.drain("hdr", attempts=3, now_ms=base + 1_000, tiers=table, sw_enabled=False)

    reply = await driver.call("hdr", now_ms=base + 1_000, tiers=table, sw_enabled=False)

    assert reply.allowed is False
    assert reply.reason == "rate_limit"
    # The raw facts the script reports, asserted rather than assumed.
    assert reply.decision.window_limit == 60
    assert reply.decision.window_used == 0
    assert reply.decision.window_reset_sec == 0
    assert reply.decision.bucket_reset_sec == 3

    headers = reply.decision.headers()
    assert headers["X-RateLimit-Limit"] == "60"
    assert headers["X-RateLimit-Remaining"] == "0"
    assert headers["X-RateLimit-Reset"] == "3"
    assert int(headers["X-RateLimit-Reset"]) >= 1
    assert int(headers["Retry-After"]) >= 1


async def test_the_window_is_account_wide_rather_than_per_endpoint(
    driver: ScriptDriver, now_ms: int
):
    """**Why this gate is not redundant with the bucket.**

    Two different endpoints have two independent token buckets, so without an account-wide gate a
    caller simply alternates between them and doubles their rate. Here the second endpoint's
    requests are counted against the same window.
    """
    base = aligned(now_ms)
    table = tiers(rpm=4)
    await driver.drain(
        "acct", attempts=4, now_ms=base + 1_000, tiers=table, endpoint="GET:/api/v1/whoami"
    )

    reply = await driver.call(
        "acct",
        now_ms=base + 1_000,
        tiers=table,
        endpoint="GET:/api/v1/logs/query",
    )

    assert reply.allowed is False
    assert reply.reason == "sliding_window"
