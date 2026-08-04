"""Unit tests for :class:`~src.fallback.LocalBucketCache` — the degraded path's token buckets.

Everything here runs against an **injected clock**, so not one assertion sleeps and every refill is
an exact number rather than "roughly a second's worth". That matters more than usual for this
class: its whole job is arithmetic, its inputs are durations, and a test that slept would be
asserting about the container's scheduler.

Four properties carry the weight of the file, and each is the reason a specific bug cannot ship:

* **``ceil(capacity / API_REPLICAS)``.** A local gate sized at the *tier's* figure is N rate
  limiters rather than one — the exact double-spend C12's distributed test exists to catch,
  reproduced on purpose in the one mode where a local answer beats no answer at all.
* **Every gate is evaluated before any is mutated, and a denial spends nothing.** Both are
  transcribed from the decision script's mutation block, and both only became assertable once one
  request touched two gates: spending the bucket's token and *then* being refused by the account
  gate would charge a caller for a request they never got.
* **The LRU cap, across both key spaces.** Without it, an outage plus a flood of distinct
  principals grows the heap until the pod is killed: the attack relocated from Redis into the
  process rather than stopped. One map rather than two, so the cap bounds the total.
* **``move_to_end`` on a hit.** An evicted gate comes back *full*, so a FIFO would hand extra
  allowance to precisely the busiest callers. The recency ordering is a limit property here, not a
  cache-efficiency one.

What this file does **not** assert is the property the two gates exist for — that a caller cannot
buy extra allowance by spreading traffic across endpoints. That is a statement about the *limiter*
choosing which gates to pass, so it lives in ``tests/unit/test_degradation.py`` where several
endpoint labels can be driven through one principal. Asserting it here would only prove that this
class does what it is told.

There is deliberately **no quota assertion anywhere in this file**, because there is deliberately
no quota enforcement in the class. See its module docstring: a quota is a cumulative
cross-replica counter and this process cannot know it. ``tests/unit/test_degradation.py`` asserts
the consequence on the wire — every ``X-Quota-*`` header omitted while degraded.
"""

from __future__ import annotations

import pytest

from src.config import Settings
from src.fallback import (
    FALLBACK_MAX_BUCKETS,
    GateSpec,
    LocalBucketCache,
    LocalVerdict,
    replica_share,
)
from src.keys import MS_PER_MINUTE

#: A bucket key shaped like the real one. The limiter passes the same
#: ``rate_limit:{user}:<METHOD>:<template>`` string it would have sent to Redis, so a caller's local
#: and shared buckets are the same bucket by name.
KEY = "rate_limit:{alice}:GET:/api/v1/whoami"
OTHER_KEY = "rate_limit:{bob}:GET:/api/v1/whoami"

#: The account-wide gate's key: the user alone, exactly the name the shared sliding window is built
#: from minus its window index.
ACCOUNT_KEY = "sw:{alice}"


def spend(
    cache: LocalBucketCache, key: str, *, capacity: int, rpm: int, cost: int
) -> LocalVerdict:
    """Drive **one** gate and return its verdict — the single-gate arithmetic under test here.

    :meth:`~src.fallback.LocalBucketCache.consume` is variadic because the limiter always passes
    two gates, but nearly every assertion in this file is about the refill/spend/retry arithmetic
    of a *single* bucket, which is identical whichever gate it is playing. Unwrapping the
    one-element tuple at the call site 35 times would bury that arithmetic in indexing; the
    multi-gate behaviour gets its own section below, where the tuple is the subject.
    """
    return cache.consume(GateSpec(key=key, capacity=capacity, rpm=rpm), cost=cost).verdicts[0]


class FakeClock:
    """A monotonic clock a test advances by hand. Seconds, matching ``time.monotonic``."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


def build(settings: Settings, clock: FakeClock, **overrides: object) -> LocalBucketCache:
    """A cache over ``settings``, with any :class:`~src.config.Settings` field overridden."""
    if overrides:
        settings = settings.model_copy(update=overrides)
    return LocalBucketCache(settings, clock=clock)


# =============================================================================================
# The capacity rule — the reason this module exists
# =============================================================================================


@pytest.mark.parametrize(
    ("total", "replicas", "expected"),
    [
        (60, 1, 60),   # a single replica enforces the WHOLE tier: no division, no under-serving
        (60, 2, 30),   # the shipped topology
        (60, 3, 20),
        (60, 7, 9),    # 8.57 -> 9: rounds UP
        (1, 4, 1),     # never rounds down to a bucket that admits nothing
        (0, 2, 0),     # a tier configured with no capacity stays with no capacity
        (-5, 2, 0),
        (60, 0, 60),   # a mis-set API_REPLICAS is floored at 1, never a ZeroDivisionError
    ],
)
def test_replica_share_is_ceiling_division_floored_at_one_replica(total, replicas, expected):
    """The arithmetic, exhaustively, because every interesting case is an edge.

    Rounding **up** is the deliberate direction: down-rounding would turn a tier of 1 into a bucket
    that admits nothing, i.e. a small tier refused outright for the duration of an outage because
    of integer division. Up-rounding costs at most ``replicas - 1`` extra tokens across the whole
    cluster.

    ``replicas=0`` is floored rather than raising: ``API_REPLICAS`` carries no positivity
    constraint, and a ``ZeroDivisionError` raised from inside the degraded path would be a crash
    during an outage caused by a config typo — on the one code path whose entire job is surviving
    outages.
    """
    assert replica_share(total, replicas) == expected


def test_capacity_is_the_tiers_share_and_not_the_tier(settings: Settings, clock: FakeClock):
    """**The N-times overspend, asserted directly.**

    With 2 replicas a free-tier caller must find 30 tokens locally, not 60. Two replicas each
    holding 60 is 120 — the precise failure this project exists to prevent, and reproducing it in
    the degraded mode would make degradation worse than the outage it degrades from.
    """
    two = build(settings, clock, api_replicas=2)
    one = build(settings, FakeClock(), api_replicas=1)

    assert two.capacity_for(60) == 30
    assert one.capacity_for(60) == 60
    # The refill RATE is divided too. A bucket of 30 refilling at the full 60/min would admit 60
    # per minute sustained — the same overspend arriving a minute later instead of immediately.
    assert two.rate_for(60) == 30
    assert one.rate_for(60) == 60


def test_the_replica_count_is_read_per_call_rather_than_cached(
    settings: Settings, clock: FakeClock
):
    """Sizing follows ``Settings``, so a future admin route that changes it is not fighting a cache."""
    cache = build(settings, clock, api_replicas=4)
    assert cache.capacity_for(60) == 15
    assert cache.stats()["replicas"] == 4


# =============================================================================================
# Allow exactly capacity, then deny
# =============================================================================================


def test_it_allows_exactly_capacity_and_then_denies(settings: Settings, clock: FakeClock):
    """Exactly N, not N-1 and not N+1 — the assertion a rate limiter is for.

    The clock does not move, so no refill can rescue the (N+1)-th request and the number is a
    statement about the bucket rather than about how fast the loop ran.
    """
    cache = build(settings, clock)

    allowed = sum(spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed for _ in range(20))

    assert allowed == 5
    assert cache.allows == 5
    assert cache.denies == 15


def test_a_denial_reports_a_retry_that_is_never_zero(settings: Settings, clock: FakeClock):
    """`Retry-After: 0` is a retry storm the limiter manufactured for itself.

    At 60 rpm one token takes exactly 1000 ms, so the first refused request is told 1000 — a real
    number derived from the refill rate, not a floor that happens to be non-zero.
    """
    cache = build(settings, clock)
    for _ in range(5):
        spend(cache, KEY, capacity=5, rpm=60, cost=1)

    verdict = spend(cache, KEY, capacity=5, rpm=60, cost=1)

    assert verdict.allowed is False
    assert verdict.retry_ms == 1000
    assert verdict.remaining == 0


def test_a_weighted_cost_is_charged_in_full(settings: Settings, clock: FakeClock):
    """The bonus's weighted cost has to work on the degraded path too, or `/logs/query` is free."""
    cache = build(settings, clock)

    first = spend(cache, KEY, capacity=10, rpm=60, cost=5)
    second = spend(cache, KEY, capacity=10, rpm=60, cost=5)
    third = spend(cache, KEY, capacity=10, rpm=60, cost=5)

    assert (first.allowed, second.allowed, third.allowed) == (True, True, False)
    assert first.remaining == 5
    assert second.remaining == 0


def test_a_cost_larger_than_capacity_reports_a_bounded_retry(
    settings: Settings, clock: FakeClock
):
    """A request this bucket can NEVER admit gets the time to a full bucket, not a fantasy.

    Mirrors the script's identical clamp: there is no honest interval for "come back when you can
    spend 9 tokens from a bucket that holds 4", so the answer is bounded at a full refill rather
    than promising a wait that would not work.
    """
    cache = build(settings, clock)

    verdict = spend(cache, KEY, capacity=4, rpm=60, cost=9)

    assert verdict.allowed is False
    # capacity 4 at 60 rpm = 4000 ms to fill from empty.
    assert verdict.retry_ms == 4000


def test_a_zero_capacity_bucket_denies_rather_than_meaning_unlimited(
    settings: Settings, clock: FakeClock
):
    """**The one deliberate divergence from the Lua script**, and it is the safe direction.

    In the decision script a non-positive limit is the documented "this gate is not enforcing
    anything" convention. On the degraded path the same reading would be a silent unmetered request
    during an outage — exactly what `X-RateLimit-Degraded` exists to make impossible to have
    quietly. So no capacity means no allowance.
    """
    cache = build(settings, clock)

    verdict = spend(cache, KEY, capacity=0, rpm=0, cost=1)

    assert verdict.allowed is False
    assert verdict.retry_ms == 1
    assert verdict.reset_ms == 0


# =============================================================================================
# Refill
# =============================================================================================


def test_it_refills_at_the_configured_rate(settings: Settings, clock: FakeClock):
    """60 rpm is one token per second, and the injected clock makes that exact.

    Half a second buys nothing (the bucket holds fractional micro-tokens but reports whole ones and
    refuses a whole-token spend); a full second buys exactly one request back.
    """
    cache = build(settings, clock)
    for _ in range(5):
        spend(cache, KEY, capacity=5, rpm=60, cost=1)
    assert spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed is False

    clock.advance(0.5)
    assert spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed is False

    clock.advance(0.5)
    assert spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed is True
    # ...and exactly one, not two: the second is refused again immediately.
    assert spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed is False


def test_refill_is_clamped_at_capacity_after_a_long_idle(settings: Settings, clock: FakeClock):
    """An hour of silence must not become an hour's worth of tokens.

    This is the clamp that also keeps `elapsed * rpm * MICRO` far below 2**53, which is where the
    script's identical arithmetic would stop being exact.
    """
    cache = build(settings, clock)
    for _ in range(5):
        spend(cache, KEY, capacity=5, rpm=60, cost=1)

    clock.advance(3600)

    allowed = sum(spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed for _ in range(20))
    assert allowed == 5


def test_a_partially_drained_bucket_is_also_clamped(settings: Settings, clock: FakeClock):
    """The clamp has to bind on the level, not only on an empty bucket.

    Draining an *empty* bucket for a full refill period lands exactly on capacity, so it never
    exercises the ceiling. A bucket that was only half spent overshoots it — and an unclamped
    overshoot is stored, which would let a caller who paused for an hour bank two buckets' worth of
    allowance and spend it in one burst.
    """
    cache = build(settings, clock)
    for _ in range(2):
        spend(cache, KEY, capacity=5, rpm=60, cost=1)

    clock.advance(3600)

    allowed = sum(spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed for _ in range(20))
    assert allowed == 5


def test_reset_ms_is_the_time_to_a_full_bucket(settings: Settings, clock: FakeClock):
    """`X-RateLimit-Reset` on the degraded path comes from here, so it has to be the real number."""
    cache = build(settings, clock)

    verdict = spend(cache, KEY, capacity=5, rpm=60, cost=1)

    # 4 of 5 tokens left, 1 token per second: full again in 1000 ms.
    assert verdict.remaining == 4
    assert verdict.reset_ms == 1000
    # Spending 3 more triples the wait, because it is a level and not a constant.
    for _ in range(3):
        verdict = spend(cache, KEY, capacity=5, rpm=60, cost=1)
    assert verdict.remaining == 1
    assert verdict.reset_ms == 4000


def test_a_clock_that_goes_backwards_credits_nothing(settings: Settings, clock: FakeClock):
    """`time.monotonic` cannot step, so this guards the injected clock — and the direction matters.

    Clamping to zero is the answer that cannot over-credit. A negative elapsed fed into the refill
    would *subtract* tokens from a caller who did nothing wrong, and an unclamped absolute value
    would hand them a refill for time that never passed.
    """
    cache = build(settings, clock)
    spend(cache, KEY, capacity=5, rpm=60, cost=1)

    clock.advance(-30)

    verdict = spend(cache, KEY, capacity=5, rpm=60, cost=1)
    assert verdict.allowed is True
    assert verdict.remaining == 3


def test_a_zero_rate_bucket_never_refills(settings: Settings, clock: FakeClock):
    """A tier with no refill rate drains once and stays drained until the outage ends.

    Unreachable from `rate_for` (which floors at 1 for any positive tier), and reachable from a
    hand-built call — so the arithmetic is pinned rather than left to divide by zero.
    """
    cache = build(settings, clock)
    assert spend(cache, KEY, capacity=1, rpm=0, cost=1).allowed is True

    clock.advance(3600)

    verdict = spend(cache, KEY, capacity=1, rpm=0, cost=1)
    assert verdict.allowed is False
    assert verdict.retry_ms == 1
    assert verdict.reset_ms == 0


# =============================================================================================
# The LRU bound
# =============================================================================================


def test_it_evicts_the_least_recently_used_bucket_at_the_cap(settings: Settings):
    """The cap evicts from the COLD end, and a fresh insert is the thing that triggers it."""
    cache = LocalBucketCache(settings, clock=FakeClock(), max_entries=3)

    for name in ("a", "b", "c"):
        spend(cache, name, capacity=5, rpm=60, cost=1)
    assert len(cache) == 3

    spend(cache, "d", capacity=5, rpm=60, cost=1)

    assert len(cache) == 3
    # The oldest went; the two that were still warm, and the newcomer, stayed.
    assert "a" not in cache
    assert all(name in cache for name in ("b", "c", "d"))
    assert cache.evictions == 1


def test_touching_a_key_moves_it_to_the_fresh_end(settings: Settings):
    """**LRU, not FIFO** — and here that is a limit property rather than a cache-hit-rate one.

    An evicted bucket comes back FULL. Under a FIFO the busiest caller in the process would be
    evicted on a fixed cycle and handed a fresh full bucket each time, so the callers using the
    most allowance would be the ones getting extra. Re-touching ``a`` must therefore save it and
    condemn ``b``.
    """
    cache = LocalBucketCache(settings, clock=FakeClock(), max_entries=3)
    for name in ("a", "b", "c"):
        spend(cache, name, capacity=5, rpm=60, cost=1)

    spend(cache, "a", capacity=5, rpm=60, cost=1)  # a is now the freshest
    spend(cache, "d", capacity=5, rpm=60, cost=1)  # ...so b is the coldest

    assert "a" in cache
    assert "b" not in cache
    assert len(cache) == 3


def test_size_never_exceeds_the_cap_under_a_flood_of_users(settings: Settings):
    """**The OOM guard.** The key space is `(user, endpoint)` and the caller chooses it.

    A degraded flood of 5 000 distinct principals must leave the process holding 50 buckets, not
    5 000. Trading an evicted bucket for an OOM kill is not close: an eviction costs its owner one
    bucket's worth of extra allowance, and an OOM costs the replica during the incident it was
    supposed to be riding out.
    """
    cache = LocalBucketCache(settings, clock=FakeClock(), max_entries=50)

    for index in range(5_000):
        spend(cache, f"rate_limit:{{user-{index}}}:GET:/x", capacity=5, rpm=60, cost=1)
        assert len(cache) <= 50

    assert len(cache) == 50
    assert cache.evictions == 4_950
    assert cache.creations == 5_000


def test_the_cap_is_floored_at_one(settings: Settings):
    """A cap of zero would evict the entry it just inserted on every call: overhead, no bucket."""
    cache = LocalBucketCache(settings, clock=FakeClock(), max_entries=0)

    spend(cache, KEY, capacity=5, rpm=60, cost=1)

    assert len(cache) == 1
    assert cache.stats()["max_entries"] == 1


def test_the_default_cap_is_the_documented_one(settings: Settings):
    assert FALLBACK_MAX_BUCKETS == 10_000
    assert LocalBucketCache(settings).stats()["max_entries"] == 10_000


def test_buckets_are_independent_per_key(settings: Settings, clock: FakeClock):
    """One principal draining their bucket must not refuse another's — that is what per-key means."""
    cache = build(settings, clock)
    for _ in range(5):
        spend(cache, KEY, capacity=5, rpm=60, cost=1)

    assert spend(cache, KEY, capacity=5, rpm=60, cost=1).allowed is False
    assert spend(cache, OTHER_KEY, capacity=5, rpm=60, cost=1).allowed is True


# =============================================================================================
# Two gates, one decision
#
# The section that only exists because the fallback used to reproduce the per-endpoint bucket and
# nothing else. See `src.fallback`'s opening rubric for the 5x that omission was worth; the
# account-wide half of the fix is asserted end to end in `tests/unit/test_degradation.py`, and what
# is pinned HERE is the machinery it needs: all gates evaluated, most restrictive wins, and a
# denial that spends nothing from anybody.
# =============================================================================================


def test_the_two_gates_are_sized_from_the_tier_and_both_are_divided(
    settings: Settings, clock: FakeClock
):
    """The bucket gets ``burst/N`` at ``rpm/N``; the account gate gets ``rpm/N`` **as both**.

    The account gate's ceiling and its refill rate being the same number is the whole idea rather
    than a shortcut: the shared gate it mirrors is a window of limit ``rpm`` over one minute, so a
    bucket holding ``ceil(rpm/N)`` and refilling ``ceil(rpm/N)`` per minute enforces the identical
    sustained rate.
    """
    cache = build(settings, clock, api_replicas=2)
    tier = settings.tier_limits["free"]  # 60 rpm, 60 burst

    bucket = cache.bucket_gate(KEY, tier)
    account = cache.account_gate(ACCOUNT_KEY, tier)

    assert (bucket.key, bucket.capacity, bucket.rpm) == (KEY, 30, 30)
    assert (account.key, account.capacity, account.rpm) == (ACCOUNT_KEY, 30, 30)


def test_a_request_is_admitted_only_if_every_gate_admits(settings: Settings, clock: FakeClock):
    """The AND across gates — a full bucket does not rescue an empty account gate.

    This is the shape of the bug the account gate exists to fix, in miniature: the per-endpoint
    bucket for a *fresh* endpoint is always full, so a caller who has spent their whole minute
    elsewhere would sail through it. Only a gate that does not know what an endpoint is can refuse
    them.
    """
    cache = build(settings, clock)
    roomy = GateSpec(key=KEY, capacity=5, rpm=60)
    tight = GateSpec(key=ACCOUNT_KEY, capacity=1, rpm=60)

    first = cache.consume(roomy, tight, cost=1)
    second = cache.consume(roomy, tight, cost=1)

    assert first.allowed is True
    assert second.allowed is False
    # ...and the verdicts say WHICH gate refused, in the order the gates were passed.
    assert [verdict.allowed for verdict in second.verdicts] == [True, False]


def test_a_denial_by_one_gate_spends_nothing_from_the_other(
    settings: Settings, clock: FakeClock
):
    """**A denial writes nothing** — the script's rule, and it only became assertable at two gates.

    Spending the bucket's token and *then* discovering the account gate refuses would charge a
    caller for a request that was never served. Worse, it compounds: a client in a retry loop would
    drain its own per-endpoint bucket with requests it is being refused, so the refusal would
    outlive the condition that caused it and the caller would still be blocked after the account
    gate had recovered.

    Ten refused attempts, and the roomy gate must still report the same four tokens after every
    one of them.
    """
    cache = build(settings, clock)
    roomy = GateSpec(key=KEY, capacity=5, rpm=0)
    tight = GateSpec(key=ACCOUNT_KEY, capacity=1, rpm=0)

    assert cache.consume(roomy, tight, cost=1).allowed is True

    for _ in range(10):
        decision = cache.consume(roomy, tight, cost=1)
        assert decision.allowed is False
        # Unchanged since the one admitted request. Not 3, not 0 — nothing was taken.
        assert decision.verdicts[0].remaining == 4

    assert cache.allows == 1
    assert cache.denies == 10


def test_the_combined_retry_is_the_max_across_the_gates_that_refused(
    settings: Settings, clock: FakeClock
):
    """The furthest wall, not the nearest — the decision script's rule, for the same reason.

    Telling a caller blocked by two gates to come back when the *nearer* one clears is telling
    them to be refused again, and every one of those retries is load this service refused and
    still had to serve.
    """
    cache = build(settings, clock)
    # Both empty and both refusing, but at different refill rates: 60 rpm is 1 000 ms per token,
    # 6 rpm is 10 000 ms.
    fast = GateSpec(key=KEY, capacity=1, rpm=60)
    slow = GateSpec(key=ACCOUNT_KEY, capacity=1, rpm=6)
    cache.consume(fast, slow, cost=1)

    decision = cache.consume(fast, slow, cost=1)

    assert decision.allowed is False
    assert [verdict.retry_ms for verdict in decision.verdicts] == [1_000, 10_000]
    assert decision.retry_ms == 10_000


def test_a_refusing_gate_never_reports_a_zero_retry(settings: Settings, clock: FakeClock):
    """The floor is applied per gate, not only to the combined answer.

    The limiter reads individual verdicts, so a gate that refuses with ``retry_ms == 0`` would
    become a ``Retry-After: 0`` — a retry storm the limiter manufactured for itself — even though
    the combined number was floored. Reachable when a gate never refills at all.
    """
    cache = build(settings, clock)
    never = GateSpec(key=KEY, capacity=0, rpm=0)

    decision = cache.consume(never, cost=1)

    assert decision.verdicts[0].retry_ms == 1
    assert decision.retry_ms == 1


def test_one_cap_covers_both_key_spaces(settings: Settings):
    """**The OOM guard, restated for two maps.** One ``OrderedDict``, so the cap bounds the total.

    Two 10 000-entry maps would be a 20 000-entry bound wearing a 10 000-entry label — and a
    degraded flood populates *both*, one entry per principal plus one per principal-endpoint pair.
    The two key spaces cannot collide (``rate_limit:...`` versus ``sw:...``), which is what makes
    sharing one map safe as well as tighter.
    """
    cache = LocalBucketCache(settings, clock=FakeClock(), max_entries=50)

    for index in range(1_000):
        cache.consume(
            GateSpec(key=f"rate_limit:{{user-{index}}}:GET:/x", capacity=5, rpm=60),
            GateSpec(key=f"sw:{{user-{index}}}", capacity=5, rpm=60),
            cost=1,
        )
        assert len(cache) <= 50

    assert len(cache) == 50
    # Two gates per request, every one of them new: 2 000 creations, 1 950 of them evicted again.
    assert cache.creations == 2_000
    assert cache.evictions == 1_950
    # ...and the counters are per REQUEST, not per gate, so they still sum to the decisions made.
    assert cache.allows + cache.denies == 1_000


def test_a_two_gate_request_cannot_evict_its_own_first_gate(settings: Settings):
    """Eviction runs once, after every gate of the request has been stored.

    Evicting inside the write loop would let the second gate drop the first — which the caller has
    already been charged for — and hand the next request a freshly full one. With a cap of exactly
    two, both gates of the newest request must survive.
    """
    cache = LocalBucketCache(settings, clock=FakeClock(), max_entries=2)
    cache.consume(GateSpec(key="cold", capacity=5, rpm=60), cost=1)

    cache.consume(
        GateSpec(key=KEY, capacity=5, rpm=60),
        GateSpec(key=ACCOUNT_KEY, capacity=5, rpm=60),
        cost=1,
    )

    assert len(cache) == 2
    assert KEY in cache
    assert ACCOUNT_KEY in cache
    assert "cold" not in cache


def test_no_gates_at_all_is_an_admission_with_nothing_to_report(
    settings: Settings, clock: FakeClock
):
    """Degenerate, unreachable from the limiter, and pinned so it cannot become a silent allow-all.

    ``consume()`` with no gates is vacuously allowed — that is what an AND over an empty set means
    — and the assertion here is that it stays *visible*: no verdicts, so the limiter has nothing to
    build a header from and would raise rather than fabricate one. The limiter always passes at
    least the bucket gate; this pins the arithmetic rather than blessing the call.
    """
    cache = build(settings, clock)

    decision = cache.consume(cost=1)

    assert decision.allowed is True
    assert decision.verdicts == ()
    assert decision.retry_ms == 0
    assert len(cache) == 0


# =============================================================================================
# Bookkeeping
# =============================================================================================


def test_clear_drops_the_buckets_and_keeps_the_counters(settings: Settings, clock: FakeClock):
    """Counters are lifetime totals for `/health` and C11.

    Resetting them here would produce a degradation metric that silently restarts whenever anyone
    cleared the cache — i.e. one that only ever looks fine.
    """
    cache = build(settings, clock)
    spend(cache, KEY, capacity=5, rpm=60, cost=1)

    cache.clear()

    assert len(cache) == 0
    assert cache.allows == 1
    assert cache.creations == 1


def test_stats_publishes_what_an_incident_review_needs(settings: Settings, clock: FakeClock):
    cache = build(settings, clock, api_replicas=2)
    for _ in range(6):
        spend(cache, KEY, capacity=5, rpm=60, cost=1)

    assert cache.stats() == {
        "size": 1,
        "max_entries": FALLBACK_MAX_BUCKETS,
        "replicas": 2,
        "allows": 5,
        "denies": 1,
        "evictions": 0,
        "creations": 1,
    }


def test_the_refill_arithmetic_matches_the_scripts_constants():
    """The per-minute constant is imported from `src.keys`, not restated.

    The script interpolates the same `MS_PER_MINUTE` into its Lua, so a caller crossing from the
    shared bucket to this one and back cannot see the limiter change its arithmetic underneath
    them. Two copies that disagreed would refill at two different rates with nothing raising.
    """
    assert MS_PER_MINUTE == 60_000
