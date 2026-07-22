"""Unit tests for the token bucket and the per-principal limiter.

**There is not a single ``sleep()`` in this file, and there must never be one.** A rate limiter
is defined entirely in terms of elapsed time, so the naive way to test it is to sleep past a
refill window — which buys a suite that is slow by construction and flaky on a loaded CI box,
where a ``sleep(0.1)`` routinely returns after 0.15 s and the assertion about "exactly one
token" fails for reasons that have nothing to do with the code. Every test here drives
:class:`FakeClock` instead: time only moves when a test says so, by exactly as much as it says,
so the expected token counts are computable by hand and identical on every machine.

The one test that does use the real clock (``test_monotonic_clock_is_used_not_wall_clock``)
never waits on it — it pins the default and then jumps a *fake* ``time.time`` to prove the
module is not reading it.
"""

from __future__ import annotations

import inspect
import math
import time

import pytest

from src.auth import Tier
from src.config import Settings, TierLimit
from src.ratelimit import (
    HEADER_LIMIT,
    HEADER_REMAINING,
    HEADER_RESET,
    HEADER_RETRY_AFTER,
    RATE_LIMIT_HEADERS,
    Decision,
    RateLimiter,
    TokenBucket,
)

#: The README's free tier, used by most tests because 20 takes is a readable loop and 10/s makes
#: every refill interval a round 0.1 s.
FREE_RATE, FREE_BURST = 10.0, 20.0


class FakeClock:
    """A manually-driven monotone clock: reading it never moves it, only :meth:`advance` does.

    Deliberately *not* the auto-advancing ``FakeClock`` in ``test_store.py``. That one models a
    clock sampled once per event; a bucket reads the clock a variable number of times per test
    (``take`` refills, ``peek`` refills, the limiter samples it again for the sweep), so a
    per-read step would make the elapsed time depend on how many internal reads the
    implementation happens to make — i.e. a test that fails when the code is refactored without
    changing behaviour.
    """

    def __init__(self, start: float = 1000.0) -> None:
        # Starts well above zero so a bug that treats "0" as "unset" cannot pass by accident.
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("FakeClock only moves forward; that is the point of monotonic")
        self.now += seconds


def make_bucket(
    rate: float = FREE_RATE, burst: float = FREE_BURST
) -> tuple[TokenBucket, FakeClock]:
    """A bucket and the clock that drives it."""
    clock = FakeClock()
    return TokenBucket(rate, burst, time_func=clock), clock


def make_limiter(**kwargs: object) -> tuple[RateLimiter, FakeClock]:
    """A limiter over the README's three tiers, and the clock that drives it."""
    clock = FakeClock()
    tiers = {
        "free": TierLimit(rate=10, burst=20),
        "pro": TierLimit(rate=100, burst=200),
        "enterprise": TierLimit(rate=1000, burst=2000),
    }
    return RateLimiter(tiers, time_func=clock, **kwargs), clock  # type: ignore[arg-type]


def drain(bucket: TokenBucket) -> int:
    """Spend the bucket dry and return how many takes were allowed."""
    allowed = 0
    while bucket.take().allowed:
        allowed += 1
        if allowed > 10_000:  # pragma: no cover - a refill bug would otherwise hang the suite
            raise AssertionError("bucket never emptied; refill is running without a clock")
    return allowed


# ---------------------------------------------------------------------------------------------
# The bucket: capacity and burst
# ---------------------------------------------------------------------------------------------


def test_bucket_starts_full() -> None:
    """A never-seen client gets its documented burst immediately, not an empty bucket."""
    bucket, _clock = make_bucket()

    decision = bucket.peek()

    assert decision.allowed is True
    assert decision.limit == 20
    assert decision.remaining == 20
    assert decision.reset_after == 0.0
    assert decision.retry_after == 0


def test_burst_allows_capacity_then_denies() -> None:
    """Exactly `burst` requests pass instantaneously; the next one does not."""
    bucket, _clock = make_bucket()

    for i in range(int(FREE_BURST)):
        decision = bucket.take()
        assert decision.allowed is True, f"request {i + 1} of {FREE_BURST:.0f} was denied"
        assert decision.remaining == int(FREE_BURST) - (i + 1)

    denied = bucket.take()
    assert denied.allowed is False
    assert denied.remaining == 0


def test_denied_request_does_not_consume_a_token() -> None:
    """Refusals must not debit the bucket, or a hammering client can never recover.

    Three denials at an empty bucket, then exactly one refill period of credit. With penalty
    accounting the balance would sit at -3 and this would need four refill periods to buy one
    request back — the limiter would have quietly become a ban that gets longer the harder you
    knock.
    """
    bucket, clock = make_bucket()
    drain(bucket)

    for _ in range(3):
        assert bucket.take().allowed is False

    clock.advance(1 / FREE_RATE)  # exactly one token's worth of time

    assert bucket.take().allowed is True
    assert bucket.take().allowed is False


def test_refill_restores_tokens_over_time() -> None:
    """Half a second at 10/s is five tokens back — no more, no fewer."""
    bucket, clock = make_bucket()
    drain(bucket)
    assert bucket.peek().remaining == 0

    clock.advance(0.5)

    assert bucket.peek().remaining == 5


@pytest.mark.parametrize(
    ("elapsed", "expected_tokens"),
    [
        (0.25, 2),  # 2.5 tokens earned; `remaining` reports WHOLE spendable tokens
        (0.5, 5),
        (1.0, 10),
        (2.0, 20),
        (4.0, 40),
    ],
)
def test_refill_is_proportional_to_elapsed_time(elapsed: float, expected_tokens: int) -> None:
    """Credit is continuous — `elapsed * rate` — not quantised to whole refill intervals.

    Burst is 100 here so none of these cases hits the ceiling; that boundary is
    ``test_refill_never_exceeds_burst``'s job. Every interval is exactly representable in binary
    so the expectation is exact rather than approximate.
    """
    bucket, clock = make_bucket(rate=FREE_RATE, burst=100.0)
    drain(bucket)

    clock.advance(elapsed)

    assert bucket.peek().remaining == expected_tokens


def test_refill_never_exceeds_burst() -> None:
    """An hour of silence buys a burst, not an hour's worth of requests.

    Without the cap a client idle for 3600 s would hold 36,000 tokens and could flatten a
    downstream with a single script — which is a sustained limit with no burst limit at all.
    """
    bucket, clock = make_bucket()
    drain(bucket)

    clock.advance(3600.0)

    assert bucket.peek().remaining == int(FREE_BURST)
    assert bucket.tokens == FREE_BURST


def test_sustained_rate_is_enforced_over_time() -> None:
    """Over a simulated window the throughput converges on `rate`, proving it is a real limiter.

    A plain counter would let all 1000 attempts through, and a fixed window would let 10 through
    per second boundary regardless of spacing. The exact expectation here is
    ``burst + rate * seconds`` = 20 + 10 x 10 = 120: the initial burst is spendable at once, and
    everything after it is paced by the refill. The tolerance is one request, for the float
    boundary at the very last step, not for timing jitter — there is none.
    """
    bucket, clock = make_bucket()
    seconds, step = 10.0, 0.01

    burst_allowed = drain(bucket)
    assert burst_allowed == int(FREE_BURST)

    sustained_allowed = 0
    for _ in range(int(seconds / step)):
        clock.advance(step)
        if bucket.take().allowed:
            sustained_allowed += 1

    expected = FREE_RATE * seconds
    assert abs(sustained_allowed - expected) <= 1, (
        f"{sustained_allowed} allowed over {seconds}s at {FREE_RATE}/s (expected ~{expected})"
    )


def test_monotonic_clock_is_used_not_wall_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """A pin against ``time.time()`` ever sneaking in as the default clock.

    Both halves matter. The signature check states the intent; the behavioural half proves it,
    by moving the wall clock 10,000 seconds forward — an NTP correction, a DST step, a container
    resuming from suspend — and asserting the bucket hands out nothing. Under ``time.time`` that
    jump would be worth 10,000 seconds of free tokens, and the reverse jump would freeze every
    bucket in the process until real time caught up.
    """
    assert (
        inspect.signature(TokenBucket.__init__).parameters["time_func"].default
        is time.monotonic
    )
    assert (
        inspect.signature(RateLimiter.__init__).parameters["time_func"].default
        is time.monotonic
    )

    # One token per 1000 s, so even a badly stalled CI box cannot legitimately earn a token
    # between these statements. No injected clock here: this bucket must use the real default.
    bucket = TokenBucket(rate=0.001, burst=1.0)
    assert bucket.take().allowed is True

    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 10_000.0)

    assert bucket.peek().remaining == 0
    assert bucket.take().allowed is False


# ---------------------------------------------------------------------------------------------
# Decision: the numbers that reach the client
# ---------------------------------------------------------------------------------------------


def test_retry_after_is_at_least_one_when_denied() -> None:
    """A sub-second deficit still advertises 1 second, never 0.

    At 1000/s a single token is a millisecond away, so the honest ``ceil`` is 1 but a naive
    ``int()`` or ``round()`` would emit ``Retry-After: 0`` — which tells a throttled client to
    retry immediately and turns a 429 into a hot loop against the endpoint that just shed it.
    """
    bucket, _clock = make_bucket(rate=1000.0, burst=5.0)
    drain(bucket)

    denied = bucket.take()

    assert denied.allowed is False
    assert denied.retry_after == 1


def test_retry_after_is_zero_when_allowed() -> None:
    """An allowed request carries no back-off — the 429 raiser is what emits the header."""
    bucket, _clock = make_bucket()

    assert bucket.take().retry_after == 0
    assert bucket.peek().retry_after == 0


def test_reset_after_is_time_to_full() -> None:
    """`reset_after` counts down to a full bucket, not to the next single token."""
    bucket, clock = make_bucket()
    drain(bucket)

    # Empty at 10/s with capacity 20 -> two seconds to full.
    assert bucket.peek().reset_after == pytest.approx(2.0)

    clock.advance(1.0)
    assert bucket.peek().reset_after == pytest.approx(1.0)

    clock.advance(1.0)
    assert bucket.peek().reset_after == pytest.approx(0.0)
    assert bucket.peek().remaining == int(FREE_BURST)


def test_remaining_is_never_negative() -> None:
    """No sequence of refusals can drive the advertised remainder below zero."""
    bucket, _clock = make_bucket(rate=1.0, burst=2.0)
    drain(bucket)

    for _ in range(25):
        decision = bucket.take()
        assert decision.allowed is False
        assert decision.remaining == 0
        assert decision.remaining >= 0


def test_headers_match_expose_headers() -> None:
    """Every header the limiter emits must be in the CORS allowlist, or browser JS cannot read it.

    ``src.ratelimit`` cannot import ``src.main`` (the dependency arrow runs the other way, and
    the module must stay importable with no web framework present), so the two lists of header
    names are physically separate. This test is the only thing standing between them and a
    silent drift in which the API sets ``X-RateLimit-Remaining`` and the dashboard's ``fetch``
    is forbidden from seeing it.
    """
    from src.main import EXPOSE_HEADERS  # local: keeps the module import out of collection

    decision = Decision(allowed=True, limit=20, remaining=19, reset_after=0.1, retry_after=0)

    assert set(decision.headers()) <= set(EXPOSE_HEADERS)
    assert set(RATE_LIMIT_HEADERS) <= set(EXPOSE_HEADERS)
    # Emitted by the 429 raiser rather than by `headers()`, but same allowlist requirement.
    assert HEADER_RETRY_AFTER in EXPOSE_HEADERS


def test_reset_header_is_delay_seconds_not_a_timestamp() -> None:
    """`X-RateLimit-Reset` is "seconds from now", rounded up — never a UNIX epoch.

    The ``X-RateLimit-*`` family is inconsistent across the industry precisely because this
    field is a timestamp in some APIs and a duration in others. Emitting a delay matches the
    ``Retry-After`` on the same response and sidesteps client/server clock skew entirely.
    """
    bucket, _clock = make_bucket()
    drain(bucket)

    headers = bucket.peek().headers()

    assert headers[HEADER_LIMIT] == "20"
    assert headers[HEADER_REMAINING] == "0"
    # Two seconds to refill 20 tokens at 10/s. A timestamp would be ~1.7e9.
    assert headers[HEADER_RESET] == "2"

    # Rounded UP, so a client that waits exactly what it was told is never early.
    partial = Decision(allowed=True, limit=20, remaining=1, reset_after=1.2, retry_after=0)
    assert partial.headers()[HEADER_RESET] == "2"


def test_decision_is_immutable() -> None:
    """A decision records what happened; nothing downstream may rewrite it."""
    decision = Decision(allowed=False, limit=20, remaining=0, reset_after=2.0, retry_after=1)

    with pytest.raises((AttributeError, TypeError)):
        decision.allowed = True  # type: ignore[misc]


# ---------------------------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("rate", "burst"), [(0.0, 10.0), (-1.0, 10.0), (10.0, 0.0), (10.0, -5.0)])
def test_non_positive_sizing_is_rejected(rate: float, burst: float) -> None:
    """A zero rate never refills and a zero burst rejects everything — both are config bugs."""
    with pytest.raises(ValueError):
        TokenBucket(rate, burst)


def test_non_positive_take_is_rejected() -> None:
    """A free request is a bug, not a feature; `peek` is how you observe without spending."""
    bucket, _clock = make_bucket()

    with pytest.raises(ValueError):
        bucket.take(0)
    with pytest.raises(ValueError):
        bucket.take(-1)


def test_empty_tier_table_is_rejected() -> None:
    """With no tiers there is no most-restrictive tier to fail closed to."""
    with pytest.raises(ValueError):
        RateLimiter({})


# ---------------------------------------------------------------------------------------------
# The limiter: tiers
# ---------------------------------------------------------------------------------------------


def test_tier_sizes_match_readme(settings: Settings) -> None:
    """The three tiers are the README's three tiers, straight out of a real Settings.

    Read from configuration rather than restated as literals in the limiter: the numbers in the
    README, the ``TIER_LIMITS`` default spec, and what a principal actually gets are one value,
    not three that can drift.
    """
    assert settings.tier_limits["free"] == TierLimit(rate=10, burst=20)
    assert settings.tier_limits["pro"] == TierLimit(rate=100, burst=200)
    assert settings.tier_limits["enterprise"] == TierLimit(rate=1000, burst=2000)

    # Every Tier the auth layer can put in a token has a configured bucket. A tier without one
    # would fall back to the most restrictive tier, which is safe but wrong.
    assert {tier.value for tier in Tier} <= set(settings.tier_limits)

    limiter = RateLimiter(settings.tier_limits, time_func=FakeClock())
    assert limiter.acquire("a", Tier.FREE).limit == 20
    assert limiter.acquire("b", Tier.PRO).limit == 200
    assert limiter.acquire("c", Tier.ENTERPRISE).limit == 2000


def test_buckets_are_isolated_per_principal() -> None:
    """One principal exhausting its bucket must not touch anyone else's."""
    limiter, _clock = make_limiter()

    for _ in range(20):
        assert limiter.acquire("alice", Tier.FREE).allowed is True
    assert limiter.acquire("alice", Tier.FREE).allowed is False

    bob = limiter.acquire("bob", Tier.FREE)
    assert bob.allowed is True
    assert bob.remaining == 19
    assert limiter.bucket_count() == 2


def test_unknown_tier_falls_back_to_most_restrictive() -> None:
    """An unrecognised tier is throttled like the cheapest tier — never granted, never a 500.

    Failing closed is the only safe default for a limiter: raising would turn a config typo into
    a 500 on every authenticated request (the limiter taking the API down is worse than the load
    it prevents), and a missing entry read as "no limit" hands an unknown principal the whole
    machine.
    """
    limiter, _clock = make_limiter()

    decision = limiter.acquire("mystery", "platinum")

    assert decision.limit == 20  # free's burst, the most restrictive configured tier
    assert decision.remaining == 19
    for _ in range(19):
        limiter.acquire("mystery", "platinum")
    assert limiter.acquire("mystery", "platinum").allowed is False


def test_tier_change_between_tokens() -> None:
    """One bucket per subject, re-sized in place: a downgrade bites at once, an upgrade is not
    retroactive.

    Access tokens outlive a tier change, so for up to ``ACCESS_TOKEN_TTL_MIN`` a principal holds
    valid tokens for both the old tier and the new one. Keying buckets by ``(subject, tier)``
    would make those two independent buckets and the principal's real ceiling their *sum*;
    keying by subject bounds it by the higher of the two, which is what "a bucket per principal"
    has to mean for the number in the README to be true.
    """
    limiter, clock = make_limiter()

    # --- downgrade: enterprise balance is clamped to free's capacity, immediately ---
    rich = limiter.acquire("user", Tier.ENTERPRISE)
    assert rich.limit == 2000
    assert rich.remaining == 1999

    poor = limiter.acquire("user", Tier.FREE)
    assert poor.limit == 20
    assert poor.remaining == 19, "an enterprise balance survived a downgrade to free"
    assert limiter.bucket_count() == 1, "a tier change must not fork a second bucket"

    # --- upgrade: the ceiling and refill rate rise now, the tokens are earned, not gifted ---
    for _ in range(19):
        limiter.acquire("user", Tier.FREE)
    assert limiter.acquire("user", Tier.FREE).allowed is False

    upgraded = limiter.acquire("user", Tier.PRO)
    assert upgraded.limit == 200
    assert upgraded.remaining == 0
    assert upgraded.allowed is False, "an upgrade must not retroactively refill a drained bucket"

    clock.advance(1.0)  # one second at pro's 100/s
    refilled = limiter.acquire("user", Tier.PRO)
    assert refilled.allowed is True
    assert refilled.remaining == 99
    assert limiter.bucket_count() == 1


def test_disabled_limiter_always_allows_but_reports_ceiling() -> None:
    """`RATE_LIMIT_ENABLED=false` is an operability switch, not a change to the response shape.

    The headers stay present and truthful when enforcement is off, so a client pacing itself
    against ``X-RateLimit-Remaining`` does not have to handle the fields disappearing mid-flight
    when an operator flips the switch. No buckets are allocated either, which makes the switch a
    genuine relief valve under memory pressure rather than a branch that still leaks.
    """
    limiter, _clock = make_limiter(enabled=False)

    for _ in range(200):  # ten times free's burst
        decision = limiter.acquire("alice", Tier.FREE)
        assert decision.allowed is True
        assert decision.limit == 20
        assert decision.remaining == 20
        assert decision.retry_after == 0

    assert limiter.acquire("alice", Tier.ENTERPRISE).limit == 2000
    assert limiter.enabled is False
    assert limiter.bucket_count() == 0


# ---------------------------------------------------------------------------------------------
# The limiter: peeking
# ---------------------------------------------------------------------------------------------


def test_peek_does_not_consume() -> None:
    """Observation is free — at the bucket and at the limiter, present or absent."""
    bucket, _clock = make_bucket()
    for _ in range(10):
        assert bucket.peek().remaining == int(FREE_BURST)
    assert bucket.take().remaining == int(FREE_BURST) - 1

    limiter, _limiter_clock = make_limiter()

    # An absent bucket is reported as a full one and is NOT materialised: a peek must not be a
    # way to grow the bucket map, because the middleware peeks on paths (401/403) that never
    # acquire.
    assert limiter.peek("ghost", Tier.FREE).remaining == 20
    assert limiter.bucket_count() == 0

    limiter.acquire("alice", Tier.FREE)
    for _ in range(5):
        assert limiter.peek("alice", Tier.FREE).remaining == 19
    assert limiter.bucket_count() == 1


# ---------------------------------------------------------------------------------------------
# The limiter: memory bound
# ---------------------------------------------------------------------------------------------


def test_idle_buckets_are_swept() -> None:
    """Buckets idle past `idle_ttl` are dropped — otherwise the map is a slow memory leak.

    Dropping one is information-free: at every configured tier a bucket refills from empty to
    full in ``burst / rate`` = 2 s, so after 300 s of silence the stored bucket and a fresh one
    are indistinguishable by construction.
    """
    limiter, clock = make_limiter(idle_ttl=300.0)

    for subject in ("a", "b", "c"):
        limiter.acquire(subject, Tier.FREE)
    assert limiter.bucket_count() == 3

    clock.advance(301.0)

    assert limiter.sweep() == 3
    assert limiter.bucket_count() == 0


def test_sweep_keeps_active_buckets() -> None:
    """Only the idle ones go; a principal that kept calling keeps its bucket."""
    limiter, clock = make_limiter(idle_ttl=300.0)
    limiter.acquire("busy", Tier.FREE)
    limiter.acquire("quiet", Tier.FREE)

    clock.advance(200.0)
    limiter.acquire("busy", Tier.FREE)  # touches `busy` only
    clock.advance(200.0)  # busy idle 200 s, quiet idle 400 s

    assert limiter.sweep() == 1
    assert limiter.bucket_count() == 1
    # Identity, not just the count: reading the private map is deliberate here because "one
    # bucket survived" is not the claim — "the RIGHT bucket survived" is.
    assert "busy" in limiter._buckets
    assert "quiet" not in limiter._buckets


def test_idle_buckets_are_swept_without_an_explicit_call() -> None:
    """The sweep runs opportunistically off the request path, not only when an operator asks.

    Nothing in production calls ``sweep()`` on a timer — there is no background thread — so if
    ``acquire`` did not trigger it the bound would exist only in the tests.
    """
    limiter, clock = make_limiter(idle_ttl=300.0)
    limiter.acquire("gone", Tier.FREE)

    clock.advance(400.0)
    limiter.acquire("here", Tier.FREE)

    assert limiter.bucket_count() == 1
    assert "here" in limiter._buckets


def test_bucket_count_stays_bounded_under_many_principals() -> None:
    """Ten times `max_buckets` distinct subjects, all active, and the map still holds the line.

    Idle-sweeping alone cannot bound this — with the clock frozen nothing is idle — so the
    overflow path has to evict the least-recently-used. That is the module's one lossy
    operation: an evicted principal rebuilds a full bucket and so can win up to one extra burst.
    Bounded memory beats perfect fidelity, because the alternative failure mode is the process
    dying, and subjects come from JWTs this service signed, so an attacker cannot mint 10,000 of
    them to flush a victim's bucket.
    """
    limiter, _clock = make_limiter(max_buckets=50)

    for i in range(500):
        limiter.acquire(f"subject-{i:04d}", Tier.FREE)

    assert limiter.bucket_count() <= 50

    # Eviction is least-recently-used, so the newest principal is the one that survives: its
    # second request must see a bucket that remembers the first.
    assert limiter.acquire("subject-0499", Tier.FREE).remaining == 18


def test_reset_clears_every_bucket() -> None:
    """The operator/test escape hatch: everyone starts full again."""
    limiter, _clock = make_limiter()
    for _ in range(20):
        limiter.acquire("alice", Tier.FREE)
    assert limiter.acquire("alice", Tier.FREE).allowed is False

    limiter.reset()

    assert limiter.bucket_count() == 0
    assert limiter.acquire("alice", Tier.FREE).remaining == 19


def test_sweep_cannot_grant_tokens_at_the_configured_ttl() -> None:
    """The invariant that makes sweeping safe, stated as an assertion.

    A bucket may only be dropped once it is provably identical to a fresh one, i.e. after
    ``burst / rate`` seconds of idleness. If a future config ever inverted this, the sweep would
    stop being a memory optimisation and start handing partially-drained principals a full
    bucket.
    """
    default_ttl = inspect.signature(RateLimiter.__init__).parameters["idle_ttl"].default
    tiers = Settings(jwt_secret="test-only-insecure-signing-key-0123456789").tier_limits

    slowest_refill = max(limit.burst / limit.rate for limit in tiers.values())

    assert math.isclose(slowest_refill, 2.0)
    assert slowest_refill < default_ttl
