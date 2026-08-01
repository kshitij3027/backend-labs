"""Unit tests for the circuit breaker's state machine — no Redis, no sleeps.

The breaker is the thing that keeps a Redis outage from becoming a latency outage. Without it,
"failing open" still costs every request the full 250 ms socket timeout before it can decide to
fail open; at 1000 rps that parks 250 coroutines on a dead socket at any instant, saturates the
bounded pool, and stampedes Redis the moment it comes back. With it, the second failure onward
costs nothing and a recovering Redis sees exactly one probe per cooldown.

Every test here drives an **injected fake clock**. That is not only about speed: a test that
`sleep`s for the real 5 s cooldown is a test that is slow *and* flaky (it asserts on wall-clock
scheduling under a loaded CI box), and the interesting assertion — "the boundary is at exactly
`cooldown`, not `cooldown ± scheduling jitter`" — is one only a controllable clock can make.
"""

import pytest

from src.config import Settings
from src.redis_client import BreakerState, CircuitBreaker


class FakeClock:
    """A monotonic clock the test advances by hand. Callable, so it drops straight in."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def breaker(clock: FakeClock) -> CircuitBreaker:
    """A breaker with the shipped defaults (5 consecutive failures, 5 s cooldown)."""
    return CircuitBreaker(failure_threshold=5, cooldown_sec=5.0, clock=clock)


def _fail(breaker: CircuitBreaker, times: int) -> None:
    """Record ``times`` failures, each preceded by the `allow_request()` a real call would make."""
    for _ in range(times):
        breaker.allow_request()
        breaker.record_failure()


# --------------------------------------------------------------------------------------------
# Closed
# --------------------------------------------------------------------------------------------


def test_a_fresh_breaker_is_closed_and_allows_everything(breaker):
    assert breaker.state is BreakerState.CLOSED
    assert breaker.is_open is False
    assert breaker.consecutive_failures == 0
    assert all(breaker.allow_request() for _ in range(10))


def test_the_defaults_come_from_settings(settings: Settings):
    """The shipped numbers, so a Settings change cannot silently detune the breaker."""
    assert settings.breaker_failures == 5
    assert settings.breaker_cooldown_sec == 5


# --------------------------------------------------------------------------------------------
# Opening
# --------------------------------------------------------------------------------------------


def test_it_opens_after_exactly_the_threshold_number_of_failures(breaker):
    """Four failures is a bad minute; five in a row is an outage. The boundary is asserted on both
    sides so an off-by-one cannot pass."""
    _fail(breaker, 4)
    assert breaker.state is BreakerState.CLOSED
    assert breaker.allow_request() is True
    assert breaker.consecutive_failures == 4

    breaker.record_failure()

    assert breaker.state is BreakerState.OPEN
    assert breaker.is_open is True
    assert breaker.consecutive_failures == 5


def test_failures_must_be_CONSECUTIVE_not_cumulative(breaker):
    """A success mid-sequence resets the run to zero.

    A counter that only increments trips eventually on ANY long-running healthy process: five
    transient blips over a week is not an outage, it is a week. Only an unbroken run is evidence
    that the store is down.
    """
    _fail(breaker, 4)
    assert breaker.consecutive_failures == 4

    breaker.record_success()
    assert breaker.consecutive_failures == 0
    assert breaker.state is BreakerState.CLOSED

    # Four more failures — nine cumulative, four consecutive. Still closed.
    _fail(breaker, 4)
    assert breaker.state is BreakerState.CLOSED
    assert breaker.allow_request() is True


def test_an_open_breaker_refuses_without_advancing_the_clock(breaker, clock):
    """Refusal is free: no socket, no timeout, no time spent. That is the whole point.

    In production this is the difference between a request costing 250 ms during an outage and
    costing 0 ms. Here it shows up as: the clock never moved, and the answer was still `False`.
    """
    _fail(breaker, 5)
    before = clock.now

    assert [breaker.allow_request() for _ in range(50)] == [False] * 50
    assert clock.now == before


def test_a_threshold_of_zero_is_clamped_to_one(clock):
    """A misconfigured `BREAKER_FAILURES=0` must not make the breaker trip before any call."""
    breaker = CircuitBreaker(failure_threshold=0, cooldown_sec=5.0, clock=clock)

    assert breaker.allow_request() is True
    breaker.record_failure()
    assert breaker.state is BreakerState.OPEN


# --------------------------------------------------------------------------------------------
# The cooldown and the single probe
# --------------------------------------------------------------------------------------------


def test_the_cooldown_boundary_is_exact(breaker, clock):
    """The advances are halves so the fake clock stays exact in binary floating point — a test
    about an exact boundary must not itself be approximate."""
    _fail(breaker, 5)

    clock.advance(4.5)
    assert breaker.allow_request() is False
    assert breaker.state is BreakerState.OPEN

    clock.advance(0.5)  # exactly 5.0 s since it opened
    assert breaker.allow_request() is True
    assert breaker.state is BreakerState.HALF_OPEN


def test_after_the_cooldown_exactly_ONE_probe_is_allowed(breaker, clock):
    """Letting the whole backlog through at the cooldown boundary is the thundering herd the
    breaker exists to prevent, merely re-armed on a timer."""
    _fail(breaker, 5)
    clock.advance(5.0)

    allowed = [breaker.allow_request() for _ in range(20)]

    assert allowed == [True] + [False] * 19
    assert breaker.state is BreakerState.HALF_OPEN


def test_a_successful_probe_closes_the_breaker(breaker, clock):
    _fail(breaker, 5)
    clock.advance(5.0)
    assert breaker.allow_request() is True

    breaker.record_success()

    assert breaker.state is BreakerState.CLOSED
    assert breaker.is_open is False
    assert breaker.consecutive_failures == 0
    assert all(breaker.allow_request() for _ in range(10))


def test_a_failed_probe_reopens_and_restarts_the_FULL_cooldown(breaker, clock):
    """Re-opening on the probe's own failure — not on a sixth cumulative failure — is what makes a
    long outage cost one probe per cooldown rather than one probe per request."""
    _fail(breaker, 5)
    clock.advance(5.0)
    assert breaker.allow_request() is True  # the probe

    breaker.record_failure()

    assert breaker.state is BreakerState.OPEN
    # The cooldown restarts from the probe's failure, so almost-a-full-cooldown is still refused.
    clock.advance(4.5)
    assert breaker.allow_request() is False
    clock.advance(0.5)
    assert breaker.allow_request() is True


def test_half_open_is_not_reported_as_open(breaker, clock):
    """`is_open` means "we have given up dialling", which mid-recovery is not true."""
    _fail(breaker, 5)
    clock.advance(5.0)
    breaker.allow_request()

    assert breaker.state is BreakerState.HALF_OPEN
    assert breaker.is_open is False


def test_reading_state_never_consumes_the_probe(breaker, clock):
    """`state` is a pure read, deliberately.

    `/health` reads this on every 10 s container probe. If observing the state performed the
    OPEN -> HALF_OPEN transition, the health check would keep eating the single probe slot a real
    request needed, and the breaker would never actually close under production traffic.
    """
    _fail(breaker, 5)
    clock.advance(60.0)

    for _ in range(10):
        assert breaker.state is BreakerState.OPEN
        assert breaker.is_open is True

    assert breaker.allow_request() is True  # the probe is still available
    assert breaker.state is BreakerState.HALF_OPEN


def test_a_failure_reported_while_open_does_not_extend_the_cooldown(breaker, clock):
    """Unreachable through `RedisGateway.run`, but it must not be a way to wedge the breaker.

    If a stray `record_failure()` re-armed the timer, a caller that reported failures faster than
    the cooldown could keep the breaker open forever — permanently degrading a service whose Redis
    had already recovered.
    """
    _fail(breaker, 5)
    clock.advance(4.0)

    breaker.record_failure()

    assert breaker.state is BreakerState.OPEN
    assert breaker.consecutive_failures == 6
    clock.advance(1.0)  # 5.0 s since it opened, despite the extra failure
    assert breaker.allow_request() is True


def test_a_zero_cooldown_probes_immediately(clock):
    """`BREAKER_COOLDOWN_SEC=0` degrades to "one probe per call", not to a wedged breaker."""
    breaker = CircuitBreaker(failure_threshold=2, cooldown_sec=0, clock=clock)
    _fail(breaker, 2)

    assert breaker.state is BreakerState.OPEN
    assert breaker.allow_request() is True
    assert breaker.state is BreakerState.HALF_OPEN


def test_a_negative_cooldown_is_clamped_to_zero(clock):
    breaker = CircuitBreaker(failure_threshold=1, cooldown_sec=-10, clock=clock)
    breaker.record_failure()

    assert breaker.is_open is True
    assert breaker.allow_request() is True


def test_the_default_clock_is_monotonic():
    """No injected clock => `time.monotonic`, so an NTP step cannot end a cooldown early.

    Constructed without a clock and driven with real time: a fresh breaker allows, five failures
    open it, and the cooldown has obviously not elapsed in the microseconds since.
    """
    breaker = CircuitBreaker(failure_threshold=5, cooldown_sec=5.0)

    assert breaker.allow_request() is True
    _fail(breaker, 5)
    assert breaker.is_open is True
    assert breaker.allow_request() is False


def test_the_full_outage_and_recovery_cycle(breaker, clock):
    """One test that walks the whole machine, in the order a real outage produces it."""
    # Healthy.
    assert breaker.allow_request() and breaker.state is BreakerState.CLOSED
    breaker.record_success()

    # Redis dies: five requests pay the timeout, then nobody does.
    _fail(breaker, 5)
    assert breaker.is_open

    # Two cooldowns pass with Redis still down: two probes total, not two thousand.
    probes = 0
    for _ in range(2):
        clock.advance(5.0)
        for _ in range(1_000):
            if breaker.allow_request():
                probes += 1
                breaker.record_failure()
    assert probes == 2

    # Redis recovers: the next probe succeeds and full service resumes.
    clock.advance(5.0)
    assert breaker.allow_request() is True
    breaker.record_success()
    assert breaker.state is BreakerState.CLOSED
    assert all(breaker.allow_request() for _ in range(100))
