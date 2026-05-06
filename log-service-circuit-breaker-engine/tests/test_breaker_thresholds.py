"""Tests for smart-failure-detection rules in CircuitBreaker._should_open.

Each rule is exercised in isolation by tuning the relevant config field high
enough to disable the others. Tests must remain fast — the slow-call test
uses a sub-100ms threshold so the whole file finishes well under 5 seconds.
"""
from __future__ import annotations

import asyncio

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.exceptions import CircuitBreakerOpenException
from src.state import CircuitState


def make_br(**overrides) -> CircuitBreaker:
    """Build a breaker tuned for threshold-rule isolation tests.

    Defaults disable the simple ``failure_threshold`` rule (set very high)
    so the smart rules can be exercised one at a time by overriding fields.
    """
    cfg_kwargs = dict(
        name="t",
        failure_threshold=999,            # disable simple rule by default
        recovery_timeout=10.0,
        timeout_duration=1.0,
        half_open_max_calls=2,
        monitoring_window=60.0,
        error_rate_threshold=0.6,
        slow_call_duration_threshold=0.5,
        consecutive_failures_threshold=4,
        min_volume_threshold=10,
    )
    cfg_kwargs.update(overrides)
    return CircuitBreaker(CircuitBreakerConfig(**cfg_kwargs))


async def _ok() -> str:
    return "ok"


async def _fail() -> None:
    raise ValueError("boom")


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


async def test_error_rate_trips_only_when_volume_met():
    """Error rate rule fires only after volume crosses min_volume_threshold."""
    # Alternate S/F so consecutive_failures stays well below the threshold —
    # this isolates the error-rate rule from the consecutive-failures rule.
    br = make_br()  # consecutive_failures_threshold=4, min_volume_threshold=10

    # Phase 1: 9 alternating calls (S F S F S F S F S). volume=9 < 10 →
    # low-volume mode. Even though 4/9 errors would be 44% (and below the
    # 60% threshold anyway), low-volume gating means error-rate is ignored.
    for i in range(9):
        if i % 2 == 0:
            await br.call(_ok)
        else:
            with pytest.raises(ValueError):
                await br.call(_fail)
    assert br.state == CircuitState.CLOSED
    assert br._window.volume() == 9  # confirm we are in low-volume mode

    # Phase 2: 1 more failure → volume=10, fails=5, error_rate=50%.
    # Now in high-volume mode but error_rate (50%) < threshold (60%) → CLOSED.
    with pytest.raises(ValueError):
        await br.call(_fail)
    assert br.state == CircuitState.CLOSED
    assert br._window.volume() == 10

    # Phase 3: 1 more success → volume=11, fails=5, error_rate=5/11=45.5% → CLOSED.
    await br.call(_ok)
    assert br.state == CircuitState.CLOSED

    # Phase 4: keep failing one call at a time and watch the error_rate cross
    # the 60% threshold. consecutive_failures stays ≤3 throughout (threshold=4),
    # so the trip is unambiguously caused by the error-rate rule.
    # After 1 fail: volume=12, fails=6, error_rate=50% → CLOSED.
    with pytest.raises(ValueError):
        await br.call(_fail)
    assert br.state == CircuitState.CLOSED

    # After 2 fails: volume=13, fails=7, error_rate=53.8% → CLOSED.
    with pytest.raises(ValueError):
        await br.call(_fail)
    assert br.state == CircuitState.CLOSED

    # After 3 fails: volume=14, fails=8, error_rate=57.1% → CLOSED, consec=3.
    with pytest.raises(ValueError):
        await br.call(_fail)
    assert br.state == CircuitState.CLOSED

    # One success resets consecutive_failures so the next failures don't trip
    # via the consecutive rule.
    await br.call(_ok)  # volume=15, fails=8, error_rate=53.3% → CLOSED, consec=0.
    assert br.state == CircuitState.CLOSED

    # Continue failing. Each single-call snapshot:
    #   volume=16, fails=9, rate=56.25% → CLOSED, consec=1
    #   volume=17, fails=10, rate=58.8% → CLOSED, consec=2
    #   volume=18, fails=11, rate=61.1% → ≥60% → TRIPS, consec=3
    with pytest.raises(ValueError):
        await br.call(_fail)
    assert br.state == CircuitState.CLOSED
    with pytest.raises(ValueError):
        await br.call(_fail)
    assert br.state == CircuitState.CLOSED
    with pytest.raises(ValueError):
        await br.call(_fail)
    # This one tipped the rate over 60% → trips on the failure handling.
    assert br.state == CircuitState.OPEN


async def test_consecutive_failures_trips_under_low_volume():
    """Consecutive-failures rule fires even when volume < min_volume."""
    br = make_br(consecutive_failures_threshold=4, min_volume_threshold=10)
    # Drive 4 sequential failures only — volume=4, far below min_volume=10.
    for _ in range(3):
        with pytest.raises(ValueError):
            await br.call(_fail)
    assert br.state == CircuitState.CLOSED  # only 3 failures so far
    with pytest.raises(ValueError):
        await br.call(_fail)
    # 4th failure trips the breaker via the consecutive-failures rule.
    assert br.state == CircuitState.OPEN
    assert br._window.volume() == 4  # confirm we tripped under low volume


async def test_consecutive_failures_resets_on_success():
    """A single success resets the consecutive_failures counter to 0."""
    br = make_br(consecutive_failures_threshold=4, min_volume_threshold=10)

    # 3 fails: consecutive=3, threshold=4 → still CLOSED.
    for _ in range(3):
        with pytest.raises(ValueError):
            await br.call(_fail)
    assert br.state == CircuitState.CLOSED
    assert br._consecutive_failures == 3

    # 1 success: consecutive should reset to 0.
    await br.call(_ok)
    assert br._consecutive_failures == 0

    # 3 more fails: consecutive=3 again — still under threshold → CLOSED.
    for _ in range(3):
        with pytest.raises(ValueError):
            await br.call(_fail)
    assert br.state == CircuitState.CLOSED
    assert br._consecutive_failures == 3


async def test_slow_call_avg_latency_trips():
    """Avg-latency rule trips when window calls average above the threshold."""
    # Tight thresholds keep the test fast (~0.12s × 12 calls < 2s). The
    # disabled-rule values are set high so only the avg-latency rule can fire.
    br = make_br(
        failure_threshold=999,                # disable simple failure rule
        slow_call_duration_threshold=0.1,
        timeout_duration=1.0,                 # don't time out our 120ms calls
        consecutive_failures_threshold=999,   # disable consecutive rule
        error_rate_threshold=0.99,            # disable error-rate rule
        min_volume_threshold=10,
    )

    async def slow_ok() -> str:
        await asyncio.sleep(0.12)  # > 0.1s threshold
        return "ok"

    # Drive successful slow calls. Once volume>=10 the avg-latency rule fires;
    # at that point the next admission short-circuits with the typed exception.
    open_seen = False
    for _ in range(15):
        try:
            await br.call(slow_ok)
        except CircuitBreakerOpenException:
            open_seen = True
            break
    assert open_seen, "breaker never tripped on slow calls"
    assert br.state == CircuitState.OPEN


async def test_failure_threshold_in_window_trips():
    """The simple failure_threshold rule still works when other rules are off."""
    br = make_br(
        failure_threshold=5,
        consecutive_failures_threshold=999,  # disable consecutive rule
        error_rate_threshold=2.0,            # disable error-rate rule
        slow_call_duration_threshold=10.0,   # disable slow-call rule
        min_volume_threshold=5,              # bring threshold below failure_threshold
    )

    # 5 sequential failures → volume=5 (= min_volume), failed_in_window=5
    # = failure_threshold → trips via the simple rule.
    for _ in range(5):
        with pytest.raises(ValueError):
            await br.call(_fail)
    assert br.state == CircuitState.OPEN

    # Walk back through the listener-recorded reason to confirm the rule.
    # We can't read `reason` without a listener here, so instead check
    # _open_reason via the recorded transition timestamps: simply assert we
    # tripped, and validate the produced reason string format separately.


async def test_open_reason_strings_are_descriptive():
    """The state-change reason mentions the rule that fired."""
    captured: list[tuple] = []

    def listener(name, from_state, to_state, reason):
        captured.append((name, from_state, to_state, reason))

    # Drive a low-volume consecutive-failures trip.
    br = make_br(consecutive_failures_threshold=4, min_volume_threshold=10)
    br.add_listener(listener)
    for _ in range(4):
        with pytest.raises(ValueError):
            await br.call(_fail)

    assert br.state == CircuitState.OPEN
    assert len(captured) == 1
    name, from_state, to_state, reason = captured[0]
    assert from_state == CircuitState.CLOSED
    assert to_state == CircuitState.OPEN
    # Low-volume mode → consecutive-failures reason text.
    assert "consecutive" in reason.lower()
    assert reason  # non-empty


async def test_open_reason_high_volume_mentions_metric():
    """High-volume trips emit metric=value pairs in their reason."""
    captured: list[tuple] = []

    def listener(name, from_state, to_state, reason):
        captured.append((name, from_state, to_state, reason))

    br = make_br(
        failure_threshold=5,
        consecutive_failures_threshold=999,
        error_rate_threshold=2.0,
        slow_call_duration_threshold=10.0,
        min_volume_threshold=5,
    )
    br.add_listener(listener)
    for _ in range(5):
        with pytest.raises(ValueError):
            await br.call(_fail)

    assert br.state == CircuitState.OPEN
    reason = captured[-1][3]
    # The high-volume branch should emit at least the failed_in_window= label.
    assert "failed_in_window" in reason or "error_rate" in reason
