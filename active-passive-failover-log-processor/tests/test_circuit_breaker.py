"""Tests for src/circuit_breaker.py — the DIY async circuit breaker.

We monkeypatch ``src.circuit_breaker.time.monotonic`` to fast-forward
the cooldown clock so transitions can be exercised without ``asyncio.sleep``.
The breaker uses ``time.monotonic`` only on the OPEN -> HALF_OPEN
admission path; freezing it on construction and bumping it deterministically
lets every test run in a few microseconds.
"""

from __future__ import annotations

import asyncio

import pytest

from src import circuit_breaker as cb_module
from src.circuit_breaker import (
    CLOSED,
    HALF_OPEN,
    OPEN,
    CircuitBreaker,
    CircuitBreakerOpen,
)


# =========================================================================
# Helpers — controllable async functions for the breaker to call.
# =========================================================================


class _Spy:
    """Awaitable counter that records call counts for the breaker tests.

    Each instance behaves like an async function: ``await spy()`` runs
    the body, returns the configured value (or raises). ``spy.calls``
    holds the total number of invocations, so tests can assert that
    the breaker actually skipped a call when expected.
    """

    def __init__(self, *, returns: object = None, raises: BaseException | None = None) -> None:
        self.returns: object = returns
        self.raises: BaseException | None = raises
        self.calls: int = 0

    async def __call__(self, *args: object, **kwargs: object) -> object:
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return self.returns


def _freeze_time(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    """Pin ``cb_module.time.monotonic`` to ``value`` for deterministic tests."""
    monkeypatch.setattr(cb_module.time, "monotonic", lambda: value)


# =========================================================================
# Construction defaults
# =========================================================================


def test_construction_defaults_are_sane() -> None:
    breaker = CircuitBreaker(name="test")
    assert breaker.state == CLOSED
    assert breaker.failure_count == 0
    metrics = breaker.metrics
    assert metrics == {
        "calls_total": 0,
        "successes_total": 0,
        "failures_total": 0,
        "opens_total": 0,
    }


def test_construction_accepts_overrides() -> None:
    breaker = CircuitBreaker(name="x", fail_max=3, reset_timeout=1.5)
    assert breaker.fail_max == 3
    assert breaker.reset_timeout == 1.5


# =========================================================================
# CLOSED -> CLOSED on success
# =========================================================================


async def test_success_in_closed_keeps_state_and_resets_failure_count() -> None:
    breaker = CircuitBreaker(name="t")
    spy = _Spy(returns="ok")

    result = await breaker.call(spy)

    assert result == "ok"
    assert breaker.state == CLOSED
    assert breaker.failure_count == 0
    assert breaker.metrics["calls_total"] == 1
    assert breaker.metrics["successes_total"] == 1
    assert breaker.metrics["failures_total"] == 0


async def test_repeated_successes_keep_breaker_closed() -> None:
    breaker = CircuitBreaker(name="t")
    spy = _Spy(returns=42)

    for _ in range(20):
        await breaker.call(spy)

    assert breaker.state == CLOSED
    assert breaker.metrics["successes_total"] == 20
    assert breaker.metrics["opens_total"] == 0


# =========================================================================
# CLOSED -> OPEN after fail_max consecutive failures
# =========================================================================


async def test_breaker_opens_after_fail_max_consecutive_failures() -> None:
    breaker = CircuitBreaker(name="t", fail_max=5)
    spy = _Spy(raises=RuntimeError("boom"))

    for _ in range(5):
        with pytest.raises(RuntimeError):
            await breaker.call(spy)

    assert breaker.state == OPEN
    assert breaker.metrics["opens_total"] == 1
    assert breaker.metrics["failures_total"] == 5
    assert spy.calls == 5  # all 5 actually executed (we counted them)


async def test_breaker_does_not_open_below_fail_max() -> None:
    breaker = CircuitBreaker(name="t", fail_max=5)
    spy = _Spy(raises=RuntimeError("boom"))

    for _ in range(4):
        with pytest.raises(RuntimeError):
            await breaker.call(spy)

    assert breaker.state == CLOSED
    assert breaker.failure_count == 4
    assert breaker.metrics["opens_total"] == 0


# =========================================================================
# OPEN rejects without invoking fn
# =========================================================================


async def test_open_state_rejects_without_invoking_fn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once OPEN, ``call`` must NOT execute ``fn`` until cooldown."""
    _freeze_time(monkeypatch, 100.0)
    breaker = CircuitBreaker(name="t", fail_max=2, reset_timeout=10.0)

    failing = _Spy(raises=RuntimeError("boom"))
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.state == OPEN
    failing_count_at_open = failing.calls

    # Now try a brand-new spy via the open breaker — it must NOT be called.
    inner = _Spy(returns="never")
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(inner)
    assert inner.calls == 0
    # And the failing spy stayed at the same count.
    assert failing.calls == failing_count_at_open


# =========================================================================
# OPEN -> HALF_OPEN after reset_timeout elapsed
# =========================================================================


async def test_open_transitions_to_half_open_after_reset_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    breaker = CircuitBreaker(name="t", fail_max=2, reset_timeout=5.0)

    # Trip the breaker at t=100.
    _freeze_time(monkeypatch, 100.0)
    failing = _Spy(raises=RuntimeError("boom"))
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.state == OPEN

    # Just before cooldown elapses — still OPEN, still rejected.
    _freeze_time(monkeypatch, 104.99)
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_Spy(returns="x"))
    assert breaker.state == OPEN

    # Cooldown elapsed. The next call transitions OPEN -> HALF_OPEN and
    # actually invokes fn.
    _freeze_time(monkeypatch, 105.01)
    succeeding = _Spy(returns="ok")
    result = await breaker.call(succeeding)
    assert result == "ok"
    assert succeeding.calls == 1
    # HALF_OPEN -> CLOSED on success
    assert breaker.state == CLOSED
    assert breaker.failure_count == 0


# =========================================================================
# HALF_OPEN failure -> OPEN with reset clock
# =========================================================================


async def test_half_open_failure_re_opens_and_resets_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    breaker = CircuitBreaker(name="t", fail_max=2, reset_timeout=5.0)

    # Trip at t=100.
    _freeze_time(monkeypatch, 100.0)
    failing = _Spy(raises=RuntimeError("boom"))
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.state == OPEN

    # Cooldown elapses; trial fails -> OPEN, opens_total=2.
    _freeze_time(monkeypatch, 106.0)
    with pytest.raises(RuntimeError):
        await breaker.call(_Spy(raises=RuntimeError("still bad")))
    assert breaker.state == OPEN
    assert breaker.metrics["opens_total"] == 2

    # The reset clock must restart at t=106. At t=110 we're still
    # within reset_timeout (5s) -> rejected.
    _freeze_time(monkeypatch, 110.0)
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_Spy(returns="ignored"))

    # At t=111.1 (>5s after the half-open re-open at t=106) we should
    # transition back to HALF_OPEN.
    _freeze_time(monkeypatch, 111.1)
    succeeding = _Spy(returns="recovered")
    result = await breaker.call(succeeding)
    assert result == "recovered"
    assert breaker.state == CLOSED


# =========================================================================
# Concurrent calls during OPEN
# =========================================================================


async def test_concurrent_calls_during_open_all_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _freeze_time(monkeypatch, 0.0)
    breaker = CircuitBreaker(name="t", fail_max=1, reset_timeout=60.0)

    with pytest.raises(RuntimeError):
        await breaker.call(_Spy(raises=RuntimeError("boom")))
    assert breaker.state == OPEN

    inner = _Spy(returns="x")

    async def attempt() -> bool:
        try:
            await breaker.call(inner)
            return True
        except CircuitBreakerOpen:
            return False

    results = await asyncio.gather(*[attempt() for _ in range(10)])
    assert all(r is False for r in results)
    assert inner.calls == 0


# =========================================================================
# Mixing successes and failures in CLOSED resets the counter
# =========================================================================


async def test_success_in_closed_resets_failure_count() -> None:
    breaker = CircuitBreaker(name="t", fail_max=5)

    failing = _Spy(raises=RuntimeError("boom"))
    succeeding = _Spy(returns="ok")

    # 4 failures, 1 success, 4 failures -> 8 failures total but never 5
    # in a row, so the breaker stays CLOSED.
    for _ in range(4):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.failure_count == 4

    await breaker.call(succeeding)
    assert breaker.failure_count == 0
    assert breaker.state == CLOSED

    for _ in range(4):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.failure_count == 4
    assert breaker.state == CLOSED


# =========================================================================
# Function arguments are forwarded
# =========================================================================


async def test_call_forwards_args_and_kwargs() -> None:
    breaker = CircuitBreaker(name="t")
    received: list[tuple[tuple, dict]] = []

    async def fn(*args: object, **kwargs: object) -> str:
        received.append((args, kwargs))
        return "done"

    result = await breaker.call(fn, 1, 2, key="value")
    assert result == "done"
    assert received == [((1, 2), {"key": "value"})]


# =========================================================================
# Metrics counters increment in lockstep with state transitions
# =========================================================================


async def test_metrics_track_calls_successes_failures_and_opens() -> None:
    breaker = CircuitBreaker(name="t", fail_max=2)

    await breaker.call(_Spy(returns="a"))
    await breaker.call(_Spy(returns="b"))
    with pytest.raises(RuntimeError):
        await breaker.call(_Spy(raises=RuntimeError("x")))
    with pytest.raises(RuntimeError):
        await breaker.call(_Spy(raises=RuntimeError("y")))

    metrics = breaker.metrics
    assert metrics["calls_total"] == 4
    assert metrics["successes_total"] == 2
    assert metrics["failures_total"] == 2
    assert metrics["opens_total"] == 1
