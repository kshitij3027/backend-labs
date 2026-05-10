import asyncio

import pytest

from src.breaker import BreakerState, CircuitBreaker, CircuitOpenError


class _FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


async def _ok():
    return "ok"


async def _boom():
    raise RuntimeError("downstream exploded")


@pytest.mark.asyncio
async def test_opens_after_five_consecutive_failures():
    clock = _FakeClock()
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0, clock=clock)
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await cb.call(_boom)
    assert cb.state == BreakerState.OPEN


@pytest.mark.asyncio
async def test_open_refuses_call_before_timeout():
    clock = _FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0, clock=clock)
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    assert cb.state == BreakerState.OPEN
    clock.t = 10.0
    with pytest.raises(CircuitOpenError):
        await cb.call(_ok)


@pytest.mark.asyncio
async def test_half_open_then_closed_on_probe_success():
    clock = _FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0, half_open_probes=2, clock=clock)
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    assert cb.state == BreakerState.OPEN
    clock.t = 35.0
    assert await cb.call(_ok) == "ok"
    assert cb.state == BreakerState.HALF_OPEN
    assert await cb.call(_ok) == "ok"
    assert cb.state == BreakerState.CLOSED


@pytest.mark.asyncio
async def test_half_open_reopens_on_probe_failure():
    clock = _FakeClock()
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0, half_open_probes=2, clock=clock)
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    clock.t = 35.0
    with pytest.raises(RuntimeError):
        await cb.call(_boom)
    assert cb.state == BreakerState.OPEN
