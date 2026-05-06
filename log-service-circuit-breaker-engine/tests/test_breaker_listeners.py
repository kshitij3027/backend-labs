"""Observer-pattern tests for ``CircuitBreaker.add_listener``.

Listeners must be invoked on every state transition, support both sync and
async callables, run through every registered listener even when one raises,
and observe the full CLOSED → OPEN → HALF_OPEN → CLOSED cycle.
"""
from __future__ import annotations

import asyncio

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.state import CircuitState


def make_br(**overrides) -> CircuitBreaker:
    """Build a breaker tuned so 4 sequential failures trip it under low volume."""
    cfg_kwargs = dict(
        name="t-listen",
        failure_threshold=999,
        recovery_timeout=0.2,
        timeout_duration=1.0,
        half_open_max_calls=2,
        monitoring_window=60.0,
        error_rate_threshold=2.0,
        slow_call_duration_threshold=10.0,
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


async def test_listener_invoked_on_close_to_open():
    """A sync listener captures CLOSED → OPEN with a non-empty reason string."""
    captured: list[tuple] = []

    def listener(name, from_state, to_state, reason):
        captured.append((name, from_state, to_state, reason))

    br = make_br()
    br.add_listener(listener)

    # Drive 4 sequential failures (low-volume mode + consecutive rule trips).
    for _ in range(4):
        with pytest.raises(ValueError):
            await br.call(_fail)

    assert br.state == CircuitState.OPEN
    assert len(captured) == 1
    name, from_state, to_state, reason = captured[0]
    assert name == "t-listen"
    assert from_state == CircuitState.CLOSED
    assert to_state == CircuitState.OPEN
    assert reason  # non-empty reason


async def test_async_listener_supported():
    """An async listener is awaited on transitions."""
    called: dict[str, bool] = {"flag": False, "args_seen": False}

    async def alistener(name, from_state, to_state, reason):
        await asyncio.sleep(0)  # yield once to prove we were truly awaited
        called["flag"] = True
        if from_state == CircuitState.CLOSED and to_state == CircuitState.OPEN:
            called["args_seen"] = True

    br = make_br()
    br.add_listener(alistener)

    for _ in range(4):
        with pytest.raises(ValueError):
            await br.call(_fail)

    assert br.state == CircuitState.OPEN
    assert called["flag"] is True
    assert called["args_seen"] is True


async def test_multiple_listeners_all_invoked():
    """Every registered listener fires on each transition."""
    counts = {"a": 0, "b": 0, "c": 0}

    def la(*args):
        counts["a"] += 1

    def lb(*args):
        counts["b"] += 1

    def lc(*args):
        counts["c"] += 1

    br = make_br()
    br.add_listener(la)
    br.add_listener(lb)
    br.add_listener(lc)

    for _ in range(4):
        with pytest.raises(ValueError):
            await br.call(_fail)

    assert br.state == CircuitState.OPEN
    assert counts == {"a": 1, "b": 1, "c": 1}


async def test_failing_listener_does_not_break_others():
    """A listener that raises does not prevent later listeners from running."""
    recorded: list[tuple] = []

    def bad_listener(*args):
        raise RuntimeError("listener exploded")

    def good_listener(name, from_state, to_state, reason):
        recorded.append((from_state, to_state))

    br = make_br()
    br.add_listener(bad_listener)
    br.add_listener(good_listener)

    for _ in range(4):
        with pytest.raises(ValueError):
            await br.call(_fail)

    assert br.state == CircuitState.OPEN
    # The bad listener didn't break the breaker, and the good one still ran.
    assert recorded == [(CircuitState.CLOSED, CircuitState.OPEN)]


async def test_full_cycle_emits_three_transitions():
    """Listener observes CLOSED→OPEN, OPEN→HALF_OPEN, HALF_OPEN→CLOSED in order."""
    transitions: list[tuple] = []

    def listener(name, from_state, to_state, reason):
        transitions.append((from_state, to_state))

    br = make_br()  # half_open_max_calls=2, recovery_timeout=0.2
    br.add_listener(listener)

    # 1) Trip CLOSED → OPEN.
    for _ in range(4):
        with pytest.raises(ValueError):
            await br.call(_fail)
    assert br.state == CircuitState.OPEN

    # 2) Wait past recovery, send first probe → admitted as HALF_OPEN.
    await asyncio.sleep(0.25)
    await br.call(_ok)
    assert br.state == CircuitState.HALF_OPEN

    # 3) Send second probe → fully closed (half_open_max_calls=2 reached).
    await br.call(_ok)
    assert br.state == CircuitState.CLOSED

    assert transitions == [
        (CircuitState.CLOSED, CircuitState.OPEN),
        (CircuitState.OPEN, CircuitState.HALF_OPEN),
        (CircuitState.HALF_OPEN, CircuitState.CLOSED),
    ]
