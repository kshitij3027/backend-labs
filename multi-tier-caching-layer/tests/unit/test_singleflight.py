"""Unit tests for in-process single-flight coalescing and early refresh.

``asyncio_mode = auto`` (see pytest.ini) means async test functions need no
decorator. These tests exercise:

* request coalescing for the same key (factory runs once for a herd),
* independence of distinct keys,
* re-running the factory after a prior call completes (key is cleared),
* exception propagation to every waiter, with recovery on the next call, and
* the probabilistic :func:`should_early_refresh` contract incl. monotonicity.
"""
from __future__ import annotations

import asyncio

import pytest

from src.singleflight import SingleFlight, should_early_refresh


# --------------------------------------------------------------------------- #
# SingleFlight.do — coalescing                                                #
# --------------------------------------------------------------------------- #
async def test_concurrent_same_key_coalesces_to_single_factory_call() -> None:
    """50 concurrent do(\"k\", ...) calls run the factory exactly once."""
    counter = 0

    async def factory() -> int:
        nonlocal counter
        counter += 1
        # Hold the in-flight window open so all 50 callers pile onto it.
        await asyncio.sleep(0.05)
        return counter

    sf = SingleFlight()
    results = await asyncio.gather(*(sf.do("k", factory) for _ in range(50)))

    assert counter == 1
    assert len(results) == 50
    assert all(r == 1 for r in results)
    # Key is cleared after completion.
    assert sf._inflight == {}


async def test_independent_keys_each_run_their_own_factory() -> None:
    """Concurrent calls for different keys do not coalesce together."""
    counter = 0

    async def factory() -> int:
        nonlocal counter
        counter += 1
        # Capture this call's own increment BEFORE yielding; otherwise both
        # concurrent calls would read the shared counter after both increments
        # have landed and return the same final value.
        mine = counter
        await asyncio.sleep(0.05)
        return mine

    sf = SingleFlight()
    res_a, res_b = await asyncio.gather(
        sf.do("a", factory),
        sf.do("b", factory),
    )

    assert counter == 2
    # Two distinct computations ran; results are the two increment values.
    assert {res_a, res_b} == {1, 2}
    assert sf._inflight == {}


async def test_factory_reruns_after_previous_call_completes() -> None:
    """The key is cleared on completion, so a later call re-runs the factory."""
    counter = 0

    async def factory() -> int:
        nonlocal counter
        counter += 1
        await asyncio.sleep(0)
        return counter

    sf = SingleFlight()
    first = await sf.do("k", factory)
    # No longer in flight after awaiting.
    assert sf._inflight == {}
    second = await sf.do("k", factory)

    assert counter == 2
    assert first == 1
    assert second == 2


# --------------------------------------------------------------------------- #
# SingleFlight.do — exception handling                                        #
# --------------------------------------------------------------------------- #
class _Boom(Exception):
    """Sentinel error raised by a failing factory."""


async def test_exception_propagates_to_all_waiters_then_recovers() -> None:
    """All concurrent waiters see the failure; key clears so the next call ok."""
    calls = 0

    async def bad_factory() -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        raise _Boom("factory failed")

    sf = SingleFlight()

    # Gather with return_exceptions so we can inspect every waiter's outcome.
    outcomes = await asyncio.gather(
        *(sf.do("k", bad_factory) for _ in range(10)),
        return_exceptions=True,
    )

    # Factory ran once despite 10 waiters; every waiter received the exception.
    assert calls == 1
    assert len(outcomes) == 10
    assert all(isinstance(o, _Boom) for o in outcomes)
    # Key was cleared even though the factory raised.
    assert sf._inflight == {}

    async def good_factory() -> str:
        return "ok"

    # A subsequent call for the same key succeeds (no stale in-flight future).
    assert await sf.do("k", good_factory) == "ok"


async def test_single_waiter_exception_is_raised_directly() -> None:
    """A lone caller of a failing factory simply sees the raised exception."""
    sf = SingleFlight()

    async def bad_factory() -> int:
        raise _Boom("nope")

    with pytest.raises(_Boom):
        await sf.do("solo", bad_factory)
    assert sf._inflight == {}


# --------------------------------------------------------------------------- #
# should_early_refresh                                                        #
# --------------------------------------------------------------------------- #
def test_early_refresh_true_when_remaining_non_positive() -> None:
    """A non-positive remaining (expired) always refreshes."""
    assert should_early_refresh(0.0, 100.0) is True
    assert should_early_refresh(-5.0, 100.0) is True


def test_early_refresh_true_when_total_non_positive() -> None:
    """A non-positive total TTL is treated as invalid -> refresh."""
    assert should_early_refresh(10.0, 0.0) is True
    assert should_early_refresh(10.0, -1.0) is True


def test_early_refresh_true_with_zero_rng_and_partial_remaining() -> None:
    """rng()==0.0 with any partial remaining (prob > 0) refreshes."""
    # remaining=50/100 -> p = 1*(1-0.5) = 0.5; 0.0 < 0.5 -> True.
    assert should_early_refresh(50.0, 100.0, rng=lambda: 0.0) is True


def test_early_refresh_false_with_high_rng_and_near_full_remaining() -> None:
    """rng() near 1 with a near-full entry (tiny prob) does not refresh."""
    # remaining = 99% of total -> p = 1*(1-0.99) = 0.01; 0.999999 < 0.01 -> False.
    total = 100.0
    remaining = total * 0.99
    assert should_early_refresh(remaining, total, rng=lambda: 0.999999) is False


def test_early_refresh_is_monotonic_in_remaining() -> None:
    """For a fixed rng sample, less remaining is at least as likely to refresh.

    With beta=1 and rng()=0.5:
      remaining=0.1*total -> p=0.9 -> 0.5 < 0.9 -> True (refresh-prone)
      remaining=0.9*total -> p=0.1 -> 0.5 < 0.1 -> False
    """
    total = 100.0
    fixed_rng = lambda: 0.5  # noqa: E731 - tiny deterministic stub

    near_expiry = should_early_refresh(0.1 * total, total, rng=fixed_rng)
    nearly_fresh = should_early_refresh(0.9 * total, total, rng=fixed_rng)

    assert near_expiry is True
    assert nearly_fresh is False
    # Monotonic ordering: True (1) >= False (0).
    assert int(near_expiry) >= int(nearly_fresh)


def test_early_refresh_beta_shifts_refresh_earlier() -> None:
    """A larger beta raises the refresh probability for the same remaining."""
    total = 100.0
    remaining = 80.0  # x=0.8
    # beta=1 -> p=0.2; beta=3 -> p=0.6 (clamped within [0,1]).
    # With rng()=0.4: beta=1 -> 0.4<0.2 False; beta=3 -> 0.4<0.6 True.
    rng = lambda: 0.4  # noqa: E731
    assert should_early_refresh(remaining, total, beta=1.0, rng=rng) is False
    assert should_early_refresh(remaining, total, beta=3.0, rng=rng) is True
