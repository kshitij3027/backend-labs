"""Tests for FailureInjector."""
import asyncio
import random
import time
import pytest
from src.failure_injection import FailureInjector, InjectedFailure


async def test_default_does_not_fail():
    """Fresh injector with no toggles should never raise."""
    injector = FailureInjector()
    for _ in range(100):
        await injector.maybe_fail()


async def test_is_down_always_raises():
    """When is_down is True, every call raises InjectedFailure."""
    injector = FailureInjector()
    injector.set_down(True)
    for _ in range(20):
        with pytest.raises(InjectedFailure):
            await injector.maybe_fail()


async def test_failure_rate_one_always_raises():
    """failure_rate=1.0 means every call raises."""
    injector = FailureInjector()
    injector.set_failure_rate(1.0)
    for _ in range(50):
        with pytest.raises(InjectedFailure):
            await injector.maybe_fail()


async def test_failure_rate_zero_never_raises():
    """failure_rate=0.0 means no calls raise."""
    injector = FailureInjector()
    injector.set_failure_rate(0.0)
    for _ in range(50):
        await injector.maybe_fail()


async def test_response_delay_blocks():
    """response_delay should cause maybe_fail to await for the configured time."""
    injector = FailureInjector()
    injector.set_response_delay(0.05)
    start = time.monotonic()
    await injector.maybe_fail()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.05


async def test_failure_rate_validates_input():
    """failure_rate must be in [0, 1]."""
    injector = FailureInjector()
    with pytest.raises(ValueError):
        injector.set_failure_rate(-0.1)
    with pytest.raises(ValueError):
        injector.set_failure_rate(1.1)


async def test_response_delay_validates_input():
    """response_delay must be >= 0."""
    injector = FailureInjector()
    with pytest.raises(ValueError):
        injector.set_response_delay(-0.5)


async def test_failure_rate_with_seeded_rng():
    """With a seeded RNG and rate=0.5, failures should be ~500 out of 1000."""
    injector = FailureInjector(rng=random.Random(42))
    injector.set_failure_rate(0.5)
    failures = 0
    for _ in range(1000):
        try:
            await injector.maybe_fail()
        except InjectedFailure:
            failures += 1
    assert 400 <= failures <= 600


async def test_reset_clears_all_toggles():
    """reset() must restore all toggles to their default values."""
    injector = FailureInjector()
    injector.set_failure_rate(0.7)
    injector.set_down(True)
    injector.set_response_delay(0.1)
    injector.reset()
    snap = injector.snapshot()
    assert snap["failure_rate"] == 0.0
    assert snap["is_down"] is False
    assert snap["response_delay"] == 0.0


async def test_snapshot_shape():
    """snapshot() returns a dict with the expected keys."""
    injector = FailureInjector()
    snap = injector.snapshot()
    assert isinstance(snap, dict)
    assert "failure_rate" in snap
    assert "is_down" in snap
    assert "response_delay" in snap


async def test_injected_failure_is_connection_error():
    """InjectedFailure must inherit from ConnectionError so service code can catch it generically."""
    err = InjectedFailure("x")
    assert isinstance(err, ConnectionError)
