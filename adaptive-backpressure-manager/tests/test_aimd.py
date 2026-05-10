import pytest

from src.aimd import AIMDLimiter


def test_multiplicative_decrease_beta_07():
    aimd = AIMDLimiter(initial_limit=100, beta=0.7)
    aimd.on_overload()
    assert aimd.limit == 70
    aimd.on_overload()
    assert aimd.limit == 49


def test_additive_increase_every_three_ticks():
    aimd = AIMDLimiter(initial_limit=10, beta=0.7, additive=1, ai_period_ticks=3)
    aimd.on_overload()
    assert aimd.limit == 7
    aimd.on_tick(); aimd.on_tick()
    assert aimd.limit == 7
    aimd.on_tick()
    assert aimd.limit == 8


def test_recovery_slow_start_halves_prev_limit():
    aimd = AIMDLimiter(initial_limit=100, beta=0.7)
    aimd.on_recovery_entry(prev_limit=80)
    assert aimd.limit == 40


def test_retry_after_jitter_within_band():
    rng_values = iter([0.0, 0.5, 1.0])
    aimd = AIMDLimiter(initial_limit=10, jitter=0.3, rng=lambda: next(rng_values))
    assert aimd.retry_after(1.0) == pytest.approx(0.7)
    assert aimd.retry_after(1.0) == pytest.approx(1.0)
    assert aimd.retry_after(1.0) == pytest.approx(1.3)


def test_try_acquire_respects_limit():
    aimd = AIMDLimiter(initial_limit=3)
    assert aimd.try_acquire() is True
    assert aimd.try_acquire() is True
    assert aimd.try_acquire() is True
    assert aimd.try_acquire() is False
    aimd.release()
    assert aimd.try_acquire() is True


def test_throttle_rate_reflects_limit():
    aimd = AIMDLimiter(initial_limit=100, beta=0.5)
    assert aimd.throttle_rate == pytest.approx(1.0)
    aimd.on_overload()
    assert aimd.throttle_rate == pytest.approx(0.5)
