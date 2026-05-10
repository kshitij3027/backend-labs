import random
from typing import Callable

from src.logging_setup import TAG_THROTTLE, get_logger


class AIMDLimiter:
    """Additive-increase/multiplicative-decrease capacity limiter."""

    def __init__(
        self,
        initial_limit: int,
        beta: float = 0.7,
        additive: int = 1,
        ai_period_ticks: int = 3,
        jitter: float = 0.3,
        rng: Callable[[], float] = random.random,
    ) -> None:
        if initial_limit <= 0:
            raise ValueError("initial_limit must be > 0")
        self._initial_limit = initial_limit
        self._limit = initial_limit
        self._beta = beta
        self._additive = additive
        self._ai_period_ticks = ai_period_ticks
        self._jitter = jitter
        self._rng = rng
        self._inflight = 0
        self._ticks_since_increase = 0
        self._log = get_logger("aimd")

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def throttle_rate(self) -> float:
        """Fraction of admission still permitted relative to initial_limit."""
        if self._initial_limit <= 0:
            return 0.0
        return self._limit / self._initial_limit

    def try_acquire(self) -> bool:
        if self._inflight >= self._limit:
            return False
        self._inflight += 1
        return True

    def release(self) -> None:
        if self._inflight > 0:
            self._inflight -= 1

    def on_overload(self) -> None:
        new = max(1, int(self._limit * self._beta))
        if new != self._limit:
            self._log.info(
                "aimd_multiplicative_decrease",
                tag=TAG_THROTTLE,
                old_limit=self._limit,
                new_limit=new,
                beta=self._beta,
            )
        self._limit = new
        self._ticks_since_increase = 0

    def on_tick(self) -> None:
        if self._limit >= self._initial_limit:
            self._ticks_since_increase = 0
            return
        self._ticks_since_increase += 1
        if self._ticks_since_increase >= self._ai_period_ticks:
            self._limit = min(self._initial_limit, self._limit + self._additive)
            self._ticks_since_increase = 0

    def on_recovery_entry(self, prev_limit: int) -> None:
        """Slow-start clamp when entering RECOVERY: limit = max(1, prev_limit // 2)."""
        clamped = max(1, prev_limit // 2)
        self._log.info(
            "aimd_recovery_slow_start",
            tag=TAG_THROTTLE,
            prev_limit=prev_limit,
            new_limit=clamped,
        )
        self._limit = clamped
        self._ticks_since_increase = 0

    def retry_after(self, base_s: float) -> float:
        """Jittered Retry-After. Returns base_s * uniform(1 - jitter, 1 + jitter)."""
        low = 1.0 - self._jitter
        high = 1.0 + self._jitter
        return base_s * (low + (high - low) * self._rng())
