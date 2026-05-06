"""Failure injection helper for service-level testing.

Services call ``await self.injector.maybe_fail()`` at the top of their real
work function. The injector's toggles let tests/demos drive synthetic
failures without polluting circuit-breaker logic.
"""
from __future__ import annotations
import asyncio
import random
from typing import Optional


class InjectedFailure(ConnectionError):
    """Raised by FailureInjector to simulate a downstream-service failure."""


class FailureInjector:
    """Tunable failure-injection helper for testing service resilience.

    Three toggles compose:
      - ``failure_rate`` — probability ``[0,1]`` that a call raises.
      - ``is_down`` — when True every call raises immediately.
      - ``response_delay`` — seconds to ``asyncio.sleep`` before any decision.

    Tests can inject a deterministic ``random.Random`` via ``rng=`` to remove
    flakiness from probabilistic toggles.
    """

    def __init__(self, *, rng: Optional[random.Random] = None) -> None:
        self.failure_rate: float = 0.0
        self.is_down: bool = False
        self.response_delay: float = 0.0
        self._rng: random.Random = rng if rng is not None else random.Random()

    # Configuration setters --------------------------------------------------
    def set_failure_rate(self, rate: float) -> None:
        if not 0.0 <= rate <= 1.0:
            raise ValueError("failure_rate must be in [0, 1]")
        self.failure_rate = rate

    def set_down(self, down: bool) -> None:
        self.is_down = bool(down)

    def set_response_delay(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("response_delay must be >= 0")
        self.response_delay = float(seconds)

    def reset(self) -> None:
        self.failure_rate = 0.0
        self.is_down = False
        self.response_delay = 0.0

    # Hot path ---------------------------------------------------------------
    async def maybe_fail(self) -> None:
        """Sleep ``response_delay``; then raise InjectedFailure if downed/sampled."""
        if self.response_delay > 0.0:
            await asyncio.sleep(self.response_delay)
        if self.is_down:
            raise InjectedFailure("service is down")
        if self.failure_rate > 0.0 and self._rng.random() < self.failure_rate:
            raise InjectedFailure("injected failure")

    def snapshot(self) -> dict:
        return {
            "failure_rate": self.failure_rate,
            "is_down": self.is_down,
            "response_delay": self.response_delay,
        }
