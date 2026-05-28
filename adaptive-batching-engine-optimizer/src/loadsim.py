"""Synthetic load generator for the adaptive batching engine.

:class:`LoadSimulator` produces *counts* of messages that "arrived" during a time
interval. It performs no real I/O, opens no sockets, and never sleeps — it simply
multiplies a target rate by an interval and occasionally injects a burst. This
keeps the control loop fully deterministic under test (inject a seeded RNG) while
still letting it react to traffic spikes.

Burst behaviour (bonus: burst traffic handling)
------------------------------------------------
Each call to :meth:`messages_for_interval` independently rolls against
``burst_probability``. On a hit, the effective rate for *that interval only* is
multiplied by ``burst_multiplier`` — e.g. a steady 100 msg/s briefly jumps to
1000 msg/s and then drops back to 100. This models the bursty arrival pattern of
real log pipelines and gives the optimizer something to absorb. The most recent
effective rate (burst-aware) is available via :meth:`current_rate`.
"""

from __future__ import annotations

import random

from src.settings import get_settings


class LoadSimulator:
    """Generates synthetic message arrivals with optional traffic bursts.

    Args:
        messages_per_second: Steady-state target rate. Defaults to
            ``settings.default_messages_per_second``.
        burst_probability: Per-interval chance ``[0, 1]`` of a burst. Defaults to
            ``settings.default_burst_probability``.
        burst_multiplier: Factor applied to the rate during a burst interval.
        rng: Injectable :class:`random.Random` for deterministic bursts. Defaults
            to a fresh, unseeded ``random.Random()``.
    """

    def __init__(
        self,
        messages_per_second: float | None = None,
        burst_probability: float | None = None,
        burst_multiplier: float = 10.0,
        rng: random.Random | None = None,
    ) -> None:
        settings = get_settings()
        self.messages_per_second: float = (
            messages_per_second
            if messages_per_second is not None
            else settings.default_messages_per_second
        )
        self.burst_probability: float = (
            burst_probability
            if burst_probability is not None
            else settings.default_burst_probability
        )
        self.burst_multiplier = burst_multiplier
        self._rng = rng if rng is not None else random.Random()
        # Effective rate of the most recent interval (burst-aware). Seeded with
        # the steady rate so current_rate() is meaningful before the first call.
        self._last_effective_rate: float = self.messages_per_second

    def set_rate(
        self, messages_per_second: float, burst_probability: float | None = None
    ) -> None:
        """Update the steady-state target rate (and optionally burst probability).

        Used by the ``/api/load`` endpoint to retarget live traffic. A negative
        rate is clamped to ``0``; ``burst_probability``, when given, is clamped to
        ``[0, 1]``.
        """
        self.messages_per_second = max(0.0, messages_per_second)
        if burst_probability is not None:
            self.burst_probability = min(1.0, max(0.0, burst_probability))

    def messages_for_interval(self, interval_seconds: float) -> int:
        """Return how many synthetic messages arrived during ``interval_seconds``.

        The base count is ``round(messages_per_second * interval_seconds)``. With
        probability ``burst_probability`` the rate is multiplied by
        ``burst_multiplier`` for this interval only. The effective rate is recorded
        for :meth:`current_rate`. The return value is always an ``int >= 0``.
        """
        if interval_seconds <= 0.0:
            self._last_effective_rate = self.messages_per_second
            return 0

        rate = self.messages_per_second
        if self._rng.random() < self.burst_probability:
            rate *= self.burst_multiplier
        self._last_effective_rate = rate

        return max(0, round(rate * interval_seconds))

    def current_rate(self) -> float:
        """Return the effective rate (records/sec) of the most recent interval.

        Reflects whether the last :meth:`messages_for_interval` call was a burst
        (``messages_per_second * burst_multiplier``) or steady-state.
        """
        return self._last_effective_rate
