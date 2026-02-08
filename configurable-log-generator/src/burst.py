"""Burst mode controller — probabilistic rate spikes."""

import time
import random
import logging

logger = logging.getLogger(__name__)


class BurstController:
    def __init__(self, frequency: float, multiplier: int, duration: int, enabled: bool):
        self._frequency = frequency
        self._multiplier = multiplier
        self._duration = duration
        self._enabled = enabled
        self._burst_active = False
        self._burst_end_time = 0.0
        self._current_multiplier = 1
        self._last_check_time = time.time()

    def get_current_multiplier(self) -> int:
        """Check burst state once per call. Returns rate multiplier (1 = normal)."""
        if not self._enabled:
            return 1

        now = time.time()

        if self._burst_active:
            if now >= self._burst_end_time:
                self._burst_active = False
                logger.info("Burst ended, returning to normal rate")
                return 1
            return self._current_multiplier

        # Roll for new burst (at most once per second)
        if now - self._last_check_time >= 1.0:
            self._last_check_time = now
            if random.random() < self._frequency:
                self._burst_active = True
                self._burst_end_time = now + self._duration
                # Randomize between multiplier and multiplier*2 for 5-10x range
                self._current_multiplier = random.randint(
                    self._multiplier, self._multiplier * 2
                )
                logger.info(
                    "BURST triggered: %dx rate for %d seconds",
                    self._current_multiplier,
                    self._duration,
                )
                return self._current_multiplier

        return 1
