import time
from typing import Callable

from src.config import Settings
from src.logging_setup import TAG_STATE, get_logger
from src.state import PressureLevel


class BackpressureManager:
    """Hysteresis + min-dwell state machine driving pressure level."""

    def __init__(self, settings: Settings, clock: Callable[[], float] = time.monotonic) -> None:
        self._settings = settings
        self._clock = clock
        self._level = PressureLevel.NORMAL
        self._entered_at = clock()
        self._log = get_logger("state_machine")
        self._last_score: float = 0.0

    @property
    def level(self) -> PressureLevel:
        return self._level

    @property
    def last_score(self) -> float:
        return self._last_score

    @property
    def entered_at(self) -> float:
        return self._entered_at

    def dwell(self) -> float:
        return self._clock() - self._entered_at

    def tick(self, score: float) -> PressureLevel:
        self._last_score = score
        target = self._target_level(score)
        if target == self._level:
            return self._level
        if self.dwell() < self._settings.min_dwell_seconds:
            return self._level
        self._transition(target)
        return self._level

    def _target_level(self, score: float) -> PressureLevel:
        s = self._settings
        cur = self._level

        if cur == PressureLevel.NORMAL:
            if score >= s.up_pressure_to_overload:
                return PressureLevel.OVERLOAD
            if score >= s.up_normal_to_pressure:
                return PressureLevel.PRESSURE
            return PressureLevel.NORMAL

        if cur == PressureLevel.PRESSURE:
            if score >= s.up_pressure_to_overload:
                return PressureLevel.OVERLOAD
            if score < s.down_pressure_to_normal:
                return PressureLevel.NORMAL
            return PressureLevel.PRESSURE

        if cur == PressureLevel.OVERLOAD:
            if score < s.down_overload_to_pressure:
                return PressureLevel.RECOVERY
            return PressureLevel.OVERLOAD

        if score >= s.up_pressure_to_overload:
            return PressureLevel.OVERLOAD
        if score < s.down_recovery_to_normal:
            return PressureLevel.NORMAL
        return PressureLevel.RECOVERY

    def _transition(self, target: PressureLevel) -> None:
        prev = self._level
        self._level = target
        self._entered_at = self._clock()
        self._log.info(
            "pressure_level_transition",
            tag=TAG_STATE,
            from_state=prev.value,
            to_state=target.value,
            score=self._last_score,
        )
