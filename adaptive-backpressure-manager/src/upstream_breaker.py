from typing import Optional

from src.config import Settings
from src.logging_setup import TAG_PRESSURE, get_logger


class UpstreamBreaker:
    """Fused-with-admission upstream breaker: trips on sustained emergency score."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consecutive_emergency = 0
        self._open = False
        self._log = get_logger("upstream_breaker")

    @property
    def is_open(self) -> bool:
        return self._open

    @property
    def consecutive_emergency(self) -> int:
        return self._consecutive_emergency

    def observe(self, score: float) -> None:
        s = self._settings
        if score >= s.up_overload_to_emergency:
            self._consecutive_emergency += 1
            if not self._open and self._consecutive_emergency >= 3:
                self._open = True
                self._log.info(
                    "upstream_breaker_open",
                    tag=TAG_PRESSURE,
                    score=score,
                )
        else:
            self._consecutive_emergency = 0
            if self._open and score < s.up_pressure_to_overload:
                self._open = False
                self._log.info(
                    "upstream_breaker_closed",
                    tag=TAG_PRESSURE,
                    score=score,
                )
