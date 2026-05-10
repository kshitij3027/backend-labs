import time
from enum import Enum
from typing import Any, Awaitable, Callable

from src.logging_setup import TAG_PRESSURE, get_logger


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when CircuitBreaker is OPEN and the call is refused."""


class CircuitBreaker:
    """3-state circuit breaker for downstream-dependency calls."""

    def __init__(
        self,
        name: str = "downstream",
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_probes: int = 2,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_probes = half_open_probes
        self._clock = clock
        self._state: BreakerState = BreakerState.CLOSED
        self._failures = 0
        self._probes = 0
        self._opened_at: float = 0.0
        self._log = get_logger("breaker")

    @property
    def state(self) -> BreakerState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failures

    async def call(self, fn: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        self._maybe_half_open()
        if self._state == BreakerState.OPEN:
            raise CircuitOpenError(f"breaker {self._name} is OPEN")
        try:
            result = await fn(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        else:
            self._on_success()
            return result

    def _maybe_half_open(self) -> None:
        if self._state != BreakerState.OPEN:
            return
        if self._clock() - self._opened_at >= self._recovery_timeout:
            self._transition(BreakerState.HALF_OPEN)
            self._probes = 0

    def _on_failure(self) -> None:
        if self._state == BreakerState.HALF_OPEN:
            self._transition(BreakerState.OPEN)
            self._opened_at = self._clock()
            self._failures = self._failure_threshold
            self._probes = 0
            return
        self._failures += 1
        if self._failures >= self._failure_threshold:
            self._transition(BreakerState.OPEN)
            self._opened_at = self._clock()

    def _on_success(self) -> None:
        if self._state == BreakerState.HALF_OPEN:
            self._probes += 1
            if self._probes >= self._half_open_probes:
                self._transition(BreakerState.CLOSED)
                self._failures = 0
                self._probes = 0
            return
        self._failures = 0

    def _transition(self, target: BreakerState) -> None:
        prev = self._state
        self._state = target
        self._log.info(
            "breaker_transition",
            tag=TAG_PRESSURE,
            name=self._name,
            from_state=prev.value,
            to_state=target.value,
        )
