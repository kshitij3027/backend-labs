"""Circuit breaker exception types."""
from __future__ import annotations


class CircuitBreakerError(Exception):
    """Base class for all circuit breaker errors."""


class CircuitBreakerOpenException(CircuitBreakerError):
    """Raised when a call is attempted on a breaker in the OPEN state."""

    def __init__(self, breaker_name: str, opened_at: float) -> None:
        self.breaker_name = breaker_name
        self.opened_at = opened_at
        super().__init__(
            f"Circuit breaker '{breaker_name}' is OPEN (opened at {opened_at:.3f})"
        )


class CircuitBreakerTimeoutException(CircuitBreakerError):
    """Raised when a wrapped call exceeds the configured timeout."""

    def __init__(self, breaker_name: str, timeout_seconds: float) -> None:
        self.breaker_name = breaker_name
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"Circuit breaker '{breaker_name}' call timed out after {timeout_seconds}s"
        )
