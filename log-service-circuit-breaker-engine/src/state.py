"""Circuit breaker states and state-change records."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CircuitState(str, Enum):
    """Three-state circuit breaker state machine."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass(frozen=True)
class StateChange:
    """An immutable record of a single state transition."""

    breaker_name: str
    from_state: CircuitState
    to_state: CircuitState
    timestamp: float
    reason: str
