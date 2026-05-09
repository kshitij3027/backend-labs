"""State-change alerter — appends transitions to a deque and logs them."""
from __future__ import annotations
import logging
import time
from collections import deque
from typing import Deque

from src.state import CircuitState

logger = logging.getLogger(__name__)


class StateChangeAlerter:
    """Listener that records breaker state transitions."""

    def __init__(self, maxlen: int = 200) -> None:
        self._events: Deque[dict] = deque(maxlen=maxlen)

    def __call__(self, name: str, from_state: CircuitState, to_state: CircuitState, reason: str) -> None:
        event = {
            "ts": time.time(),
            "name": name,
            "from": from_state.value if hasattr(from_state, "value") else str(from_state),
            "to": to_state.value if hasattr(to_state, "value") else str(to_state),
            "reason": reason,
        }
        self._events.append(event)
        logger.warning(
            "circuit '%s' transitioned %s -> %s (%s)",
            event["name"], event["from"], event["to"], event["reason"],
        )

    def events(self) -> list[dict]:
        return list(self._events)

    def clear(self) -> None:
        self._events.clear()
