"""Alert primitives — a Pydantic model and an in-memory ring buffer.

The ring buffer keeps the last N alerts (default 100). When the buffer
is full, the oldest alert is evicted on the next add. Future extensions
might fan out to webhooks or email — the AlertSink interface is the
seam for that.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from typing import Literal, Optional

from pydantic import BaseModel, Field


Severity = Literal["info", "warning", "critical"]
AlertType = Literal["frequency_spike", "off_hours_access", "unknown_actor", "integrity_break"]


class Alert(BaseModel):
    id: str
    type: AlertType
    severity: Severity
    actor: Optional[str] = None
    resource: Optional[str] = None
    message: str
    ts: float  # unix epoch seconds


class AlertSink:
    """Process-wide in-memory ring buffer for the most recent alerts."""

    def __init__(self, capacity: int = 100) -> None:
        self._buf: deque[Alert] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def add(self, *, type: AlertType, severity: Severity, message: str,
            actor: Optional[str] = None, resource: Optional[str] = None) -> Alert:
        alert = Alert(
            id=str(uuid.uuid4()),
            type=type,
            severity=severity,
            actor=actor,
            resource=resource,
            message=message,
            ts=time.time(),
        )
        with self._lock:
            self._buf.append(alert)
        return alert

    def recent(self, limit: int = 100) -> list[Alert]:
        with self._lock:
            items = list(self._buf)
        # Newest first.
        items.reverse()
        return items[:limit]

    def clear(self) -> None:
        with self._lock:
            self._buf.clear()


# Process-wide singleton (initialised by lifespan in main.py).
_SINK: Optional[AlertSink] = None


def set_sink(sink: AlertSink) -> None:
    global _SINK
    _SINK = sink


def get_sink() -> AlertSink:
    global _SINK
    if _SINK is None:
        _SINK = AlertSink()
    return _SINK


def reset_sink_for_tests() -> None:
    global _SINK
    _SINK = None
