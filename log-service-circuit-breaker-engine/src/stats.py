"""Per-breaker statistics + sliding-window call records."""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional

from src.state import CircuitState


@dataclass
class CallRecord:
    """A single recorded call within the sliding window.

    Attributes:
        timestamp: Wall-clock time at which the call completed (seconds).
        success: ``True`` if the call succeeded, ``False`` for any failure.
        latency: Wall-clock duration of the call in seconds.
    """

    timestamp: float
    success: bool
    latency: float


class CallWindow:
    """A bounded sliding window of :class:`CallRecord`\\ s trimmed to ``window_seconds``.

    Operations are O(1) amortized for record + trim because old entries are
    only popped when accessed (lazy trim). Every read method (``volume``,
    ``error_rate``, ``avg_latency``) trims expired entries off the left of
    the deque before computing its result.

    All methods accept an optional ``now`` keyword argument so tests can drive
    the window deterministically without having to monkeypatch ``time.time``.
    """

    def __init__(self, window_seconds: float):
        """Create a new sliding window.

        Args:
            window_seconds: Width of the window in seconds. Must be > 0.

        Raises:
            ValueError: If ``window_seconds`` is not strictly positive.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds: float = window_seconds
        self._records: Deque[CallRecord] = deque()

    def record(self, success: bool, latency: float, *, now: Optional[float] = None) -> None:
        """Append a new call record to the window.

        Args:
            success: Whether the call succeeded.
            latency: Latency of the call, in seconds.
            now: Optional override for the current timestamp; defaults to
                ``time.time()``.
        """
        ts = time.time() if now is None else now
        self._records.append(CallRecord(timestamp=ts, success=success, latency=latency))

    def trim(self, now: Optional[float] = None) -> None:
        """Drop all records older than ``window_seconds`` ago.

        Args:
            now: Optional override for the current timestamp; defaults to
                ``time.time()``.
        """
        cutoff = (time.time() if now is None else now) - self.window_seconds
        while self._records and self._records[0].timestamp < cutoff:
            self._records.popleft()

    def volume(self, now: Optional[float] = None) -> int:
        """Return the count of call records currently inside the window."""
        self.trim(now)
        return len(self._records)

    def error_rate(self, now: Optional[float] = None) -> float:
        """Return the fraction of failures over the current window.

        An empty window reports 0.0 (no observed errors yet).
        """
        self.trim(now)
        if not self._records:
            return 0.0
        failures = sum(1 for r in self._records if not r.success)
        return failures / len(self._records)

    def avg_latency(self, now: Optional[float] = None) -> float:
        """Return the average latency over the current window.

        An empty window reports 0.0.
        """
        self.trim(now)
        if not self._records:
            return 0.0
        return sum(r.latency for r in self._records) / len(self._records)

    def clear(self) -> None:
        """Drop every record currently in the window."""
        self._records.clear()

    def __len__(self) -> int:
        """Return the raw record count (without trimming)."""
        return len(self._records)


@dataclass
class CircuitStats:
    """Cumulative + state-tracking stats for one breaker.

    Attributes:
        total_calls: Total calls observed (success + failure + timeout).
        successful_calls: Calls that returned without raising.
        failed_calls: Calls that raised a non-timeout exception.
        timeout_calls: Calls that exceeded the configured timeout.
        state_changes: Total number of state transitions executed.
        last_failure_time: Wall-clock time of the most recent failure, if any.
        last_success_time: Wall-clock time of the most recent success, if any.
        current_state: Current :class:`CircuitState` of the breaker.
        opened_at: Wall-clock time of the most recent OPEN transition, if any.
        cumulative_open_duration: Total seconds spent in the OPEN state.
    """

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    timeout_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    current_state: CircuitState = CircuitState.CLOSED
    opened_at: Optional[float] = None
    cumulative_open_duration: float = 0.0

    def success_rate(self) -> float:
        """Fraction of total calls that succeeded.

        Returns 1.0 when no calls have been observed yet (vacuously healthy).
        """
        if self.total_calls == 0:
            return 1.0
        return self.successful_calls / self.total_calls

    def to_dict(self) -> dict:
        """Serialize the stats to a JSON-friendly dictionary.

        ``current_state`` is emitted as its string value (e.g. ``"CLOSED"``)
        rather than the enum instance, so callers can hand the dict straight
        to ``json.dumps`` or a templating layer.
        """
        return {
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "timeout_calls": self.timeout_calls,
            "state_changes": self.state_changes,
            "last_failure_time": self.last_failure_time,
            "last_success_time": self.last_success_time,
            "current_state": self.current_state.value,
            "opened_at": self.opened_at,
            "cumulative_open_duration": self.cumulative_open_duration,
            "success_rate": self.success_rate(),
        }
