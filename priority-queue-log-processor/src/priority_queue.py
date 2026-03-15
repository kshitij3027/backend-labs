"""Thread-safe priority queue backed by a binary heap."""

import heapq
import threading
import time
from typing import Optional

from src.config import Settings
from src.models import REMOVED, LogMessage, Priority


class ThreadSafePriorityQueue:
    """A bounded priority queue with backpressure and priority promotion.

    Uses heapq internally. Each heap entry is a list
    ``[priority_value, counter, message]`` where *counter* is a monotonically
    increasing int that guarantees FIFO ordering among messages with the same
    priority.  Lazy deletion is used for ``promote``: the old entry's message
    slot is overwritten with the ``REMOVED`` sentinel and a fresh entry is
    pushed.
    """

    def __init__(self, max_size: int, settings: Optional[Settings] = None) -> None:
        self._max_size = max_size
        self._heap: list = []
        self._lock = threading.Lock()
        self._counter: int = 0
        self._entry_map: dict[str, list] = {}
        self._size: int = 0
        self._priority_counts: dict[Priority, int] = {p: 0 for p in Priority}

        # Backpressure watermarks
        if settings is not None:
            self._low_wm = settings.backpressure_low_watermark
            self._med_wm = settings.backpressure_medium_watermark
            self._high_wm = settings.backpressure_high_watermark
        else:
            self._low_wm = 0.8
            self._med_wm = 0.9
            self._high_wm = 0.95

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push(self, message: LogMessage) -> bool:
        """Insert a message into the queue.

        Returns True on success, False if the message was rejected due to
        capacity limits or backpressure policy.
        """
        with self._lock:
            if self._size >= self._max_size:
                return False

            utilization = self._size / self._max_size if self._max_size > 0 else 0.0

            # Backpressure: high watermark — only CRITICAL allowed
            if utilization >= self._high_wm and message.priority != Priority.CRITICAL:
                return False

            # Backpressure: medium watermark — reject MEDIUM and LOW
            if utilization >= self._med_wm and message.priority in (
                Priority.MEDIUM,
                Priority.LOW,
            ):
                return False

            # Backpressure: low watermark — reject LOW only
            if utilization >= self._low_wm and message.priority == Priority.LOW:
                return False

            entry: list = [message.priority.value, self._counter, message]
            heapq.heappush(self._heap, entry)
            self._entry_map[message.id] = entry
            self._counter += 1
            self._size += 1
            self._priority_counts[message.priority] += 1
            return True

    def pop(self, timeout: float = 0) -> Optional[LogMessage]:
        """Remove and return the highest-priority message.

        Returns None if the queue is empty.  The *timeout* parameter is
        accepted for interface compatibility but is not currently used for
        blocking.
        """
        with self._lock:
            while self._heap:
                entry = heapq.heappop(self._heap)
                if entry[2] is REMOVED:
                    continue
                msg: LogMessage = entry[2]
                self._size -= 1
                self._priority_counts[msg.priority] -= 1
                self._entry_map.pop(msg.id, None)
                return msg
            return None

    def promote(self, message_id: str, new_priority: Priority) -> bool:
        """Change a queued message's priority (lower value = higher).

        The message's ``created_at`` is reset to the current time so that the
        aging clock restarts after promotion.  Returns False if the message
        is not found.
        """
        with self._lock:
            if message_id not in self._entry_map:
                return False

            old_entry = self._entry_map[message_id]
            old_msg: LogMessage = old_entry[2]
            old_priority = old_msg.priority

            # Mark old entry as removed (lazy deletion)
            old_entry[2] = REMOVED

            # Build updated message
            promoted = LogMessage(
                id=old_msg.id,
                timestamp=old_msg.timestamp,
                created_at=time.time(),
                priority=new_priority,
                source=old_msg.source,
                message=old_msg.message,
                original_priority=old_msg.original_priority,
            )

            new_entry: list = [new_priority.value, self._counter, promoted]
            heapq.heappush(self._heap, new_entry)
            self._entry_map[message_id] = new_entry
            self._counter += 1

            # Adjust counts
            self._priority_counts[old_priority] -= 1
            self._priority_counts[new_priority] += 1
            return True

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        return self._size

    @property
    def is_empty(self) -> bool:
        return self._size == 0

    @property
    def is_full(self) -> bool:
        return self._size >= self._max_size

    @property
    def utilization(self) -> float:
        return self._size / self._max_size if self._max_size > 0 else 0.0

    @property
    def priority_counts(self) -> dict[Priority, int]:
        return dict(self._priority_counts)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Return a snapshot of queue statistics."""
        with self._lock:
            return {
                "size": self._size,
                "max_size": self._max_size,
                "utilization": self._size / self._max_size if self._max_size > 0 else 0.0,
                "priority_counts": {p.name: self._priority_counts[p] for p in Priority},
                "is_full": self._size >= self._max_size,
            }
