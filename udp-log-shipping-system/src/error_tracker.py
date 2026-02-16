"""In-memory ring buffer for recent ERROR log entries."""

import threading


class ErrorTracker:
    def __init__(self, max_size: int = 100):
        self._max_size = max_size
        self._errors: list[dict] = []
        self._lock = threading.Lock()

    def add(self, entry: dict):
        """Store an error entry, evicting the oldest if at capacity."""
        with self._lock:
            self._errors.append(entry)
            if len(self._errors) > self._max_size:
                self._errors.pop(0)

    def get_recent(self, n: int = 10) -> list[dict]:
        """Return the N most recent errors."""
        with self._lock:
            return list(self._errors[-n:])

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._errors)
