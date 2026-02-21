import collections
import threading


class LogStore:
    """Thread-safe in-memory log storage backed by a bounded deque."""

    def __init__(self, max_size=1000):
        self._logs = collections.deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._total_count = 0

    def add(self, log_entry):
        """Append a log entry to the store and increment the total count."""
        with self._lock:
            self._logs.append(log_entry)
            self._total_count += 1

    def get_recent(self, count=50):
        """Return the last `count` entries as a list, most recent first."""
        with self._lock:
            return list(self._logs)[-count:][::-1]

    def get_all(self):
        """Return all entries as a list, most recent first."""
        with self._lock:
            return list(reversed(self._logs))

    @property
    def total_count(self):
        """Total number of log entries ever added."""
        return self._total_count

    @property
    def current_size(self):
        """Number of log entries currently held in the store."""
        return len(self._logs)

    def clear(self):
        """Clear all entries and reset the total count."""
        with self._lock:
            self._logs.clear()
            self._total_count = 0
