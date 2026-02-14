"""Thread-safe log persistence — writes log entries to disk."""

import os
import threading
from datetime import datetime, timezone


class LogPersistence:
    """Append-only file writer with thread-safe access."""

    def __init__(self, log_dir: str, log_filename: str, enabled: bool = True):
        self._enabled = enabled
        self._lock = threading.Lock()
        self._file = None

        if self._enabled:
            os.makedirs(log_dir, exist_ok=True)
            path = os.path.join(log_dir, log_filename)
            self._file = open(path, "a", encoding="utf-8")

    def write(self, level: str, message: str) -> bool:
        """Write a log entry. Returns True if written, False if disabled or closed."""
        if not self._enabled or self._file is None:
            return False

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        line = f"{timestamp} [{level.upper()}] {message}\n"

        with self._lock:
            self._file.write(line)
            self._file.flush()
        return True

    def close(self):
        """Close the file handle."""
        if self._file is not None:
            with self._lock:
                self._file.close()
                self._file = None
