"""Append-only log writer with size-based and time-based rotation."""

import os
import threading
from datetime import datetime, timezone

from src.config import Config


class LogWriter:
    def __init__(self, config: Config, time_func=None):
        self._config = config
        self._time_func = time_func or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()
        self._file = None
        self._filepath = os.path.join(config.log_dir, config.log_filename)
        self._last_rotation = self._time_func()
        os.makedirs(config.log_dir, exist_ok=True)
        self._open()

    def _open(self):
        self._file = open(self._filepath, "a")

    def _close(self):
        if self._file and not self._file.closed:
            self._file.close()

    def _should_rotate_size(self) -> bool:
        try:
            return os.path.getsize(self._filepath) >= self._config.max_file_size_bytes
        except OSError:
            return False

    def _should_rotate_time(self) -> bool:
        elapsed = (self._time_func() - self._last_rotation).total_seconds()
        return elapsed >= self._config.rotation_interval_seconds

    def _rotate(self) -> str:
        """Rename-and-create rotation. Returns the path of the rotated file."""
        self._close()
        now = self._time_func()
        timestamp = now.strftime("%Y%m%d_%H%M%S_") + f"{now.microsecond:06d}"
        rotated_name = f"{self._config.log_filename}.{timestamp}"
        rotated_path = os.path.join(self._config.log_dir, rotated_name)
        os.rename(self._filepath, rotated_path)
        self._open()
        self._last_rotation = now
        return rotated_path

    def write(self, entry: str) -> str | None:
        """Append a line. Returns rotated file path if rotation occurred."""
        with self._lock:
            self._file.write(entry if entry.endswith("\n") else entry + "\n")
            self._file.flush()

            if self._should_rotate_size() or self._should_rotate_time():
                return self._rotate()
            return None

    def close(self):
        with self._lock:
            self._close()
