"""Server-side rotating file writer for received log entries."""

import json
import os
import threading


class RotatingLogWriter:
    """Thread-safe log writer that rotates files every N entries."""

    def __init__(self, log_dir: str, max_per_file: int = 10):
        self._log_dir = log_dir
        self._max_per_file = max_per_file
        self._lock = threading.Lock()
        self._file_index = 0
        self._entry_count = 0
        self._current_file = None
        os.makedirs(log_dir, exist_ok=True)
        self._open_next_file()

    def _open_next_file(self):
        if self._current_file:
            self._current_file.close()
        filename = f"log_{self._file_index:03d}.jsonl"
        path = os.path.join(self._log_dir, filename)
        self._current_file = open(path, "a")
        self._entry_count = 0

    def write(self, log_entry: dict):
        """Write a log entry, rotating if max entries reached."""
        with self._lock:
            if self._entry_count >= self._max_per_file:
                self._file_index += 1
                self._open_next_file()
            self._current_file.write(json.dumps(log_entry) + "\n")
            self._current_file.flush()
            self._entry_count += 1

    @property
    def current_file_index(self) -> int:
        with self._lock:
            return self._file_index

    def close(self):
        with self._lock:
            if self._current_file:
                self._current_file.close()
                self._current_file = None
