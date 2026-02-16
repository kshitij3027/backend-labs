"""Buffered writer — batches log entries and flushes to disk."""

import json
import os
import threading
import time
import logging

logger = logging.getLogger(__name__)


class BufferedWriter:
    def __init__(self, log_dir: str, log_filename: str, flush_count: int, flush_timeout_sec: int):
        self._log_dir = log_dir
        self._log_filename = log_filename
        self._flush_count = flush_count
        self._flush_timeout_sec = flush_timeout_sec

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()
        self._stop_event = threading.Event()

        os.makedirs(self._log_dir, exist_ok=True)

        self._timer_thread = threading.Thread(target=self._flush_timer, daemon=True)
        self._timer_thread.start()

    @property
    def _log_path(self) -> str:
        return os.path.join(self._log_dir, self._log_filename)

    def append(self, entry: dict):
        """Add an entry to the buffer. Flushes if count threshold reached."""
        with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self._flush_count:
                self._flush_locked()

    def write_immediate(self, entry: dict):
        """Write a single entry directly to disk, bypassing the buffer."""
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    def flush(self):
        """Public flush — write all buffered entries to disk."""
        with self._lock:
            self._flush_locked()

    def close(self):
        """Stop the timer thread and flush remaining entries."""
        self._stop_event.set()
        self._timer_thread.join(timeout=5)
        self.flush()
        logger.info("BufferedWriter closed")

    def _flush_locked(self):
        """Write buffered entries to disk. Must be called with self._lock held."""
        if not self._buffer:
            return

        entries = self._buffer[:]
        self._buffer.clear()
        self._last_flush = time.monotonic()

        with open(self._log_path, "a", encoding="utf-8") as f:
            for entry in entries:
                f.write(json.dumps(entry) + "\n")

        logger.debug("Flushed %d entries to %s", len(entries), self._log_path)

    def _flush_timer(self):
        """Background thread that flushes on timeout."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=1.0)
            with self._lock:
                elapsed = time.monotonic() - self._last_flush
                if elapsed >= self._flush_timeout_sec and self._buffer:
                    self._flush_locked()
