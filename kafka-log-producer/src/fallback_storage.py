"""Disk-based fallback buffer for when Kafka is unavailable."""

import json
import os
import threading
from pathlib import Path
from typing import Callable

from src.models import LogEntry


class FallbackStorage:
    """Append-only JSONL file that stores LogEntry instances when the Kafka
    producer cannot accept messages (buffer full, broker down, etc.).

    The file is atomically drained and deleted once entries are replayed.
    """

    def __init__(self, storage_path: str = "/tmp/kafka_fallback.jsonl") -> None:
        self._path = Path(storage_path)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, entries: list[LogEntry]) -> int:
        """Append *entries* as JSON lines. Returns the number written."""
        with self._lock:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "a") as fh:
                for entry in entries:
                    fh.write(entry.to_kafka_value() + "\n")
            return len(entries)

    # ------------------------------------------------------------------
    # Drain
    # ------------------------------------------------------------------

    def drain(
        self,
        callback: Callable[[list[LogEntry]], None],
        chunk_size: int = 100,
    ) -> int:
        """Read all stored entries, invoke *callback* in chunks, then delete
        the file.  Returns the total number of entries drained."""
        with self._lock:
            if not self._path.exists() or self._path.stat().st_size == 0:
                return 0

            with open(self._path, "r") as fh:
                lines = fh.readlines()

            entries = [LogEntry.from_kafka_value(line.strip()) for line in lines if line.strip()]
            total = len(entries)

            for i in range(0, total, chunk_size):
                chunk = entries[i : i + chunk_size]
                callback(chunk)

            self._path.unlink(missing_ok=True)
            return total

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def has_data(self) -> bool:
        """Return ``True`` if the fallback file exists and is non-empty."""
        return self._path.exists() and self._path.stat().st_size > 0

    @property
    def count(self) -> int:
        """Return the number of entries stored on disk."""
        if not self._path.exists():
            return 0
        with open(self._path, "r") as fh:
            return sum(1 for line in fh if line.strip())
