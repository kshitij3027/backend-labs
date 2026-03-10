"""Local JSONL file fallback when RabbitMQ is unavailable."""

import json
import os
import threading
import logging

logger = logging.getLogger(__name__)


class FallbackStorage:
    """Writes batches to JSONL files when the broker is down, drains them on recovery."""

    def __init__(self, storage_dir="fallback_logs"):
        self._storage_dir = storage_dir
        self._lock = threading.Lock()
        os.makedirs(storage_dir, exist_ok=True)
        self._file_path = os.path.join(storage_dir, "fallback.jsonl")

    def write(self, batch):
        """Append a batch of log entries to the fallback JSONL file."""
        with self._lock:
            with open(self._file_path, "a") as f:
                for entry in batch:
                    f.write(json.dumps(entry) + "\n")
        logger.info("Wrote %d entries to fallback storage", len(batch))

    def drain(self, callback, chunk_size=100):
        """Read all entries from fallback, call callback in chunks, delete file.

        Args:
            callback: Callable(list[dict]) to process each chunk.
            chunk_size: Number of entries per callback invocation.

        Returns:
            Total number of entries drained.
        """
        with self._lock:
            if not os.path.exists(self._file_path):
                return 0

            entries = []
            try:
                with open(self._file_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entries.append(json.loads(line))
            except (json.JSONDecodeError, IOError) as e:
                logger.error("Error reading fallback file: %s", e)
                return 0

            if not entries:
                return 0

            total = len(entries)

            # Process in chunks
            for i in range(0, total, chunk_size):
                chunk = entries[i:i + chunk_size]
                callback(chunk)

            # Delete the file after successful drain
            try:
                os.remove(self._file_path)
            except OSError:
                pass

            logger.info("Drained %d entries from fallback storage", total)
            return total

    def has_data(self):
        """Check if there is fallback data to drain."""
        with self._lock:
            return os.path.exists(self._file_path) and os.path.getsize(self._file_path) > 0
