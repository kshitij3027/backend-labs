"""BatchWriter: consumer thread that drains the queue and flushes JSON batches."""

import json
import os
import time
import queue
import logging
from datetime import datetime
from threading import Thread

from src.config import Config
from src.parsers import parse_line

logger = logging.getLogger(__name__)


class BatchWriter(Thread):
    def __init__(self, q: queue.Queue, config: Config, processor=None):
        super().__init__(daemon=True)
        self._queue = q
        self._config = config
        self._processor = processor
        self._buffer: list[dict] = []
        self._last_flush = time.time()
        self._running = True
        self._batch_count = 0
        self._total_entries = 0

    @property
    def total_entries(self) -> int:
        return self._total_entries

    @property
    def batch_count(self) -> int:
        return self._batch_count

    def _flush(self):
        """Write buffered entries as a JSON array file."""
        if not self._buffer:
            return

        os.makedirs(self._config.output_dir, exist_ok=True)
        self._batch_count += 1
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"collected_{ts}_{self._batch_count:03d}.json"
        filepath = os.path.join(self._config.output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self._buffer, f, indent=2)

        count = len(self._buffer)
        self._total_entries += count
        logger.info("Flushed %d entries to %s (total: %d)", count, filename, self._total_entries)
        self._buffer.clear()
        self._last_flush = time.time()

    def run(self):
        while self._running:
            try:
                line, source_file = self._queue.get(timeout=0.5)
            except queue.Empty:
                # Check time-based flush
                if self._buffer and (time.time() - self._last_flush) >= self._config.flush_interval:
                    self._flush()
                continue

            entry = parse_line(line, source_file)
            if entry is None:
                continue

            # Apply filter + tag if processor is available
            if self._processor:
                entry = self._processor.process(entry)
                if entry is None:
                    continue

            # Convert to output dict
            entry_dict = {
                "timestamp": entry.timestamp,
                "level": entry.level,
                "id": entry.id,
                "service": entry.service,
                "user_id": entry.user_id,
                "request_id": entry.request_id,
                "duration_ms": entry.duration_ms,
                "message": entry.message,
                "source_file": entry.source_file,
                "tags": entry.tags,
                "captured_at": datetime.now().isoformat(),
                "raw": entry.raw,
            }
            self._buffer.append(entry_dict)

            # Size-based flush
            if len(self._buffer) >= self._config.batch_size:
                self._flush()

    def stop(self):
        """Signal shutdown and flush remaining buffer."""
        self._running = False
        # Drain any remaining items in queue
        while True:
            try:
                line, source_file = self._queue.get_nowait()
                entry = parse_line(line, source_file)
                if entry is None:
                    continue
                if self._processor:
                    entry = self._processor.process(entry)
                    if entry is None:
                        continue
                entry_dict = {
                    "timestamp": entry.timestamp,
                    "level": entry.level,
                    "id": entry.id,
                    "service": entry.service,
                    "user_id": entry.user_id,
                    "request_id": entry.request_id,
                    "duration_ms": entry.duration_ms,
                    "message": entry.message,
                    "source_file": entry.source_file,
                    "tags": entry.tags,
                    "captured_at": datetime.now().isoformat(),
                    "raw": entry.raw,
                }
                self._buffer.append(entry_dict)
            except queue.Empty:
                break
        self._flush()
