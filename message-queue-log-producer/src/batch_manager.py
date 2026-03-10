"""Hybrid size+time batch manager with daemon flush thread."""

import threading
import logging

logger = logging.getLogger(__name__)


class BatchManager:
    """Batches log entries and flushes by size threshold or time interval.

    Uses a daemon thread that waits on a threading.Event with a timeout.
    The event is set when the buffer reaches max_size, causing immediate flush.
    If the event times out (flush_interval_s), it flushes whatever is buffered.
    """

    def __init__(self, max_size, flush_interval_s, on_flush):
        """
        Args:
            max_size: Number of entries that triggers an immediate flush.
            flush_interval_s: Max seconds between flushes (time-based).
            on_flush: Callable(batch) invoked with the flushed list of entries.
        """
        self._max_size = max_size
        self._flush_interval = flush_interval_s
        self._on_flush = on_flush
        self._buffer = []
        self._lock = threading.Lock()
        self._size_event = threading.Event()
        self._stop_event = threading.Event()

        self._thread = threading.Thread(target=self._run, daemon=True, name="batch-flush")
        self._thread.start()

    def add(self, entry):
        """Add a log entry to the buffer. Thread-safe."""
        with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self._max_size:
                self._size_event.set()

    @property
    def buffer_size(self):
        """Current number of entries in the buffer."""
        with self._lock:
            return len(self._buffer)

    def stop(self):
        """Stop the flush thread, performing a final flush."""
        self._stop_event.set()
        self._size_event.set()  # Wake up the thread if waiting
        self._thread.join(timeout=5)
        self._flush()  # Final flush of remaining entries

    def _run(self):
        """Daemon thread loop: wait for size event or timeout, then flush."""
        while not self._stop_event.is_set():
            # Wait for either: buffer full (event set) or timeout
            triggered = self._size_event.wait(timeout=self._flush_interval)
            if triggered:
                self._size_event.clear()
            self._flush()

    def _flush(self):
        """Atomically swap the buffer and invoke the callback."""
        with self._lock:
            if not self._buffer:
                return
            batch, self._buffer = self._buffer, []

        try:
            self._on_flush(batch)
        except Exception:
            logger.exception("Error in on_flush callback")
