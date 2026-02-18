"""Batch buffer — collects log entries and flushes on size or time threshold."""

import threading
import time
import logging

logger = logging.getLogger(__name__)


class BatchBuffer:
    """Thread-safe buffer that batches log entries and flushes when either
    the batch size threshold is reached or a time interval elapses.

    The on_flush callback is always invoked OUTSIDE the lock so that
    slow consumers (e.g. network I/O) never block producers from adding
    new entries.
    """

    def __init__(
        self,
        batch_size: int,
        flush_interval: float,
        on_flush,
        shutdown_event: threading.Event,
    ):
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._on_flush = on_flush
        self._shutdown = shutdown_event

        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

        self._timer_thread = threading.Thread(
            target=self._flush_timer, daemon=True
        )
        self._timer_thread.start()

    # Public API

    def add(self, entry: dict):
        """Append an entry to the buffer. Flushes immediately if the
        batch-size threshold is reached."""
        batch_to_flush = None

        with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self._batch_size:
                batch_to_flush = self._buffer[:]
                self._buffer.clear()
                self._last_flush = time.monotonic()

        if batch_to_flush is not None:
            self._safe_flush(batch_to_flush)

    def stop(self):
        """Signal the timer thread to stop, wait for it, and flush any
        remaining entries."""
        self._shutdown.set()
        self._timer_thread.join(timeout=5)

        batch_to_flush = None
        with self._lock:
            if self._buffer:
                batch_to_flush = self._buffer[:]
                self._buffer.clear()
                self._last_flush = time.monotonic()

        if batch_to_flush is not None:
            self._safe_flush(batch_to_flush)

    @property
    def pending_count(self) -> int:
        """Number of entries currently waiting in the buffer."""
        with self._lock:
            return len(self._buffer)

    # Internal helpers

    def _flush_timer(self):
        """Background thread that periodically checks whether the flush
        interval has elapsed and, if so, flushes the buffer."""
        while not self._shutdown.is_set():
            self._shutdown.wait(timeout=1.0)

            batch_to_flush = None
            with self._lock:
                elapsed = time.monotonic() - self._last_flush
                if elapsed >= self._flush_interval and self._buffer:
                    batch_to_flush = self._buffer[:]
                    self._buffer.clear()
                    self._last_flush = time.monotonic()

            if batch_to_flush is not None:
                self._safe_flush(batch_to_flush)

    def _safe_flush(self, batch: list[dict]):
        """Invoke the on_flush callback with error handling so that a
        failing callback never crashes the buffer internals."""
        try:
            self._on_flush(batch)
            logger.debug("Flushed batch of %d entries", len(batch))
        except Exception:
            logger.exception(
                "on_flush callback failed for batch of %d entries", len(batch)
            )
