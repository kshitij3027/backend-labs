"""Tests for the BatchBuffer module."""

import threading
import time

import pytest

from src.batch_buffer import BatchBuffer


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_buffer(batch_size=3, flush_interval=60.0):
    """Create a BatchBuffer wired to a simple list-based collector."""
    flushed: list[list[dict]] = []

    def on_flush(batch):
        flushed.append(batch)

    shutdown = threading.Event()
    buf = BatchBuffer(
        batch_size=batch_size,
        flush_interval=flush_interval,
        on_flush=on_flush,
        shutdown_event=shutdown,
    )
    return buf, flushed, shutdown


def _entry(i: int) -> dict:
    return {"seq": i, "message": f"log-{i}"}


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

class TestBatchSizeFlush:
    """Flushing triggered by reaching the batch-size threshold."""

    def test_flush_at_batch_size(self):
        """Adding exactly batch_size entries triggers one flush."""
        buf, flushed, shutdown = _make_buffer(batch_size=3)

        for i in range(3):
            buf.add(_entry(i))

        assert len(flushed) == 1
        assert len(flushed[0]) == 3
        assert buf.pending_count == 0

        shutdown.set()
        buf.stop()

    def test_no_premature_flush(self):
        """Adding fewer than batch_size entries does NOT trigger a flush."""
        buf, flushed, shutdown = _make_buffer(batch_size=5)

        for i in range(4):
            buf.add(_entry(i))

        assert len(flushed) == 0
        assert buf.pending_count == 4

        shutdown.set()
        buf.stop()

    def test_multiple_batches(self):
        """Adding 2x batch_size entries triggers exactly two flushes."""
        buf, flushed, shutdown = _make_buffer(batch_size=3)

        for i in range(6):
            buf.add(_entry(i))

        assert len(flushed) == 2
        assert len(flushed[0]) == 3
        assert len(flushed[1]) == 3
        assert buf.pending_count == 0

        shutdown.set()
        buf.stop()


class TestTimerFlush:
    """Flushing triggered by the background timer thread."""

    def test_timer_flush(self):
        """Entries below batch_size are flushed after flush_interval elapses."""
        buf, flushed, shutdown = _make_buffer(
            batch_size=10, flush_interval=0.5
        )

        buf.add(_entry(0))
        assert len(flushed) == 0

        # Wait long enough for the timer thread to trigger a flush.
        # The timer polls every 1.0 s, so we need >1.5 s to be safe.
        time.sleep(2.0)

        assert len(flushed) == 1
        assert len(flushed[0]) == 1
        assert buf.pending_count == 0

        shutdown.set()
        buf.stop()


class TestShutdown:
    """Flushing on stop / shutdown."""

    def test_shutdown_flush(self):
        """Calling stop() flushes remaining entries that are below the
        batch-size threshold."""
        buf, flushed, shutdown = _make_buffer(batch_size=10)

        for i in range(4):
            buf.add(_entry(i))

        assert len(flushed) == 0

        buf.stop()

        assert len(flushed) == 1
        assert len(flushed[0]) == 4

    def test_empty_buffer_no_op(self):
        """Stopping an empty buffer never calls on_flush."""
        buf, flushed, shutdown = _make_buffer(batch_size=5)

        buf.stop()

        assert len(flushed) == 0


class TestDynamicConfig:
    """Dynamically changing batch_size and flush_interval at runtime."""

    def test_dynamic_batch_size_change(self):
        """Lowering batch_size below the current buffer length triggers
        an immediate flush."""
        buf, flushed, shutdown = _make_buffer(batch_size=5)

        for i in range(3):
            buf.add(_entry(i))

        assert len(flushed) == 0
        assert buf.pending_count == 3

        # Shrink batch_size to 3 — buffer already has 3 entries, so it
        # should flush immediately.
        buf.batch_size = 3

        assert len(flushed) == 1
        assert len(flushed[0]) == 3
        assert buf.pending_count == 0

        shutdown.set()
        buf.stop()

    def test_dynamic_flush_interval_change(self):
        """flush_interval can be updated at runtime."""
        buf, flushed, shutdown = _make_buffer(
            batch_size=100, flush_interval=60.0
        )

        buf.flush_interval = 0.5
        assert buf.flush_interval == 0.5

        buf.add(_entry(0))
        time.sleep(2.0)

        assert len(flushed) == 1

        shutdown.set()
        buf.stop()


class TestPendingCount:
    """The pending_count property."""

    def test_pending_count_reflects_buffer_state(self):
        """pending_count tracks the number of unflushed entries."""
        buf, flushed, shutdown = _make_buffer(batch_size=5)

        assert buf.pending_count == 0

        buf.add(_entry(0))
        buf.add(_entry(1))
        assert buf.pending_count == 2

        # Fill to threshold — flush empties the buffer.
        for i in range(2, 5):
            buf.add(_entry(i))
        assert buf.pending_count == 0

        shutdown.set()
        buf.stop()
