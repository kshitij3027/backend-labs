"""Tests for src/batch_buffer.py — BatchBuffer batching and flushing logic."""

import threading
import time

import pytest

from src.batch_buffer import BatchBuffer


# ── Helpers ─────────────────────────────────────────────────────────


def _make_entry(i: int) -> dict:
    """Create a simple log-like entry with an index for ordering checks."""
    return {"index": i, "message": f"log entry {i}"}


def _make_buffer(
    batch_size: int = 5,
    flush_interval: float = 999.0,
    on_flush=None,
    shutdown_event: threading.Event | None = None,
) -> tuple[BatchBuffer, list[list[dict]], threading.Event]:
    """Factory that returns (buffer, flushed_batches_list, shutdown_event).

    Uses a very high flush_interval by default so timer-based flushes
    don't interfere with batch-size tests.
    """
    flushed: list[list[dict]] = []
    if on_flush is None:
        on_flush = lambda batch: flushed.append(batch)  # noqa: E731
    event = shutdown_event or threading.Event()
    buf = BatchBuffer(
        batch_size=batch_size,
        flush_interval=flush_interval,
        on_flush=on_flush,
        shutdown_event=event,
    )
    return buf, flushed, event


# ── Tests: batch-size triggered flush ───────────────────────────────


class TestBatchSizeFlush:
    """Verify that the buffer flushes exactly when batch_size is reached."""

    def test_flush_at_batch_size(self):
        """Adding exactly batch_size entries triggers a single flush with
        all entries."""
        buf, flushed, _ = _make_buffer(batch_size=3)

        for i in range(3):
            buf.add(_make_entry(i))

        assert len(flushed) == 1
        assert len(flushed[0]) == 3
        buf.stop()

    def test_no_premature_flush(self):
        """Adding fewer than batch_size entries does NOT trigger a flush."""
        buf, flushed, _ = _make_buffer(batch_size=5)

        for i in range(4):
            buf.add(_make_entry(i))

        # Give a brief window for any erroneous async flush
        time.sleep(0.2)
        assert len(flushed) == 0
        buf.stop()

    def test_multiple_batches(self):
        """Adding 2x batch_size entries triggers exactly two flushes."""
        buf, flushed, _ = _make_buffer(batch_size=4)

        for i in range(8):
            buf.add(_make_entry(i))

        assert len(flushed) == 2
        assert len(flushed[0]) == 4
        assert len(flushed[1]) == 4
        buf.stop()

    def test_three_batches_plus_remainder(self):
        """Adding 3.5x batch_size → 3 flushes; remainder stays buffered."""
        buf, flushed, _ = _make_buffer(batch_size=2)

        for i in range(7):
            buf.add(_make_entry(i))

        assert len(flushed) == 3
        assert buf.pending_count == 1
        buf.stop()


# ── Tests: timer-based flush ────────────────────────────────────────


class TestTimerFlush:
    """Verify that the background timer flushes partial batches."""

    def test_timer_flushes_partial_batch(self):
        """Entries below batch_size are flushed after flush_interval elapses."""
        buf, flushed, _ = _make_buffer(batch_size=100, flush_interval=0.5)

        for i in range(3):
            buf.add(_make_entry(i))

        # Wait long enough for the timer to fire (interval=0.5s + timer
        # poll period up to 1s)
        time.sleep(2.0)

        assert len(flushed) == 1
        assert len(flushed[0]) == 3
        buf.stop()

    def test_timer_does_not_flush_empty_buffer(self):
        """Timer fires but buffer is empty → no flush callback."""
        buf, flushed, _ = _make_buffer(batch_size=100, flush_interval=0.5)

        time.sleep(2.0)

        assert len(flushed) == 0
        buf.stop()


# ── Tests: shutdown / stop ──────────────────────────────────────────


class TestShutdown:
    """Verify that stop() drains remaining entries."""

    def test_stop_drains_remaining(self):
        """Calling stop() flushes entries that haven't reached batch_size."""
        buf, flushed, _ = _make_buffer(batch_size=10)

        for i in range(6):
            buf.add(_make_entry(i))

        assert len(flushed) == 0  # not yet at batch_size

        buf.stop()

        assert len(flushed) == 1
        assert len(flushed[0]) == 6

    def test_stop_with_empty_buffer(self):
        """Calling stop() with no entries does NOT trigger a flush."""
        buf, flushed, _ = _make_buffer(batch_size=5)
        buf.stop()

        assert len(flushed) == 0


# ── Tests: error handling ───────────────────────────────────────────


class TestCallbackErrors:
    """Verify resilience when the on_flush callback raises."""

    def test_callback_exception_does_not_crash(self):
        """A failing callback doesn't crash the buffer; subsequent flushes
        still work."""
        call_count = 0
        results: list[list[dict]] = []

        def flaky_callback(batch):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated failure")
            results.append(batch)

        buf, _, _ = _make_buffer(batch_size=2, on_flush=flaky_callback)

        # First batch → callback raises
        buf.add(_make_entry(0))
        buf.add(_make_entry(1))
        assert call_count == 1

        # Second batch → callback succeeds
        buf.add(_make_entry(2))
        buf.add(_make_entry(3))
        assert call_count == 2
        assert len(results) == 1
        assert len(results[0]) == 2

        buf.stop()


# ── Tests: pending_count ────────────────────────────────────────────


class TestPendingCount:
    """Verify pending_count reflects the current buffer state."""

    def test_pending_count_increases(self):
        """pending_count grows as entries are added."""
        buf, _, _ = _make_buffer(batch_size=10)

        assert buf.pending_count == 0
        buf.add(_make_entry(0))
        assert buf.pending_count == 1
        buf.add(_make_entry(1))
        assert buf.pending_count == 2
        buf.stop()

    def test_pending_count_resets_after_flush(self):
        """pending_count drops to zero after a batch-size flush."""
        buf, _, _ = _make_buffer(batch_size=3)

        for i in range(3):
            buf.add(_make_entry(i))

        assert buf.pending_count == 0
        buf.stop()

    def test_pending_count_after_stop(self):
        """pending_count is zero after stop() drains the buffer."""
        buf, _, _ = _make_buffer(batch_size=10)

        for i in range(4):
            buf.add(_make_entry(i))

        assert buf.pending_count == 4
        buf.stop()
        assert buf.pending_count == 0


# ── Tests: entry ordering ──────────────────────────────────────────


class TestEntryOrdering:
    """Verify that entries are flushed in insertion order."""

    def test_order_preserved_in_single_batch(self):
        """Entries within a single batch maintain insertion order."""
        buf, flushed, _ = _make_buffer(batch_size=5)

        for i in range(5):
            buf.add(_make_entry(i))

        assert len(flushed) == 1
        indices = [e["index"] for e in flushed[0]]
        assert indices == [0, 1, 2, 3, 4]
        buf.stop()

    def test_order_preserved_across_batches(self):
        """Entries across multiple batches maintain global insertion order."""
        buf, flushed, _ = _make_buffer(batch_size=3)

        for i in range(9):
            buf.add(_make_entry(i))

        assert len(flushed) == 3
        all_indices = []
        for batch in flushed:
            all_indices.extend(e["index"] for e in batch)
        assert all_indices == list(range(9))
        buf.stop()

    def test_order_preserved_with_stop_drain(self):
        """Entries flushed via stop() maintain insertion order."""
        buf, flushed, _ = _make_buffer(batch_size=100)

        for i in range(7):
            buf.add(_make_entry(i))

        buf.stop()

        assert len(flushed) == 1
        indices = [e["index"] for e in flushed[0]]
        assert indices == list(range(7))
