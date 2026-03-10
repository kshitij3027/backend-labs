"""Tests for BatchManager — size-based, time-based, and stop flushing."""

import threading
import time

import pytest

from src.batch_manager import BatchManager


@pytest.fixture
def flush_capture():
    """Provide a callback that captures flush batches and an event for synchronization."""
    results = []
    event = threading.Event()

    def on_flush(batch):
        results.append(batch)
        event.set()

    return results, event, on_flush


class TestBatchManager:
    """Tests for the BatchManager class."""

    def test_add_increases_buffer_size(self, flush_capture):
        """Adding entries increases buffer_size accordingly."""
        _, _, on_flush = flush_capture
        bm = BatchManager(max_size=100, flush_interval_s=5.0, on_flush=on_flush)
        try:
            bm.add({"msg": "a"})
            bm.add({"msg": "b"})
            bm.add({"msg": "c"})
            assert bm.buffer_size == 3
        finally:
            bm.stop()

    def test_size_based_flush(self, flush_capture):
        """Reaching max_size triggers an immediate flush via callback."""
        results, event, on_flush = flush_capture
        bm = BatchManager(max_size=3, flush_interval_s=5.0, on_flush=on_flush)
        try:
            bm.add({"msg": "a"})
            bm.add({"msg": "b"})
            bm.add({"msg": "c"})

            triggered = event.wait(timeout=2.0)
            assert triggered, "Flush callback was not called within timeout"
            assert len(results) == 1
            assert len(results[0]) == 3
        finally:
            bm.stop()

    def test_time_based_flush(self, flush_capture):
        """Entries are flushed after flush_interval_s even if max_size not reached."""
        results, event, on_flush = flush_capture
        bm = BatchManager(max_size=100, flush_interval_s=0.5, on_flush=on_flush)
        try:
            bm.add({"msg": "lone_entry"})

            triggered = event.wait(timeout=2.0)
            assert triggered, "Time-based flush callback was not called within timeout"
            assert len(results) == 1
            assert results[0] == [{"msg": "lone_entry"}]
        finally:
            bm.stop()

    def test_empty_buffer_no_callback(self):
        """An empty buffer should not trigger the callback on time-based flush."""
        results = []

        def on_flush(batch):
            results.append(batch)

        bm = BatchManager(max_size=100, flush_interval_s=0.3, on_flush=on_flush)
        try:
            time.sleep(0.5)
            assert len(results) == 0, "Callback should not be invoked for an empty buffer"
        finally:
            bm.stop()

    def test_stop_final_flush(self):
        """Calling stop() flushes remaining entries that haven't reached threshold."""
        results = []

        def on_flush(batch):
            results.append(batch)

        bm = BatchManager(max_size=100, flush_interval_s=5.0, on_flush=on_flush)
        bm.add({"msg": "pending1"})
        bm.add({"msg": "pending2"})
        bm.stop()

        assert len(results) == 1
        assert len(results[0]) == 2
        assert results[0][0] == {"msg": "pending1"}
        assert results[0][1] == {"msg": "pending2"}

    def test_multiple_flushes(self):
        """Callback is called once per batch when max_size is reached multiple times."""
        results = []
        events = [threading.Event(), threading.Event()]
        call_count = 0

        def on_flush(batch):
            nonlocal call_count
            results.append(batch)
            if call_count < len(events):
                events[call_count].set()
            call_count += 1

        bm = BatchManager(max_size=3, flush_interval_s=5.0, on_flush=on_flush)
        try:
            # First batch of 3
            bm.add({"msg": "a1"})
            bm.add({"msg": "a2"})
            bm.add({"msg": "a3"})
            events[0].wait(timeout=2.0)

            # Second batch of 3
            bm.add({"msg": "b1"})
            bm.add({"msg": "b2"})
            bm.add({"msg": "b3"})
            events[1].wait(timeout=2.0)

            assert len(results) == 2
            assert len(results[0]) == 3
            assert len(results[1]) == 3
        finally:
            bm.stop()

    def test_callback_receives_correct_data(self, flush_capture):
        """The callback batch contains exactly the entries that were added."""
        results, event, on_flush = flush_capture
        entries = [{"level": "INFO", "msg": "hello"}, {"level": "WARN", "msg": "caution"}, {"level": "ERROR", "msg": "fail"}]

        bm = BatchManager(max_size=3, flush_interval_s=5.0, on_flush=on_flush)
        try:
            for entry in entries:
                bm.add(entry)

            event.wait(timeout=2.0)
            assert len(results) == 1
            assert results[0] == entries
        finally:
            bm.stop()

    def test_buffer_cleared_after_flush(self, flush_capture):
        """After a size-based flush, the buffer_size should be 0."""
        _, event, on_flush = flush_capture
        bm = BatchManager(max_size=3, flush_interval_s=5.0, on_flush=on_flush)
        try:
            bm.add({"msg": "x"})
            bm.add({"msg": "y"})
            bm.add({"msg": "z"})

            event.wait(timeout=2.0)
            # Small sleep to let the flush thread finish clearing the buffer
            time.sleep(0.1)
            assert bm.buffer_size == 0
        finally:
            bm.stop()
