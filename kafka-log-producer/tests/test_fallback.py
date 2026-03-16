"""Tests for FallbackStorage — uses tmp_path for isolation."""

import pytest

from src.fallback_storage import FallbackStorage
from src.models import LogEntry, LogLevel


def _make_entries(n: int = 3) -> list[LogEntry]:
    """Return *n* deterministic LogEntry instances."""
    return [
        LogEntry(
            level=LogLevel.INFO,
            message=f"fallback msg {i}",
            service="fb-svc",
            trace_id=f"trace{i:04d}",
        )
        for i in range(n)
    ]


class TestWriteAndHasData:
    """Verify write persists entries and has_data reflects file state."""

    def test_write_then_has_data(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "fb.jsonl"))
        assert not storage.has_data()

        entries = _make_entries(2)
        written = storage.write(entries)

        assert written == 2
        assert storage.has_data()

    def test_has_data_false_when_empty(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "fb.jsonl"))
        assert not storage.has_data()


class TestDrainCallback:
    """Verify drain reads entries and invokes callback correctly."""

    def test_drain_calls_callback_with_entries(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "fb.jsonl"))
        entries = _make_entries(3)
        storage.write(entries)

        received: list[LogEntry] = []

        def collector(chunk: list[LogEntry]) -> None:
            received.extend(chunk)

        drained = storage.drain(collector, chunk_size=100)

        assert drained == 3
        assert len(received) == 3
        # Messages should match what was written
        assert received[0].message == "fallback msg 0"
        assert received[2].message == "fallback msg 2"
        # File should be deleted after drain
        assert not storage.has_data()

    def test_drain_respects_chunk_size(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "fb.jsonl"))
        storage.write(_make_entries(5))

        chunk_sizes: list[int] = []

        def size_tracker(chunk: list[LogEntry]) -> None:
            chunk_sizes.append(len(chunk))

        storage.drain(size_tracker, chunk_size=2)

        # 5 entries with chunk_size=2 -> chunks of [2, 2, 1]
        assert chunk_sizes == [2, 2, 1]


class TestEmptyDrain:
    """Verify drain on empty/missing file returns 0."""

    def test_drain_empty_returns_zero(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "fb.jsonl"))

        called = False

        def should_not_be_called(chunk):
            nonlocal called
            called = True

        result = storage.drain(should_not_be_called)

        assert result == 0
        assert not called


class TestCount:
    """Verify count reflects the number of stored entries."""

    def test_count_matches_written(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "fb.jsonl"))
        assert storage.count == 0

        storage.write(_make_entries(4))
        assert storage.count == 4

    def test_count_zero_when_no_file(self, tmp_path):
        storage = FallbackStorage(str(tmp_path / "nonexistent.jsonl"))
        assert storage.count == 0
