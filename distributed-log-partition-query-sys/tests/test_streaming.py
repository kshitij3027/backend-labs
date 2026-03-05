import pytest
from datetime import datetime, timedelta, timezone

from src.models import LogEntry
from src.coordinator.streaming import StreamingMerger


def make_entries(
    partition_id: str, count: int, offset_minutes: int = 0
) -> list[LogEntry]:
    now = datetime.now(tz=timezone.utc)
    return [
        LogEntry(
            timestamp=now - timedelta(minutes=offset_minutes + i),
            level="INFO",
            service="test",
            message=f"msg-{i}",
            partition_id=partition_id,
        )
        for i in range(count)
    ]


class TestStreamingMerger:
    @pytest.mark.asyncio
    async def test_stream_basic(self):
        p1 = make_entries("p1", 3, offset_minutes=0)
        p2 = make_entries("p2", 3, offset_minutes=1)

        merger = StreamingMerger()
        results = []
        async for entry in merger.merge_stream([p1, p2], sort_order="desc"):
            results.append(entry)

        assert len(results) == 6
        for i in range(len(results) - 1):
            assert results[i].timestamp >= results[i + 1].timestamp

    @pytest.mark.asyncio
    async def test_stream_with_limit(self):
        p1 = make_entries("p1", 10)
        merger = StreamingMerger()
        results = []
        async for entry in merger.merge_stream([p1], sort_order="desc", limit=3):
            results.append(entry)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_stream_empty(self):
        merger = StreamingMerger()
        results = []
        async for entry in merger.merge_stream([[], []], sort_order="desc"):
            results.append(entry)
        assert results == []

    @pytest.mark.asyncio
    async def test_stream_ascending(self):
        p1 = make_entries("p1", 5, offset_minutes=10)
        p1.reverse()  # sort asc
        merger = StreamingMerger()
        results = []
        async for entry in merger.merge_stream([p1], sort_order="asc"):
            results.append(entry)
        for i in range(len(results) - 1):
            assert results[i].timestamp <= results[i + 1].timestamp
