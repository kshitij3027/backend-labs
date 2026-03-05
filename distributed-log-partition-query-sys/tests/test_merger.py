from datetime import datetime, timedelta, timezone

from src.coordinator.merger import ResultMerger
from src.models import LogEntry


def make_entries(partition_id: str, timestamps: list[datetime]) -> list[LogEntry]:
    """Helper to create entries with given timestamps."""
    return [
        LogEntry(
            timestamp=ts,
            level="INFO",
            service="test",
            message=f"msg-{i}",
            partition_id=partition_id,
        )
        for i, ts in enumerate(timestamps)
    ]


class TestResultMerger:
    def setup_method(self):
        self.merger = ResultMerger(max_merge_size=1000)
        self.now = datetime.now(tz=timezone.utc)

    def test_merge_two_partitions_desc(self):
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in [0, 2, 4]]
        )
        p2 = make_entries(
            "p2", [self.now - timedelta(minutes=i) for i in [1, 3, 5]]
        )
        results = self.merger.merge([p1, p2], sort_order="desc")
        timestamps = [r.timestamp for r in results]
        for i in range(len(timestamps) - 1):
            assert timestamps[i] >= timestamps[i + 1]
        assert len(results) == 6

    def test_merge_two_partitions_asc(self):
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in [4, 2, 0]]
        )
        p2 = make_entries(
            "p2", [self.now - timedelta(minutes=i) for i in [5, 3, 1]]
        )
        results = self.merger.merge([p1, p2], sort_order="asc")
        timestamps = [r.timestamp for r in results]
        for i in range(len(timestamps) - 1):
            assert timestamps[i] <= timestamps[i + 1]

    def test_early_termination_with_limit(self):
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in range(0, 20, 2)]
        )
        p2 = make_entries(
            "p2", [self.now - timedelta(minutes=i) for i in range(1, 20, 2)]
        )
        results = self.merger.merge([p1, p2], sort_order="desc", limit=5)
        assert len(results) == 5
        timestamps = [r.timestamp for r in results]
        for i in range(len(timestamps) - 1):
            assert timestamps[i] >= timestamps[i + 1]

    def test_empty_partitions(self):
        results = self.merger.merge([[], []])
        assert results == []

    def test_one_empty_partition(self):
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in [0, 1, 2]]
        )
        results = self.merger.merge([p1, []], sort_order="desc")
        assert len(results) == 3

    def test_single_partition(self):
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in [0, 1, 2]]
        )
        results = self.merger.merge([p1], sort_order="desc")
        assert len(results) == 3

    def test_max_merge_size(self):
        merger = ResultMerger(max_merge_size=3)
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in range(10)]
        )
        results = merger.merge([p1], sort_order="desc")
        assert len(results) == 3

    def test_three_partitions(self):
        p1 = make_entries(
            "p1", [self.now - timedelta(minutes=i) for i in [0, 3, 6]]
        )
        p2 = make_entries(
            "p2", [self.now - timedelta(minutes=i) for i in [1, 4, 7]]
        )
        p3 = make_entries(
            "p3", [self.now - timedelta(minutes=i) for i in [2, 5, 8]]
        )
        results = self.merger.merge([p1, p2, p3], sort_order="desc")
        assert len(results) == 9
        timestamps = [r.timestamp for r in results]
        for i in range(len(timestamps) - 1):
            assert timestamps[i] >= timestamps[i + 1]
