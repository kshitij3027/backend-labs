"""Tests for the MetricStore in-memory time-series storage."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.models import MetricPoint
from src.storage import MetricStore


def _make_point(
    node_id: str = "node-1",
    metric_name: str = "cpu_usage",
    value: float = 50.0,
    ts: datetime | None = None,
) -> MetricPoint:
    if ts is None:
        ts = datetime.now(timezone.utc)
    return MetricPoint(
        timestamp=ts,
        node_id=node_id,
        metric_name=metric_name,
        value=value,
    )


class TestStoreAndRetrieve:
    def test_store_and_retrieve_single_point(self, metric_store: MetricStore) -> None:
        point = _make_point(value=42.0)
        metric_store.store([point])

        latest = metric_store.get_latest("node-1", "cpu_usage")
        assert latest is not None
        assert latest.value == 42.0
        assert latest.node_id == "node-1"

    def test_get_latest_returns_most_recent(self, metric_store: MetricStore) -> None:
        base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        points = [
            _make_point(value=10.0, ts=base),
            _make_point(value=20.0, ts=base + timedelta(seconds=5)),
            _make_point(value=30.0, ts=base + timedelta(seconds=10)),
        ]
        metric_store.store(points)

        latest = metric_store.get_latest("node-1", "cpu_usage")
        assert latest is not None
        assert latest.value == 30.0

    def test_get_latest_returns_none_for_empty(self, metric_store: MetricStore) -> None:
        assert metric_store.get_latest("node-99", "nonexistent") is None


class TestGetRange:
    def test_filters_correctly_by_timestamp(
        self, metric_store: MetricStore, sample_points: list[MetricPoint]
    ) -> None:
        metric_store.store(sample_points)

        # sample_points go from 2025-01-01T00:00:00 to +45 seconds
        start = datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, 0, 0, 30, tzinfo=timezone.utc)

        result = metric_store.get_range("node-1", "cpu_usage", start, end)

        # Points at seconds 10, 15, 20, 25, 30 → indices 2, 3, 4, 5, 6
        assert len(result) == 5
        for point in result:
            assert start <= point.timestamp <= end

    def test_get_range_empty_when_no_match(self, metric_store: MetricStore) -> None:
        point = _make_point(
            ts=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc), value=10.0
        )
        metric_store.store([point])

        result = metric_store.get_range(
            "node-1",
            "cpu_usage",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        assert result == []


class TestGetAllInWindow:
    def test_returns_only_recent_points(self, metric_store: MetricStore) -> None:
        now = datetime.now(timezone.utc)
        old_point = _make_point(value=10.0, ts=now - timedelta(seconds=600))
        recent_point = _make_point(value=20.0, ts=now - timedelta(seconds=2))

        metric_store.store([old_point, recent_point])

        result = metric_store.get_all_in_window(window_seconds=60.0)
        assert len(result) == 1
        assert result[0].value == 20.0

    def test_returns_points_across_series(self, metric_store: MetricStore) -> None:
        now = datetime.now(timezone.utc)
        p1 = _make_point(node_id="node-1", metric_name="cpu", value=50.0, ts=now)
        p2 = _make_point(node_id="node-2", metric_name="memory", value=70.0, ts=now)

        metric_store.store([p1, p2])

        result = metric_store.get_all_in_window(window_seconds=10.0)
        assert len(result) == 2


class TestRingBufferEviction:
    def test_evicts_oldest_when_full(self) -> None:
        store = MetricStore(max_points_per_series=5)
        base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        points = [
            _make_point(value=float(i), ts=base + timedelta(seconds=i))
            for i in range(10)
        ]
        store.store(points)

        # Should retain only the last 5
        assert store.point_count() == 5
        latest = store.get_latest("node-1", "cpu_usage")
        assert latest is not None
        assert latest.value == 9.0

        # Oldest retained should be value=5.0
        all_points = store.get_range(
            "node-1",
            "cpu_usage",
            base,
            base + timedelta(seconds=100),
        )
        assert len(all_points) == 5
        assert all_points[0].value == 5.0


class TestMetadata:
    def test_get_node_ids(self, metric_store: MetricStore) -> None:
        p1 = _make_point(node_id="node-1", metric_name="cpu")
        p2 = _make_point(node_id="node-2", metric_name="cpu")
        p3 = _make_point(node_id="node-3", metric_name="memory")

        metric_store.store([p1, p2, p3])
        assert metric_store.get_node_ids() == {"node-1", "node-2", "node-3"}

    def test_get_metric_names(self, metric_store: MetricStore) -> None:
        p1 = _make_point(node_id="node-1", metric_name="cpu")
        p2 = _make_point(node_id="node-1", metric_name="memory")
        p3 = _make_point(node_id="node-2", metric_name="disk_io")

        metric_store.store([p1, p2, p3])
        assert metric_store.get_metric_names() == {"cpu", "memory", "disk_io"}

    def test_point_count(
        self, metric_store: MetricStore, sample_points: list[MetricPoint]
    ) -> None:
        assert metric_store.point_count() == 0
        metric_store.store(sample_points)
        assert metric_store.point_count() == 10

        # Add more for a different series
        extra = _make_point(node_id="node-2", metric_name="memory", value=60.0)
        metric_store.store([extra])
        assert metric_store.point_count() == 11
