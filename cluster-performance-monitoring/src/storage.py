"""In-memory time-series metric storage backed by fixed-size ring buffers."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone

from src.models import MetricPoint


class MetricStore:
    """Thread-safe in-memory storage for metric time-series data.

    Each unique ``(node_id, metric_name)`` pair gets its own deque with a
    fixed maximum length, acting as a ring buffer that automatically evicts
    the oldest points when full.
    """

    def __init__(self, max_points_per_series: int = 17280) -> None:
        """Initialise the store.

        Args:
            max_points_per_series: Maximum number of points retained per
                ``(node_id, metric_name)`` series.  Default is
                ``86400 / 5 = 17280`` (24 hours at 5-second intervals).
        """
        self._max_points = max_points_per_series
        self._data: dict[tuple[str, str], deque[MetricPoint]] = {}

    def _key(self, node_id: str, metric_name: str) -> tuple[str, str]:
        return (node_id, metric_name)

    def _get_or_create_series(self, node_id: str, metric_name: str) -> deque[MetricPoint]:
        key = self._key(node_id, metric_name)
        if key not in self._data:
            self._data[key] = deque(maxlen=self._max_points)
        return self._data[key]

    def store(self, points: list[MetricPoint]) -> None:
        """Append metric points to their respective series buffers.

        Args:
            points: List of metric points to store.
        """
        for point in points:
            series = self._get_or_create_series(point.node_id, point.metric_name)
            series.append(point)

    def get_latest(self, node_id: str, metric_name: str) -> MetricPoint | None:
        """Return the most recent point for a series, or ``None`` if empty.

        Args:
            node_id: The cluster node identifier.
            metric_name: The metric name.

        Returns:
            The latest ``MetricPoint`` or ``None``.
        """
        key = self._key(node_id, metric_name)
        series = self._data.get(key)
        if not series:
            return None
        return series[-1]

    def get_range(
        self,
        node_id: str,
        metric_name: str,
        start: datetime,
        end: datetime,
    ) -> list[MetricPoint]:
        """Return all points in a series within the given time range (inclusive).

        Args:
            node_id: The cluster node identifier.
            metric_name: The metric name.
            start: Start of the time range (inclusive).
            end: End of the time range (inclusive).

        Returns:
            List of matching ``MetricPoint`` objects ordered by timestamp.
        """
        key = self._key(node_id, metric_name)
        series = self._data.get(key)
        if not series:
            return []
        return [p for p in series if start <= p.timestamp <= end]

    def get_all_in_window(self, window_seconds: float) -> list[MetricPoint]:
        """Return all points across every series within the last *window_seconds*.

        Args:
            window_seconds: How far back from *now* to look.

        Returns:
            List of ``MetricPoint`` objects from all series.
        """
        cutoff = datetime.now(timezone.utc).timestamp() - window_seconds
        result: list[MetricPoint] = []
        for series in self._data.values():
            for point in series:
                ts = point.timestamp.timestamp()
                if ts >= cutoff:
                    result.append(point)
        return result

    def get_node_ids(self) -> set[str]:
        """Return the set of all node IDs that have stored data."""
        return {key[0] for key in self._data}

    def get_metric_names(self) -> set[str]:
        """Return the set of all metric names that have stored data."""
        return {key[1] for key in self._data}

    def point_count(self) -> int:
        """Return the total number of stored points across all series."""
        return sum(len(series) for series in self._data.values())
