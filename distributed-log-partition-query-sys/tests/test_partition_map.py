from datetime import datetime, timedelta, timezone

from src.models import PartitionInfo, Query, TimeRange
from src.coordinator.partition_map import PartitionMap


class TestPartitionMap:
    def setup_method(self):
        self.pm = PartitionMap()
        self.now = datetime.now(tz=timezone.utc)

    def _make_partition(
        self, pid: str, healthy: bool = True, days_back: int = 7
    ) -> PartitionInfo:
        return PartitionInfo(
            partition_id=pid,
            url=f"http://{pid}:8081",
            healthy=healthy,
            time_range=TimeRange(
                start=self.now - timedelta(days=days_back),
                end=self.now,
            ),
            log_count=1000,
        )

    def test_register_and_get_all(self):
        self.pm.register(self._make_partition("p1"))
        self.pm.register(self._make_partition("p2"))
        assert self.pm.total_count == 2
        assert len(self.pm.get_all()) == 2

    def test_get_healthy_excludes_unhealthy(self):
        self.pm.register(self._make_partition("p1", healthy=True))
        self.pm.register(self._make_partition("p2", healthy=False))
        healthy = self.pm.get_healthy()
        assert len(healthy) == 1
        assert healthy[0].partition_id == "p1"

    def test_mark_unhealthy(self):
        self.pm.register(self._make_partition("p1"))
        self.pm.mark_unhealthy("p1")
        assert self.pm.healthy_count == 0

    def test_mark_healthy(self):
        self.pm.register(self._make_partition("p1", healthy=False))
        self.pm.mark_healthy("p1")
        assert self.pm.healthy_count == 1

    def test_relevant_partitions_no_time_range(self):
        self.pm.register(self._make_partition("p1"))
        self.pm.register(self._make_partition("p2"))
        query = Query(limit=10)  # no time_range
        relevant = self.pm.get_relevant_partitions(query)
        assert len(relevant) == 2

    def test_relevant_partitions_with_overlap(self):
        self.pm.register(self._make_partition("p1", days_back=7))
        self.pm.register(self._make_partition("p2", days_back=7))
        query = Query(
            time_range=TimeRange(
                start=self.now - timedelta(days=3),
                end=self.now,
            )
        )
        relevant = self.pm.get_relevant_partitions(query)
        assert len(relevant) == 2

    def test_relevant_partitions_no_overlap(self):
        self.pm.register(self._make_partition("p1", days_back=7))
        query = Query(
            time_range=TimeRange(
                start=self.now - timedelta(days=30),
                end=self.now - timedelta(days=20),
            )
        )
        relevant = self.pm.get_relevant_partitions(query)
        assert len(relevant) == 0

    def test_relevant_excludes_unhealthy(self):
        self.pm.register(self._make_partition("p1", healthy=True))
        self.pm.register(self._make_partition("p2", healthy=False))
        query = Query(limit=10)
        relevant = self.pm.get_relevant_partitions(query)
        assert len(relevant) == 1

    def test_partition_with_no_time_range_included(self):
        """Partitions with unknown time range should be included."""
        self.pm.register(PartitionInfo(partition_id="p1", url="http://p1:8081"))
        query = Query(
            time_range=TimeRange(
                start=self.now - timedelta(days=1),
                end=self.now,
            )
        )
        relevant = self.pm.get_relevant_partitions(query)
        assert len(relevant) == 1
