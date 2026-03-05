from src.models import PartitionInfo, Query, TimeRange


class PartitionMap:
    def __init__(self):
        self._partitions: dict[str, PartitionInfo] = {}

    def register(self, info: PartitionInfo) -> None:
        """Register or update a partition."""
        self._partitions[info.partition_id] = info

    def get_all(self) -> list[PartitionInfo]:
        return list(self._partitions.values())

    def get_healthy(self) -> list[PartitionInfo]:
        return [p for p in self._partitions.values() if p.healthy]

    def get_relevant_partitions(self, query: Query) -> list[PartitionInfo]:
        """Get partitions relevant to this query based on health and time-range overlap."""
        healthy = self.get_healthy()

        if not query.time_range:
            return healthy

        relevant = []
        for p in healthy:
            if p.time_range is None:
                # Unknown time range -- include to be safe
                relevant.append(p)
                continue
            # Check overlap: query range overlaps partition range
            if self._ranges_overlap(query.time_range, p.time_range):
                relevant.append(p)

        return relevant

    @staticmethod
    def _ranges_overlap(query_range: TimeRange, partition_range: TimeRange) -> bool:
        """Check if two time ranges overlap."""
        return (
            query_range.start <= partition_range.end
            and query_range.end >= partition_range.start
        )

    def mark_healthy(self, partition_id: str) -> None:
        if partition_id in self._partitions:
            self._partitions[partition_id].healthy = True

    def mark_unhealthy(self, partition_id: str) -> None:
        if partition_id in self._partitions:
            self._partitions[partition_id].healthy = False

    @property
    def total_count(self) -> int:
        return len(self._partitions)

    @property
    def healthy_count(self) -> int:
        return len(self.get_healthy())
