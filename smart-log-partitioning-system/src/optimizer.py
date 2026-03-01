"""Query optimizer that prunes irrelevant partitions for faster queries."""

from src.router import PartitionRouter
from src.manager import PartitionManager


class QueryOptimizer:
    """Optimizes queries by pruning partitions that can't contain matching results."""

    def __init__(self, router: PartitionRouter, manager: PartitionManager):
        self.router = router
        self.manager = manager
        self._query_history = []  # Track optimization results for metrics

    def optimize(self, query: dict) -> dict:
        """Analyze a query and determine which partitions to scan.

        Args:
            query: dict with optional keys:
                - source: str — filter by source
                - level: str — filter by level
                - time_range: dict with "start" and "end" (ISO format)

        Returns:
            dict with:
                - partition_ids: list[str] — partitions to scan
                - total_partitions: int — total partitions in system
                - partitions_scanned: int — number we'll actually scan
                - pruned: int — number of partitions pruned
                - improvement_factor: float — total / scanned (1.0 = no improvement)
        """
        all_partitions = self.manager.get_all_partition_ids()
        total = len(all_partitions)

        if total == 0:
            result = {
                "partition_ids": [],
                "total_partitions": 0,
                "partitions_scanned": 0,
                "pruned": 0,
                "improvement_factor": 1.0,
            }
            self._query_history.append(result)
            return result

        # Start with all partitions
        candidate_ids = set(all_partitions)
        strategy = self.router.config.strategy

        # Prune by source if source filter provided
        source = query.get("source")
        if source:
            if strategy == "source":
                # Source strategy: hash to single partition
                source_partitions = set(self.router.get_all_partition_ids_for_source(source))
                candidate_ids &= source_partitions
            elif strategy == "hybrid":
                # Hybrid: filter partitions whose source component matches
                source_part = self.router.get_partition_id_for_source(source)
                candidate_ids = {pid for pid in candidate_ids if pid.split("_", 1)[0] == source_part}

        # Prune by time range if provided
        time_range = query.get("time_range")
        if time_range and strategy in ("time", "hybrid"):
            time_buckets = set(self.router.get_partition_ids_for_time_range(
                time_range["start"], time_range["end"]
            ))
            if strategy == "time":
                candidate_ids &= time_buckets
            elif strategy == "hybrid":
                # For hybrid, match the time component of each partition ID
                candidate_ids = {
                    pid for pid in candidate_ids
                    if "_".join(pid.split("_")[1:]) in time_buckets
                }

        scanned = len(candidate_ids)
        pruned = total - scanned
        improvement = total / scanned if scanned > 0 else 1.0

        result = {
            "partition_ids": sorted(candidate_ids),
            "total_partitions": total,
            "partitions_scanned": scanned,
            "pruned": pruned,
            "improvement_factor": round(improvement, 2),
        }
        self._query_history.append(result)
        return result

    def get_efficiency_metrics(self) -> dict:
        """Get aggregate efficiency metrics across all queries.

        Returns:
            dict with:
                - total_queries: int
                - avg_improvement_factor: float
                - avg_partitions_scanned_pct: float (percentage)
                - total_partitions_pruned: int
        """
        if not self._query_history:
            return {
                "total_queries": 0,
                "avg_improvement_factor": 0.0,
                "avg_partitions_scanned_pct": 0.0,
                "total_partitions_pruned": 0,
            }

        total_queries = len(self._query_history)
        avg_improvement = sum(r["improvement_factor"] for r in self._query_history) / total_queries

        scanned_pcts = []
        for r in self._query_history:
            if r["total_partitions"] > 0:
                scanned_pcts.append(r["partitions_scanned"] / r["total_partitions"] * 100)
            else:
                scanned_pcts.append(0.0)
        avg_scanned_pct = sum(scanned_pcts) / len(scanned_pcts)

        total_pruned = sum(r["pruned"] for r in self._query_history)

        return {
            "total_queries": total_queries,
            "avg_improvement_factor": round(avg_improvement, 2),
            "avg_partitions_scanned_pct": round(avg_scanned_pct, 2),
            "total_partitions_pruned": total_pruned,
        }
