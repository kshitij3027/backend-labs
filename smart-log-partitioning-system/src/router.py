"""Partition router for directing log entries to the correct partition."""

import hashlib
from datetime import datetime, timedelta

from src.config import PartitionConfig


class PartitionRouter:
    """Routes log entries to partitions based on the configured strategy."""

    def __init__(self, config: PartitionConfig):
        self.config = config

    def route(self, entry: dict) -> str:
        """Route a log entry to a partition based on the configured strategy.

        Args:
            entry: A log entry dict with at least "source" and/or "timestamp" keys.

        Returns:
            A partition identifier string.
        """
        if self.config.strategy == "source":
            return self._route_source(entry)
        elif self.config.strategy == "time":
            return self._route_time(entry)
        elif self.config.strategy == "hybrid":
            return self._route_hybrid(entry)
        else:
            raise ValueError(f"Unknown partition strategy: {self.config.strategy}")

    def _route_source(self, entry: dict) -> str:
        """Route by hashing the source field.

        Returns a string like "0", "1", "2" based on MD5 hash modulo num_nodes.
        """
        source = entry["source"]
        hash_val = int(hashlib.md5(source.encode()).hexdigest(), 16)
        partition = hash_val % self.config.num_nodes
        return str(partition)

    def _route_time(self, entry: dict) -> str:
        """Route by time bucket.

        Parses the ISO format timestamp and buckets by time_bucket_hours.
        Returns format "YYYYMMDD_HH" (e.g., "20260228_14").
        """
        ts = datetime.fromisoformat(entry["timestamp"])
        bucket_hour = (ts.hour // self.config.time_bucket_hours) * self.config.time_bucket_hours
        return ts.strftime("%Y%m%d_") + f"{bucket_hour:02d}"

    def _route_hybrid(self, entry: dict) -> str:
        """Route by combining source and time partitions.

        Returns format "N_YYYYMMDD_HH" (e.g., "0_20260228_14").
        """
        source_part = self._route_source(entry)
        time_part = self._route_time(entry)
        return f"{source_part}_{time_part}"

    def get_partition_id_for_source(self, source: str) -> str:
        """Hash a source string to a partition ID.

        Args:
            source: The source identifier string.

        Returns:
            The partition ID as a string digit.
        """
        hash_val = int(hashlib.md5(source.encode()).hexdigest(), 16)
        return str(hash_val % self.config.num_nodes)

    def get_all_partition_ids_for_source(self, source: str) -> list[str]:
        """Get all partition IDs that could contain data for a given source.

        For source strategy: returns [single_id] since source maps deterministically.
        For time strategy: returns [] since time-based partitions can't be pruned by source.
        For hybrid strategy: returns [source_part] (just the source component).
        """
        if self.config.strategy == "source":
            return [self.get_partition_id_for_source(source)]
        elif self.config.strategy == "time":
            return []
        elif self.config.strategy == "hybrid":
            return [self.get_partition_id_for_source(source)]
        else:
            raise ValueError(f"Unknown partition strategy: {self.config.strategy}")

    def get_partition_ids_for_time_range(self, start: str, end: str) -> list[str]:
        """Get all time bucket partition IDs covering a time range.

        Iterates from start to end in time_bucket_hours increments, returning
        the partition ID for each bucket.

        Args:
            start: ISO format start timestamp.
            end: ISO format end timestamp.

        Returns:
            List of time bucket partition IDs (format "YYYYMMDD_HH").
        """
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        increment = timedelta(hours=self.config.time_bucket_hours)

        # Align start to bucket boundary
        start_hour = (start_dt.hour // self.config.time_bucket_hours) * self.config.time_bucket_hours
        current = start_dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

        partition_ids = []
        while current <= end_dt:
            bucket_hour = (current.hour // self.config.time_bucket_hours) * self.config.time_bucket_hours
            partition_id = current.strftime("%Y%m%d_") + f"{bucket_hour:02d}"
            if partition_id not in partition_ids:
                partition_ids.append(partition_id)
            current += increment

        return partition_ids
