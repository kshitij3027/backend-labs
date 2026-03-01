"""Partition manager for storing and querying log entries across partitions."""

import hashlib
import json
import os
from collections import defaultdict

from src.config import PartitionConfig


class BloomFilter:
    """Space-efficient probabilistic set membership test.

    Uses multiple hash functions (MD5-based) for low false-positive rates.
    No false negatives guaranteed.
    """

    def __init__(self, size: int = 1000, num_hashes: int = 3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [False] * size

    def _hashes(self, item: str) -> list[int]:
        """Generate multiple hash positions for an item."""
        positions = []
        for i in range(self.num_hashes):
            h = hashlib.md5(f"{item}_{i}".encode()).hexdigest()
            positions.append(int(h, 16) % self.size)
        return positions

    def add(self, item: str) -> None:
        """Add an item to the bloom filter."""
        for pos in self._hashes(item):
            self.bit_array[pos] = True

    def might_contain(self, item: str) -> bool:
        """Check if an item might be in the set. No false negatives."""
        return all(self.bit_array[pos] for pos in self._hashes(item))

    @property
    def fill_ratio(self) -> float:
        """Fraction of bits set to True."""
        return sum(self.bit_array) / self.size


class PartitionManager:
    """Manages storage, retrieval, and statistics for partitioned log entries."""

    def __init__(self, config: PartitionConfig):
        self.config = config
        self.partitions = defaultdict(list)  # partition_id -> [entries]
        self.bloom_filters = defaultdict(lambda: BloomFilter())  # partition_id -> BloomFilter
        os.makedirs(config.data_dir, exist_ok=True)

    def store(self, partition_id: str, entry: dict) -> None:
        """Store a log entry in the specified partition (memory + JSONL file)."""
        self.partitions[partition_id].append(entry)
        self.bloom_filters[partition_id].add(entry.get("source", ""))
        self._append_to_file(partition_id, entry)

    def _append_to_file(self, partition_id: str, entry: dict) -> None:
        """Append a single entry to the partition's JSONL file."""
        filepath = os.path.join(self.config.data_dir, f"partition_{partition_id}.jsonl")
        with open(filepath, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def query(self, partition_ids: list[str], filters: dict | None = None) -> list[dict]:
        """Query entries from specified partitions, applying optional filters.

        Filters can include:
        - source: str — filter by source field
        - level: str — filter by level field
        - time_range: dict with "start" and "end" (ISO format strings)
        """
        results = []
        filters = filters or {}

        for pid in partition_ids:
            for entry in self.partitions.get(pid, []):
                if self._matches_filters(entry, filters):
                    results.append(entry)

        return results

    def query_with_bloom(self, partition_ids: list[str], filters: dict | None = None) -> list[dict]:
        """Query with bloom filter pre-filtering for source-based queries."""
        filters = filters or {}
        source = filters.get("source")

        if source:
            # Use bloom filters to skip partitions that definitely don't have this source
            filtered_pids = [
                pid for pid in partition_ids
                if pid in self.bloom_filters and self.bloom_filters[pid].might_contain(source)
            ]
        else:
            filtered_pids = partition_ids

        return self.query(filtered_pids, filters)

    def _matches_filters(self, entry: dict, filters: dict) -> bool:
        """Check if an entry matches all provided filters."""
        if "source" in filters and entry.get("source") != filters["source"]:
            return False
        if "level" in filters and entry.get("level") != filters["level"]:
            return False
        if "time_range" in filters:
            ts = entry.get("timestamp", "")
            start = filters["time_range"].get("start", "")
            end = filters["time_range"].get("end", "")
            if ts < start or ts > end:
                return False
        return True

    def get_stats(self) -> dict:
        """Get partition statistics including counts, variance, and hotspot detection."""
        counts = {pid: len(entries) for pid, entries in self.partitions.items()}
        total = sum(counts.values())
        num_partitions = len(counts)

        if num_partitions == 0:
            return {
                "total_entries": 0,
                "num_partitions": 0,
                "partitions": {},
                "variance_pct": 0.0,
                "hotspots": [],
                "bloom_filters": {},
            }

        avg = total / num_partitions

        # Calculate variance as percentage (coefficient of variation)
        if avg > 0:
            variance = sum((c - avg) ** 2 for c in counts.values()) / num_partitions
            std_dev = variance ** 0.5
            variance_pct = (std_dev / avg) * 100
        else:
            variance_pct = 0.0

        # Detect hotspots (partitions with > 1.5x average)
        hotspots = [pid for pid, c in counts.items() if c > avg * 1.5]

        return {
            "total_entries": total,
            "num_partitions": num_partitions,
            "partitions": counts,
            "variance_pct": round(variance_pct, 2),
            "hotspots": hotspots,
            "bloom_filters": {
                pid: {"fill_ratio": round(bf.fill_ratio, 4)}
                for pid, bf in self.bloom_filters.items()
            },
        }

    def get_all_partition_ids(self) -> list[str]:
        """Return all partition IDs that have data."""
        return list(self.partitions.keys())

    def load_from_disk(self) -> None:
        """Load all partition data from JSONL files on disk."""
        self.partitions.clear()
        self.bloom_filters.clear()
        if not os.path.exists(self.config.data_dir):
            return

        for filename in os.listdir(self.config.data_dir):
            if filename.startswith("partition_") and filename.endswith(".jsonl"):
                # Extract partition ID from filename: partition_{id}.jsonl
                pid = filename[len("partition_"):-len(".jsonl")]
                filepath = os.path.join(self.config.data_dir, filename)
                with open(filepath, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            self.partitions[pid].append(json.loads(line))

        # Rebuild bloom filters from loaded data
        for pid, entries in self.partitions.items():
            for entry in entries:
                self.bloom_filters[pid].add(entry.get("source", ""))

    def clear(self) -> None:
        """Clear all in-memory data and remove JSONL files."""
        self.partitions.clear()
        self.bloom_filters.clear()
        if os.path.exists(self.config.data_dir):
            for filename in os.listdir(self.config.data_dir):
                if filename.endswith(".jsonl"):
                    os.remove(os.path.join(self.config.data_dir, filename))
