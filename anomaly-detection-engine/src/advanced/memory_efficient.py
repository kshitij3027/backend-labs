"""Memory-efficient pattern storage using HyperLogLog and Count-Min Sketch.

HyperLogLog (from datasketch) provides approximate cardinality counting for
unique IPs, user agents, and paths.  Count-Min Sketch (custom numpy-based
implementation) provides approximate frequency counting for IPs and paths.

Both structures use fixed memory regardless of the number of items stored,
making them ideal for high-throughput log processing.
"""
from __future__ import annotations

import threading

import numpy as np
from datasketch import HyperLogLog


class CountMinSketch:
    """A probabilistic data structure for approximate frequency counting.

    Uses *depth* independent hash functions over a table of shape
    ``(depth, width)``.  Insertions increment all ``depth`` rows;
    queries return the **minimum** across those rows (which is the
    tightest upper-bound estimate).

    Args:
        width:  Number of columns (counters per hash function).
        depth:  Number of hash functions (rows).
        seed:   Base seed used to derive per-row hash parameters.
    """

    def __init__(self, width: int = 1000, depth: int = 5, seed: int = 42) -> None:
        self._width = width
        self._depth = depth
        self._table: np.ndarray = np.zeros((depth, width), dtype=np.int64)

        # Derive (a, b) pairs for each hash function using a seeded RNG.
        rng = np.random.RandomState(seed)
        self._hash_params: list[tuple[int, int]] = [
            (int(rng.randint(1, 2**31 - 1)), int(rng.randint(0, 2**31 - 1)))
            for _ in range(depth)
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, item: str, count: int = 1) -> None:
        """Add *count* occurrences of *item* to the sketch."""
        h = hash(item)
        for i, (a, b) in enumerate(self._hash_params):
            idx = (a * h + b) % self._width
            self._table[i, idx] += count

    def query(self, item: str) -> int:
        """Return the estimated frequency of *item* (never under-counts)."""
        h = hash(item)
        estimates: list[int] = []
        for i, (a, b) in enumerate(self._hash_params):
            idx = (a * h + b) % self._width
            estimates.append(int(self._table[i, idx]))
        return min(estimates)

    def get_memory_bytes(self) -> int:
        """Return the number of bytes used by the internal table."""
        return int(self._table.nbytes)


class PatternStore:
    """Thread-safe store combining HyperLogLog and Count-Min Sketch.

    Tracks approximate cardinality (unique count) of IPs, user agents,
    and paths via HyperLogLog, and approximate frequency of individual
    IPs and paths via Count-Min Sketch.
    """

    def __init__(self) -> None:
        # HyperLogLog instances for cardinality estimation
        self._ip_hll = HyperLogLog(p=8)
        self._ua_hll = HyperLogLog(p=8)
        self._path_hll = HyperLogLog(p=8)

        # Count-Min Sketch instances for frequency estimation
        self._ip_cms = CountMinSketch()
        self._path_cms = CountMinSketch()

        self._total_patterns: int = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_pattern(self, ip: str, user_agent: str, path: str) -> None:
        """Record a single observed pattern (thread-safe)."""
        with self._lock:
            # Update cardinality sketches
            self._ip_hll.update(ip.encode("utf-8"))
            self._ua_hll.update(user_agent.encode("utf-8"))
            self._path_hll.update(path.encode("utf-8"))

            # Update frequency sketches
            self._ip_cms.add(ip)
            self._path_cms.add(path)

            self._total_patterns += 1

    def get_unique_ip_count(self) -> int:
        """Return the estimated number of unique IPs observed."""
        return int(self._ip_hll.count())

    def get_unique_ua_count(self) -> int:
        """Return the estimated number of unique user agents observed."""
        return int(self._ua_hll.count())

    def get_unique_path_count(self) -> int:
        """Return the estimated number of unique paths observed."""
        return int(self._path_hll.count())

    def get_ip_frequency(self, ip: str) -> int:
        """Return the estimated frequency of a specific IP."""
        return self._ip_cms.query(ip)

    def get_path_frequency(self, path: str) -> int:
        """Return the estimated frequency of a specific path."""
        return self._path_cms.query(path)

    def get_memory_usage(self) -> int:
        """Return total estimated memory usage in bytes.

        Includes both CMS tables plus a rough estimate for the HLL
        internal registers (each HLL with p=8 uses 2^8 = 256 registers).
        """
        cms_bytes = self._ip_cms.get_memory_bytes() + self._path_cms.get_memory_bytes()
        # Each HLL with p=8 has 2^8 = 256 registers, ~8 bytes each
        hll_bytes = 3 * 256 * 8
        return cms_bytes + hll_bytes

    def get_stats(self) -> dict:
        """Return a summary dict suitable for inclusion in engine stats."""
        return {
            "unique_ips": self.get_unique_ip_count(),
            "unique_user_agents": self.get_unique_ua_count(),
            "unique_paths": self.get_unique_path_count(),
            "total_patterns": self._total_patterns,
            "memory_usage_bytes": self.get_memory_usage(),
        }
