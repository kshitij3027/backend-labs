"""Benchmark: compare compression algorithms and levels on sample log data."""

import json
import time
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.compression import CompressionHandler
from src.models import create_log_entry, entry_to_dict


def generate_sample_logs(count: int = 1000) -> bytes:
    """Generate sample log data as a JSON array."""
    entries = []
    for i in range(count):
        entry = create_log_entry(
            message=f"Processing request #{i} for user_{i % 100}",
            level=["INFO", "DEBUG", "WARNING", "ERROR"][i % 4],
            service=f"service-{i % 5}",
            metadata={"request_id": f"req-{i:06d}", "duration_ms": i * 1.5},
        )
        entries.append(entry_to_dict(entry))
    return json.dumps(entries).encode("utf-8")


def run_benchmark():
    data = generate_sample_logs(1000)
    original_size = len(data)

    print(f"\nBenchmark: {len(data):,} bytes of sample log data (1000 entries)")
    print(f"{'Algorithm':<10} {'Level':<7} {'Original':<12} {'Compressed':<12} {'Ratio':<8} {'Time (ms)':<10}")
    print("-" * 65)

    for algorithm in ("gzip", "zlib"):
        for level in range(1, 10):
            handler = CompressionHandler(algorithm=algorithm, level=level, bypass_threshold=0)

            # Average over 3 runs
            total_time = 0
            result = None
            for _ in range(3):
                result = handler.compress(data)
                total_time += result.time_ms

            avg_time = total_time / 3
            print(
                f"{algorithm:<10} {level:<7} {original_size:<12,} {result.compressed_size:<12,} "
                f"{result.ratio:<8.2f} {avg_time:<10.2f}"
            )

    print()


if __name__ == "__main__":
    run_benchmark()
