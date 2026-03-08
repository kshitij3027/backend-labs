#!/usr/bin/env python3
"""Load testing script for the cluster performance monitoring system.

Tests:
1. MetricStore insertion throughput (target: 10K+ points/sec)
2. Memory footprint after 100K points (target: <100MB)
3. Aggregation speed over 50K points (target: <500ms)
"""

import sys
import time
import tracemalloc

# Add parent directory to path so we can import src
sys.path.insert(0, ".")

from datetime import datetime, timezone
from src.models import MetricPoint, NodeInfo
from src.storage import MetricStore
from src.aggregator import MetricAggregator

PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} — {detail}")

def generate_points(count: int) -> list[MetricPoint]:
    """Generate a batch of test metric points."""
    now = datetime.now(timezone.utc)
    metrics = ["cpu_usage", "memory_usage", "throughput", "write_latency", "read_latency"]
    nodes = ["node-1", "node-2", "node-3"]
    points = []
    for i in range(count):
        points.append(MetricPoint(
            timestamp=now,
            node_id=nodes[i % len(nodes)],
            metric_name=metrics[i % len(metrics)],
            value=float(i % 100),
        ))
    return points

def test_insertion_throughput():
    """Test MetricStore can handle 10K+ insertions per second."""
    print("\n1. Insertion Throughput")
    store = MetricStore(max_points_per_series=200000)
    points = generate_points(100_000)

    start = time.perf_counter()
    store.store(points)
    elapsed = time.perf_counter() - start

    rate = len(points) / elapsed
    print(f"     100K points stored in {elapsed:.3f}s ({rate:,.0f} points/sec)")
    check(f"Insertion rate >= 10,000/sec", rate >= 10_000, f"got {rate:,.0f}/sec")
    return store

def test_memory_footprint():
    """Test memory stays under 100MB for 100K points."""
    print("\n2. Memory Footprint")
    tracemalloc.start()

    store = MetricStore(max_points_per_series=200000)
    points = generate_points(100_000)
    store.store(points)

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak / (1024 * 1024)
    print(f"     Peak memory: {peak_mb:.1f} MB")
    check(f"Peak memory < 100 MB", peak_mb < 100, f"got {peak_mb:.1f} MB")

def test_aggregation_speed():
    """Test aggregation over 50K points completes in <500ms."""
    print("\n3. Aggregation Speed")
    store = MetricStore(max_points_per_series=200000)
    points = generate_points(50_000)
    store.store(points)

    aggregator = MetricAggregator(store, window_seconds=86400)

    start = time.perf_counter()
    stats = aggregator.get_all_node_stats()
    elapsed = time.perf_counter() - start

    elapsed_ms = elapsed * 1000
    print(f"     Aggregated {len(stats)} series in {elapsed_ms:.1f}ms")
    check(f"Aggregation < 500ms", elapsed_ms < 500, f"got {elapsed_ms:.1f}ms")

def main():
    print("=" * 50)
    print("Cluster Performance Monitor - Load Test")
    print("=" * 50)

    test_insertion_throughput()
    test_memory_footprint()
    test_aggregation_speed()

    total = PASS + FAIL
    print(f"\n{'='*50}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'='*50}")

    sys.exit(0 if FAIL == 0 else 1)

if __name__ == "__main__":
    main()
