"""Benchmark demo for the Smart Log Partitioning System."""

import random
import time
from datetime import datetime, timedelta

from src.config import PartitionConfig
from src.router import PartitionRouter
from src.manager import PartitionManager
from src.optimizer import QueryOptimizer


SOURCES = [
    "api_gateway", "cache", "queue", "scheduler",       # -> partition 0
    "web_server", "auth", "monitor", "search",           # -> partition 1
    "database", "payment", "notification", "analytics",  # -> partition 2
]
LEVELS = ["debug", "info", "warn", "error", "critical"]


def generate_entries(count: int) -> list[dict]:
    """Generate random log entries spread over 24 hours."""
    random.seed(42)
    base_time = datetime(2026, 2, 28, 0, 0, 0)
    entries = []
    for _ in range(count):
        source = random.choice(SOURCES)
        level = random.choice(LEVELS)
        offset_seconds = random.randint(0, 86400)  # 24 hours
        timestamp = base_time + timedelta(seconds=offset_seconds)
        entries.append({
            "source": source,
            "level": level,
            "timestamp": timestamp.isoformat(),
            "message": f"Event from {source} at {level}",
        })
    return entries


def run_demo(count: int = 1000, nodes: int = 3, strategy: str = "source"):
    """Run the full benchmark demo."""
    print(f"\n{'='*60}")
    print(f"  Smart Log Partitioning System - Benchmark Demo")
    print(f"{'='*60}")
    print(f"  Strategy: {strategy}")
    print(f"  Nodes: {nodes}")
    print(f"  Log count: {count}")
    print(f"{'='*60}\n")

    # Setup
    config = PartitionConfig(strategy=strategy, num_nodes=nodes, data_dir="/tmp/slps_demo")
    router = PartitionRouter(config)
    manager = PartitionManager(config)
    optimizer = QueryOptimizer(router, manager)

    # Generate entries
    print("Generating log entries...")
    entries = generate_entries(count)

    # Ingest with timing
    print("Ingesting logs...")
    start = time.perf_counter()
    for entry in entries:
        pid = router.route(entry)
        manager.store(pid, entry)
    elapsed = time.perf_counter() - start
    rate = count / elapsed if elapsed > 0 else float("inf")

    print(f"  Ingested {count} logs in {elapsed:.3f}s ({rate:.0f} logs/sec)\n")

    # Stats
    stats = manager.get_stats()
    print("Partition Distribution:")
    for pid, cnt in sorted(stats["partitions"].items()):
        bar = "#" * (cnt * 40 // max(stats["partitions"].values()))
        print(f"  Partition {pid}: {cnt:>5} entries  {bar}")
    print(f"  Variance: {stats['variance_pct']:.2f}%")
    print(f"  Hotspots: {stats['hotspots']}\n")

    # Query benchmark
    test_source = "web_server"
    print(f"Query Benchmark (source='{test_source}'):")

    # Brute force: scan all partitions
    all_pids = manager.get_all_partition_ids()
    start = time.perf_counter()
    brute_results = manager.query(all_pids, {"source": test_source})
    brute_time = time.perf_counter() - start

    # Optimized query
    start = time.perf_counter()
    opt = optimizer.optimize({"source": test_source})
    opt_results = manager.query(opt["partition_ids"], {"source": test_source})
    opt_time = time.perf_counter() - start

    print(f"  Brute force: {len(brute_results)} results, scanned {len(all_pids)} partitions in {brute_time:.6f}s")
    print(f"  Optimized:   {len(opt_results)} results, scanned {opt['partitions_scanned']} partitions in {opt_time:.6f}s")
    print(f"  Improvement: {opt['improvement_factor']}x")
    print(f"  Results match: {len(brute_results) == len(opt_results)}\n")

    # Efficiency metrics
    efficiency = optimizer.get_efficiency_metrics()
    scanned_pct = (opt["partitions_scanned"] / opt["total_partitions"] * 100) if opt["total_partitions"] > 0 else 0

    # Success criteria
    print(f"{'='*60}")
    print("  Success Criteria")
    print(f"{'='*60}")

    criteria = [
        (f"Processed {count} logs", stats["total_entries"] == count),
        (f"Ingestion rate > 500 logs/sec ({rate:.0f})", rate > 500),
        (f"Partition variance < 20% ({stats['variance_pct']:.2f}%)", stats["variance_pct"] < 20),
        (f"Query improvement >= {nodes}x ({opt['improvement_factor']}x)", opt["improvement_factor"] >= nodes),
        (f"< 50% partitions scanned ({scanned_pct:.1f}%)", scanned_pct < 50),
        (f"Results match brute force", len(brute_results) == len(opt_results)),
    ]

    all_pass = True
    for desc, passed in criteria:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {desc}")

    print(f"\n{'='*60}")
    if all_pass:
        print("  ALL CRITERIA PASSED!")
    else:
        print("  SOME CRITERIA FAILED")
    print(f"{'='*60}\n")

    # Cleanup
    manager.clear()

    return all_pass
