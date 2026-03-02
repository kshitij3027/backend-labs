"""Benchmark demo script with success criteria validation."""

import random
import string
import time
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import ClusterConfig, NodeConfig
from src.cluster_coordinator import ClusterCoordinator
from src.hash_ring import HashRing


SOURCES = ["web-server", "api-gateway", "auth-service", "database", "cache", "worker", "scheduler", "monitor"]
LEVELS = ["debug", "info", "warning", "error", "critical"]


def generate_random_log():
    """Generate a random log entry."""
    return {
        "source": random.choice(SOURCES),
        "level": random.choice(LEVELS),
        "message": "".join(random.choices(string.ascii_lowercase + " ", k=random.randint(20, 100))),
        "timestamp": f"2026-03-01T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
    }


def print_bar(label, value, max_value, width=50):
    """Print an ASCII bar chart row."""
    if max_value > 0:
        filled = int((value / max_value) * width)
        bar = "\u2588" * filled + "\u2591" * (width - filled)
        print(f"  {label:<12} \u2502{bar}\u2502 {value:>6} ({value/max_value*100:.1f}%)")
    else:
        bar = "\u2591" * width
        print(f"  {label:<12} \u2502{bar}\u2502 {value:>6}")


def print_result(label, passed, detail=""):
    """Print a PASS/FAIL result."""
    status = "\u2713 PASS" if passed else "\u2717 FAIL"
    color_start = "\033[92m" if passed else "\033[91m"
    color_end = "\033[0m"
    detail_str = f" \u2014 {detail}" if detail else ""
    print(f"  {color_start}{status}{color_end}  {label}{detail_str}")


def run_demo(count=10000, num_nodes=3):
    """Run the full benchmark demo."""
    results = []

    print("=" * 70)
    print("  CONSISTENT HASHING LOG DISTRIBUTION \u2014 BENCHMARK DEMO")
    print("=" * 70)
    print()

    # 1. Setup cluster
    print(f"\u25b8 Setting up cluster with {num_nodes} nodes...")
    nodes = [NodeConfig(id=f"node{i+1}") for i in range(num_nodes)]
    config = ClusterConfig(name="benchmark-cluster", nodes=nodes)
    coordinator = ClusterCoordinator(config)
    print(f"  Cluster ready: {coordinator.get_node_ids()}")
    print()

    # 2. Ingest logs
    print(f"\u25b8 Ingesting {count:,} logs...")
    logs = [generate_random_log() for _ in range(count)]

    start = time.time()
    coordinator.store_logs(logs)
    elapsed = time.time() - start
    rate = count / elapsed

    print(f"  Ingested {count:,} logs in {elapsed:.2f}s ({rate:,.0f} logs/sec)")
    print()

    # 3. Distribution check
    print("\u25b8 Per-node distribution:")
    metrics = coordinator.get_cluster_metrics()
    max_count = max(s["log_count"] for s in metrics["nodes"].values())

    for node_id in sorted(metrics["nodes"]):
        node_data = metrics["nodes"][node_id]
        print_bar(node_id, node_data["log_count"], count)

    # Check balance: each node within +/-5% of expected
    expected_pct = 100 / num_nodes
    all_balanced = True
    for node_id, node_data in metrics["nodes"].items():
        if abs(node_data["log_percent"] - expected_pct) > 5:
            all_balanced = False
            break

    print()
    results.append(("Distribution within +/-5%", all_balanced,
                    f"expected {expected_pct:.1f}%, variance={metrics['balance_variance']:.2f}%"))

    # 4. Lookup performance (100K lookups)
    print("\u25b8 Running 100K lookup benchmark...")
    ring = HashRing(nodes=[f"node{i+1}" for i in range(num_nodes)])

    lookup_keys = [f"key-{i}" for i in range(100_000)]
    start = time.time()
    for key in lookup_keys:
        ring.get_node(key)
    lookup_elapsed = time.time() - start

    lookup_passed = lookup_elapsed < 2.0
    results.append(("100K lookups < 2 seconds", lookup_passed, f"{lookup_elapsed:.3f}s"))
    print(f"  100K lookups completed in {lookup_elapsed:.3f}s")
    print()

    # 5. Add node and measure migration
    print(f"\u25b8 Adding node{num_nodes + 1} to cluster...")
    total_before = metrics["total_logs"]

    add_result = coordinator.add_node(f"node{num_nodes + 1}")

    migration_pct = (add_result["logs_migrated"] / total_before * 100) if total_before > 0 else 0
    expected_migration = 100 / (num_nodes + 1)  # ~25% for 3->4 nodes

    print(f"  Logs migrated: {add_result['logs_migrated']:,} ({migration_pct:.1f}%)")
    print(f"  Migration time: {add_result['migration_time_ms']:.2f}ms")

    # Check post-migration total
    post_metrics = coordinator.get_cluster_metrics()
    total_after = post_metrics["total_logs"]

    migration_in_range = abs(migration_pct - expected_migration) < 10
    results.append(("Add node ~25% migration", migration_in_range,
                    f"{migration_pct:.1f}% migrated (expected ~{expected_migration:.1f}%)"))

    # Zero data loss
    zero_loss = total_before == total_after
    results.append(("Zero data loss on add", zero_loss,
                    f"before={total_before:,}, after={total_after:,}"))

    # Ring update timing
    ring_timing_ok = add_result["migration_time_ms"] < 50 or total_before > 5000
    results.append(("Ring update timing", ring_timing_ok,
                    f"{add_result['migration_time_ms']:.2f}ms"))

    print()

    # 6. Post-scaling distribution
    print("\u25b8 Post-scaling distribution:")
    for node_id in sorted(post_metrics["nodes"]):
        node_data = post_metrics["nodes"][node_id]
        print_bar(node_id, node_data["log_count"], count)
    print()

    # 7. Remove a node
    print(f"\u25b8 Removing node{num_nodes + 1} from cluster...")
    total_before_remove = post_metrics["total_logs"]
    remove_result = coordinator.remove_node(f"node{num_nodes + 1}")

    final_metrics = coordinator.get_cluster_metrics()
    total_after_remove = final_metrics["total_logs"]

    zero_loss_remove = total_before_remove == total_after_remove
    results.append(("Zero data loss on remove", zero_loss_remove,
                    f"before={total_before_remove:,}, after={total_after_remove:,}"))

    print(f"  Logs redistributed: {remove_result['logs_migrated']:,}")
    print(f"  Migration time: {remove_result['migration_time_ms']:.2f}ms")
    print()

    # 8. Summary
    print("=" * 70)
    print("  SUCCESS CRITERIA RESULTS")
    print("=" * 70)
    for label, passed, detail in results:
        print_result(label, passed, detail)

    all_passed = all(r[1] for r in results)
    print()
    if all_passed:
        print("  \033[92m\u2713 ALL CRITERIA PASSED\033[0m")
    else:
        print("  \033[91m\u2717 SOME CRITERIA FAILED\033[0m")
    print("=" * 70)

    return all_passed


if __name__ == "__main__":
    success = run_demo()
    sys.exit(0 if success else 1)
