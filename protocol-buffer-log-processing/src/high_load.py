"""Concurrent multi-service high-load simulation.

Spawns one thread per service, each generating logs at the configured rate.
Every log entry is serialized with both JSON and Protobuf so we can compare
throughput, size, and cost under realistic concurrent conditions.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.config import Config
from src.log_generator import SERVICE_NAMES, generate_log_entry
from src.serializer import serialize_json, serialize_protobuf


# ---------------------------------------------------------------------------
# Per-worker simulation
# ---------------------------------------------------------------------------


def _worker(
    service_name: str,
    rate: int,
    duration: int,
    lock: threading.Lock,
    shared_counters: dict[str, Any],
) -> dict[str, Any]:
    """Generate logs for a single service at the target rate.

    Args:
        service_name: The service to simulate.
        rate: Target logs per second.
        duration: How many seconds to run.
        lock: Lock protecting *shared_counters*.
        shared_counters: Mutable dict accumulating aggregate totals.

    Returns:
        Per-service stats dict.
    """
    total_logs = 0
    json_bytes_total = 0
    proto_bytes_total = 0
    json_time_total = 0.0
    proto_time_total = 0.0

    interval = 1.0 / rate if rate > 0 else 0.0
    start = time.perf_counter()
    deadline = start + duration

    while time.perf_counter() < deadline:
        iter_start = time.perf_counter()

        entry = generate_log_entry(service_name=service_name)

        # Serialize as JSON
        t0 = time.perf_counter()
        json_data = serialize_json([entry])
        t1 = time.perf_counter()
        json_time_total += t1 - t0
        json_bytes_total += len(json_data)

        # Serialize as Protobuf
        t0 = time.perf_counter()
        proto_data = serialize_protobuf([entry])
        t1 = time.perf_counter()
        proto_time_total += t1 - t0
        proto_bytes_total += len(proto_data)

        total_logs += 1

        # Throttle to target rate
        elapsed = time.perf_counter() - iter_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    wall_time = time.perf_counter() - start
    achieved_rate = total_logs / wall_time if wall_time > 0 else 0.0

    per_service = {
        "service": service_name,
        "total_logs": total_logs,
        "wall_time_s": wall_time,
        "achieved_rate": achieved_rate,
        "json_bytes": json_bytes_total,
        "json_time_s": json_time_total,
        "proto_bytes": proto_bytes_total,
        "proto_time_s": proto_time_total,
    }

    # Accumulate into shared counters
    with lock:
        shared_counters["total_logs"] += total_logs
        shared_counters["json_bytes"] += json_bytes_total
        shared_counters["json_time_s"] += json_time_total
        shared_counters["proto_bytes"] += proto_bytes_total
        shared_counters["proto_time_s"] += proto_time_total

    return per_service


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_high_load_simulation(config: Config) -> dict[str, Any]:
    """Run a concurrent high-load simulation across all services.

    One thread per service generates logs at ``config.HIGH_SCALE_RATE``
    logs/sec for ``config.HIGH_LOAD_DURATION`` seconds.

    Args:
        config: Application configuration.

    Returns:
        A results dict with ``per_service``, ``aggregate``, and
        ``projections`` keys.
    """
    lock = threading.Lock()
    shared_counters: dict[str, Any] = {
        "total_logs": 0,
        "json_bytes": 0,
        "json_time_s": 0.0,
        "proto_bytes": 0,
        "proto_time_s": 0.0,
    }

    services = SERVICE_NAMES[: config.NUM_SERVICES]
    per_service_results: list[dict[str, Any]] = []

    overall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=len(services)) as executor:
        futures = {
            executor.submit(
                _worker,
                svc,
                config.HIGH_SCALE_RATE,
                config.HIGH_LOAD_DURATION,
                lock,
                shared_counters,
            ): svc
            for svc in services
        }

        for future in as_completed(futures):
            per_service_results.append(future.result())

    overall_wall = time.perf_counter() - overall_start

    # Sort by service name for stable output
    per_service_results.sort(key=lambda r: r["service"])

    # Aggregate stats
    total_logs = shared_counters["total_logs"]
    json_bytes = shared_counters["json_bytes"]
    proto_bytes = shared_counters["proto_bytes"]
    json_time = shared_counters["json_time_s"]
    proto_time = shared_counters["proto_time_s"]

    size_ratio = json_bytes / proto_bytes if proto_bytes > 0 else float("inf")
    speed_ratio = json_time / proto_time if proto_time > 0 else float("inf")
    aggregate_rate = total_logs / overall_wall if overall_wall > 0 else 0.0

    # Cost projections for DAILY_LOG_VOLUME
    daily_volume = config.DAILY_LOG_VOLUME
    avg_json_per_log = json_bytes / total_logs if total_logs > 0 else 0
    avg_proto_per_log = proto_bytes / total_logs if total_logs > 0 else 0
    avg_json_time_per_log = json_time / total_logs if total_logs > 0 else 0
    avg_proto_time_per_log = proto_time / total_logs if total_logs > 0 else 0

    projected_json_gb = (avg_json_per_log * daily_volume) / (1024**3)
    projected_proto_gb = (avg_proto_per_log * daily_volume) / (1024**3)
    projected_json_time_h = (avg_json_time_per_log * daily_volume) / 3600
    projected_proto_time_h = (avg_proto_time_per_log * daily_volume) / 3600

    return {
        "config": {
            "rate_per_service": config.HIGH_SCALE_RATE,
            "duration_s": config.HIGH_LOAD_DURATION,
            "num_services": len(services),
            "daily_volume": daily_volume,
        },
        "per_service": per_service_results,
        "aggregate": {
            "total_logs": total_logs,
            "wall_time_s": overall_wall,
            "aggregate_rate": aggregate_rate,
            "json_bytes": json_bytes,
            "proto_bytes": proto_bytes,
            "json_time_s": json_time,
            "proto_time_s": proto_time,
            "size_ratio": size_ratio,
            "speed_ratio": speed_ratio,
        },
        "projections": {
            "daily_volume": daily_volume,
            "json_storage_gb": projected_json_gb,
            "proto_storage_gb": projected_proto_gb,
            "storage_savings_gb": projected_json_gb - projected_proto_gb,
            "json_serial_time_h": projected_json_time_h,
            "proto_serial_time_h": projected_proto_time_h,
        },
    }


# ---------------------------------------------------------------------------
# Report formatter
# ---------------------------------------------------------------------------


def _human_bytes(n: float) -> str:
    """Format a byte count for display."""
    if n >= 1_073_741_824:
        return f"{n / 1_073_741_824:.2f} GB"
    if n >= 1_048_576:
        return f"{n / 1_048_576:.2f} MB"
    if n >= 1_024:
        return f"{n / 1_024:.2f} KB"
    return f"{n:.0f} B"


def print_high_load_report(results: dict[str, Any]) -> None:
    """Print a formatted high-load simulation report.

    Args:
        results: The dict returned by :func:`run_high_load_simulation`.
    """
    cfg = results["config"]
    agg = results["aggregate"]
    proj = results["projections"]

    width = 72

    print()
    print("=" * width)
    print("  HIGH-LOAD CONCURRENT SIMULATION REPORT")
    print("=" * width)

    # --- Configuration ---
    print()
    print("Configuration:")
    print(f"  Services ............ {cfg['num_services']}")
    print(f"  Target rate ......... {cfg['rate_per_service']:,} logs/sec per service")
    print(f"  Duration ............ {cfg['duration_s']}s")
    print(
        f"  Expected total ...... ~{cfg['rate_per_service'] * cfg['num_services'] * cfg['duration_s']:,} logs"
    )
    print()

    # --- Per-service throughput ---
    print("-" * width)
    print("  Per-Service Throughput")
    print("-" * width)
    print(
        f"  {'Service':<25} {'Logs':>8} {'Rate (l/s)':>12} {'JSON':>10} {'Protobuf':>10}"
    )
    print(f"  {'─' * 25} {'─' * 8} {'─' * 12} {'─' * 10} {'─' * 10}")

    for svc in results["per_service"]:
        print(
            f"  {svc['service']:<25} {svc['total_logs']:>8,} "
            f"{svc['achieved_rate']:>12.1f} "
            f"{_human_bytes(svc['json_bytes']):>10} "
            f"{_human_bytes(svc['proto_bytes']):>10}"
        )
    print()

    # --- Aggregate comparison ---
    print("-" * width)
    print("  Aggregate: JSON vs Protobuf Under Concurrency")
    print("-" * width)
    print(f"  Total logs generated ... {agg['total_logs']:,}")
    print(f"  Wall-clock time ........ {agg['wall_time_s']:.2f}s")
    print(f"  Aggregate throughput ... {agg['aggregate_rate']:,.0f} logs/sec")
    print()
    print(f"  JSON  total size ....... {_human_bytes(agg['json_bytes'])}")
    print(f"  Proto total size ....... {_human_bytes(agg['proto_bytes'])}")
    print(f"  Size ratio ............. {agg['size_ratio']:.2f}x (JSON / Protobuf)")
    print()
    print(f"  JSON  serial time ...... {agg['json_time_s']:.4f}s (cumulative)")
    print(f"  Proto serial time ...... {agg['proto_time_s']:.4f}s (cumulative)")
    print(f"  Speed ratio ............ {agg['speed_ratio']:.2f}x (JSON / Protobuf)")
    print()

    # --- Cost projections ---
    print("-" * width)
    print(f"  Cost Projections for {proj['daily_volume']:,} Daily Logs")
    print("-" * width)
    print(f"  JSON  storage/day ...... {proj['json_storage_gb']:.2f} GB")
    print(f"  Proto storage/day ...... {proj['proto_storage_gb']:.2f} GB")
    print(f"  Storage savings ........ {proj['storage_savings_gb']:.2f} GB/day")
    print()
    print(f"  JSON  serial time ...... {proj['json_serial_time_h']:.2f} CPU-hours/day")
    print(f"  Proto serial time ...... {proj['proto_serial_time_h']:.2f} CPU-hours/day")
    print()
    print("=" * width)
    print()
