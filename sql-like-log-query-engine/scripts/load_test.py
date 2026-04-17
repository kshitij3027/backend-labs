"""Load test driver for the distributed SQL-like log query engine.

Runs ``--concurrent-users`` asyncio workers for ``--duration`` seconds, each
one picking a query from a small representative pool (temporal, text search,
analytical) and firing ``POST /api/query`` at the coordinator. Measurements
are excluded during a ``--warmup`` period so cold-start latency doesn't
skew percentiles.

At the end it prints a human-readable report — total requests, QPS, error
rate, partial-failure rate, and p50 / p95 / p99 latency — and exits with
code ``0`` when sustained QPS meets the spec target of ≥ 100, otherwise
``1``. Any uncaught exception also exits non-zero.

Invoke inside Docker via::

    docker compose run --rm test python scripts/load_test.py \\
        --concurrent-users 20 --duration 30 --target-url http://coordinator:8000
"""

from __future__ import annotations

import asyncio
import statistics
import sys
import time
import traceback
from dataclasses import dataclass, field

import click
import httpx


# Minimum sustained QPS per ``project_requirements.md`` §5.
QPS_TARGET = 100.0

# Representative query pool — same shapes as ``scripts/demo.py``.
#
#   1. Temporal      — exercises partition pruning by time range.
#   2. Text search   — exercises CONTAINS across indexed fields.
#   3. Analytical    — exercises two-phase aggregation with GROUP BY.
#
# All three hit the same ``logs`` table; rotating through them gives the
# coordinator a realistic, mixed workload rather than just hammering the
# result cache with a single query shape.
TEMPORAL_QUERY = (
    "SELECT * FROM logs WHERE level = 'ERROR' "
    "AND timestamp BETWEEN '2026-04-08' AND '2026-04-14' LIMIT 10"
)
TEXT_QUERY = (
    "SELECT * FROM logs WHERE message CONTAINS 'timeout' LIMIT 10"
)
ANALYTICAL_QUERY = (
    "SELECT service, COUNT(*) AS cnt FROM logs "
    "GROUP BY service ORDER BY cnt DESC LIMIT 5"
)

QUERY_POOLS: dict[str, list[str]] = {
    "all": [TEMPORAL_QUERY, TEXT_QUERY, ANALYTICAL_QUERY],
    "temporal": [TEMPORAL_QUERY],
    "text": [TEXT_QUERY],
    "analytical": [ANALYTICAL_QUERY],
}


@dataclass
class Stats:
    """Shared counters + latency samples collected by all workers."""

    success: int = 0
    failure: int = 0
    partial: int = 0
    latencies_ms: list[float] = field(default_factory=list)


async def _worker(
    worker_id: int,
    client: httpx.AsyncClient,
    pool: list[str],
    stats: Stats,
    measurement_start: float,
    stop_at: float,
) -> None:
    """Run one async worker loop until ``stop_at`` wall-clock monotonic time.

    Latency samples and counters whose request *started* before
    ``measurement_start`` are discarded so warmup doesn't pollute the
    percentile distribution.
    """

    # Round-robin through the pool — keeps the mix deterministic and even
    # so every query shape gets roughly the same share of traffic.
    idx = worker_id
    while True:
        now = time.monotonic()
        if now >= stop_at:
            return

        query = pool[idx % len(pool)]
        idx += 1
        in_measurement = now >= measurement_start

        t0 = time.perf_counter()
        try:
            response = await client.post(
                "/api/query", json={"query": query}
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            success = False
            partial = False
            if response.status_code == 200:
                try:
                    body = response.json()
                except ValueError:
                    body = None
                if isinstance(body, dict):
                    success = True
                    partial = bool(body.get("partial_results"))

            if in_measurement:
                if success:
                    stats.success += 1
                    stats.latencies_ms.append(elapsed_ms)
                    if partial:
                        stats.partial += 1
                else:
                    stats.failure += 1
        except Exception:
            if in_measurement:
                stats.failure += 1


def _percentile(sorted_samples: list[float], q: float) -> float:
    """Return the ``q``-th percentile (0..100) via ``statistics.quantiles``.

    ``statistics.quantiles(n=100)`` returns the 99 cut-points p1..p99; we
    index into that list for p50/p95/p99. Falls back to the single sample
    (or ``0.0``) when there aren't enough data points to compute quantiles.
    """

    if not sorted_samples:
        return 0.0
    if len(sorted_samples) == 1:
        return sorted_samples[0]
    cutpoints = statistics.quantiles(sorted_samples, n=100)
    # cutpoints[0] == p1, cutpoints[49] == p50, cutpoints[94] == p95, ...
    idx = max(0, min(len(cutpoints) - 1, int(q) - 1))
    return cutpoints[idx]


def _format_report(
    *,
    concurrent_users: int,
    target_url: str,
    query_mix: str,
    duration_sec: float,
    warmup_sec: float,
    measured_sec: float,
    stats: Stats,
    qps: float,
    error_rate: float,
    partial_rate: float,
    p50: float,
    p95: float,
    p99: float,
) -> str:
    """Render the final human-readable report block."""

    total = stats.success + stats.failure
    lines = [
        "==============================================================",
        "  Distributed SQL-Like Log Query Engine — Load Test Report",
        "==============================================================",
        f"  Target URL          : {target_url}",
        f"  Concurrent users    : {concurrent_users}",
        f"  Query mix           : {query_mix}",
        f"  Warmup (sec)        : {warmup_sec:.1f}",
        f"  Measured duration   : {measured_sec:.2f} sec",
        f"  Total duration      : {duration_sec:.2f} sec",
        "--------------------------------------------------------------",
        f"  Total requests      : {total}",
        f"  Successful          : {stats.success}",
        f"  Failed              : {stats.failure}",
        f"  Error rate          : {error_rate:.2f}%",
        f"  Partial-failure rate: {partial_rate:.2f}%"
        " (of successful responses)",
        "--------------------------------------------------------------",
        f"  Throughput (QPS)    : {qps:.2f} requests/sec",
        f"  Latency p50         : {p50:.2f} ms",
        f"  Latency p95         : {p95:.2f} ms",
        f"  Latency p99         : {p99:.2f} ms",
        "==============================================================",
    ]
    return "\n".join(lines)


async def _run_load_test(
    *,
    concurrent_users: int,
    duration: float,
    target_url: str,
    query_mix: str,
    warmup: float,
) -> int:
    """Spin up workers, collect stats, print the report, return exit code."""

    if query_mix not in QUERY_POOLS:
        click.echo(
            f"Unknown --query-mix '{query_mix}'. Valid options: "
            f"{', '.join(sorted(QUERY_POOLS))}",
            err=True,
        )
        return 1

    pool = QUERY_POOLS[query_mix]
    stats = Stats()

    # Give httpx enough connection head-room to cover every worker without
    # starving; match the coordinator's own timeout (30 s).
    limits = httpx.Limits(
        max_connections=max(concurrent_users * 2, 50),
        max_keepalive_connections=max(concurrent_users * 2, 50),
    )
    timeout = httpx.Timeout(30.0, connect=10.0)

    async with httpx.AsyncClient(
        base_url=target_url.rstrip("/"),
        limits=limits,
        timeout=timeout,
    ) as client:
        # Cheap liveness check — fail fast with a clear error rather than
        # letting every worker spam connection-refused exceptions.
        try:
            hr = await client.get("/api/health", timeout=5.0)
            if hr.status_code != 200:
                click.echo(
                    f"Coordinator /api/health returned "
                    f"{hr.status_code}; aborting.",
                    err=True,
                )
                return 1
        except Exception as exc:
            click.echo(
                f"Could not reach coordinator at {target_url}: {exc}",
                err=True,
            )
            return 1

        wall_start = time.monotonic()
        measurement_start = wall_start + warmup
        stop_at = wall_start + warmup + duration

        workers = [
            asyncio.create_task(
                _worker(
                    worker_id=i,
                    client=client,
                    pool=pool,
                    stats=stats,
                    measurement_start=measurement_start,
                    stop_at=stop_at,
                )
            )
            for i in range(concurrent_users)
        ]

        await asyncio.gather(*workers, return_exceptions=True)

        measured_sec = max(0.001, time.monotonic() - measurement_start)

    total_requests = stats.success + stats.failure
    error_rate = (
        (stats.failure / total_requests) * 100.0 if total_requests else 0.0
    )
    partial_rate = (
        (stats.partial / stats.success) * 100.0 if stats.success else 0.0
    )
    qps = stats.success / measured_sec if measured_sec > 0 else 0.0

    sorted_lat = sorted(stats.latencies_ms)
    p50 = _percentile(sorted_lat, 50)
    p95 = _percentile(sorted_lat, 95)
    p99 = _percentile(sorted_lat, 99)

    report = _format_report(
        concurrent_users=concurrent_users,
        target_url=target_url,
        query_mix=query_mix,
        duration_sec=warmup + duration,
        warmup_sec=warmup,
        measured_sec=measured_sec,
        stats=stats,
        qps=qps,
        error_rate=error_rate,
        partial_rate=partial_rate,
        p50=p50,
        p95=p95,
        p99=p99,
    )
    print(report)

    if stats.success == 0:
        print(f"FAIL: no successful queries — QPS below target: {qps:.2f} requests/sec")
        return 1

    if qps >= QPS_TARGET:
        print(f"PASS: QPS target met: {qps:.2f} requests/sec")
        return 0
    print(f"FAIL: QPS below target: {qps:.2f} requests/sec (target {QPS_TARGET:.0f})")
    return 1


@click.command()
@click.option(
    "--concurrent-users",
    default=20,
    show_default=True,
    type=int,
    help="Number of concurrent asyncio workers.",
)
@click.option(
    "--duration",
    default=30,
    show_default=True,
    type=int,
    help="Measurement duration in seconds (excluding warmup).",
)
@click.option(
    "--target-url",
    default="http://coordinator:8000",
    show_default=True,
    help="Base URL of the coordinator service.",
)
@click.option(
    "--query-mix",
    default="all",
    show_default=True,
    type=click.Choice(["all", "temporal", "text", "analytical"]),
    help="Query shape distribution to drive.",
)
@click.option(
    "--warmup",
    default=3,
    show_default=True,
    type=int,
    help="Warmup period in seconds before measurements begin.",
)
def main(
    concurrent_users: int,
    duration: int,
    target_url: str,
    query_mix: str,
    warmup: int,
) -> None:
    """Run the load test and exit 0 on target-met, 1 otherwise."""

    if concurrent_users <= 0:
        click.echo("--concurrent-users must be >= 1", err=True)
        sys.exit(1)
    if duration <= 0:
        click.echo("--duration must be >= 1", err=True)
        sys.exit(1)
    if warmup < 0:
        click.echo("--warmup must be >= 0", err=True)
        sys.exit(1)

    try:
        exit_code = asyncio.run(
            _run_load_test(
                concurrent_users=concurrent_users,
                duration=float(duration),
                target_url=target_url,
                query_mix=query_mix,
                warmup=float(warmup),
            )
        )
    except Exception:
        click.echo("Unexpected error during load test:", err=True)
        traceback.print_exc()
        sys.exit(1)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
