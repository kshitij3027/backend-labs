"""Async load-test harness for the Faceted Log Search Engine.

Usage: python scripts/load_test.py [--concurrency=100] [--duration=30] [--warmup=5] [--seed-count=50000]

Success criteria (from Section 5 of project_requirements.md):
  - p95 < 100ms
  - qps >= 65
  - zero errors

Exits non-zero if any criterion fails.

Flow:
  1. Wait for ``/health`` to return 200 (up to 30s).
  2. Seed ``--seed-count`` synthetic rows via ``POST /api/logs/generate`` (seed=42).
  3. Warm the service for ``--warmup`` seconds — latencies discarded.
  4. Main run for ``--duration`` seconds with ``--concurrency`` async workers
     each firing POST /api/search with a weighted mix of query shapes:
       - 60% single-facet (one of service / level / region)
       - 30% multi-facet (2-3 dims with 1-2 values each)
       - 10% free-text from {"timeout","ok","rate","connection","unauthorized"}
  5. Print p50 / p95 / p99 / mean latency, qps, error count and pass/fail.
  6. Exit 0 on PASS, 1 on FAIL.

The harness intentionally has retries=0: we measure raw per-request latency
and refuse to hide errors behind retries.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx


# ---------------------------------------------------------------------------
# Query-shape catalog — kept in sync with src/search/generator.py so the
# values we send actually match the seeded distribution.
# ---------------------------------------------------------------------------

SERVICES: List[str] = ["payments", "auth", "api-gateway", "cache", "orders"]
LEVELS: List[str] = ["INFO", "WARN", "ERROR", "DEBUG", "FATAL"]
REGIONS: List[str] = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]
LATENCY_BUCKETS: List[str] = ["0-100ms", "100-500ms", "500ms-2s", "2s+"]

# Known message tokens from generator.MESSAGE_TEMPLATES.
FREE_TEXT_TOKENS: List[str] = [
    "timeout",
    "ok",
    "rate",
    "connection",
    "unauthorized",
]


def _pick_single_facet_body() -> Dict[str, Any]:
    """60% shape: one random facet dimension, one or two values."""
    dim = random.choice(["service", "level", "region"])
    if dim == "service":
        values = random.sample(SERVICES, k=random.choice([1, 2]))
    elif dim == "level":
        values = random.sample(LEVELS, k=random.choice([1, 2]))
    else:
        values = random.sample(REGIONS, k=random.choice([1, 2]))
    return {"filters": {dim: values}, "limit": 10}


def _pick_multi_facet_body() -> Dict[str, Any]:
    """30% shape: 2-3 dims, 1-2 values each."""
    pool: Dict[str, List[Any]] = {
        "service": SERVICES,
        "level": LEVELS,
        "region": REGIONS,
        "latency_bucket": LATENCY_BUCKETS,
    }
    dims = random.sample(list(pool.keys()), k=random.choice([2, 3]))
    filters: Dict[str, List[Any]] = {}
    for dim in dims:
        k = random.choice([1, 2])
        filters[dim] = random.sample(pool[dim], k=min(k, len(pool[dim])))
    return {"filters": filters, "limit": 10}


def _pick_free_text_body() -> Dict[str, Any]:
    """10% shape: free-text substring query from the known-token list."""
    return {"query": random.choice(FREE_TEXT_TOKENS), "limit": 10}


def pick_query_body() -> Dict[str, Any]:
    """Weighted 60/30/10 mix across the three shapes."""
    r = random.random()
    if r < 0.60:
        return _pick_single_facet_body()
    if r < 0.90:
        return _pick_multi_facet_body()
    return _pick_free_text_body()


# ---------------------------------------------------------------------------
# Metrics + worker runtime
# ---------------------------------------------------------------------------

@dataclass
class Metrics:
    """Shared accumulator for one run (warmup or main)."""

    latencies_ms: List[float] = field(default_factory=list)
    errors: int = 0
    error_samples: List[str] = field(default_factory=list)

    def record(self, latency_ms: float) -> None:
        self.latencies_ms.append(latency_ms)

    def record_error(self, message: str) -> None:
        self.errors += 1
        # Keep only the first handful of error strings so we don't blow
        # memory when the app is in a degraded state.
        if len(self.error_samples) < 10:
            self.error_samples.append(message)


async def wait_for_health(client: httpx.AsyncClient, deadline_s: float = 30.0) -> None:
    """Poll ``/health`` until 200 or deadline."""
    elapsed = 0.0
    sleep = 0.5
    last_err: Optional[str] = None
    while elapsed < deadline_s:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                print(f"  health OK after {elapsed:.1f}s")
                return
            last_err = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 — best-effort poll
            last_err = str(exc)
        await asyncio.sleep(sleep)
        elapsed += sleep
    raise SystemExit(f"/health not ready after {deadline_s}s (last: {last_err})")


async def seed(client: httpx.AsyncClient, count: int, seed_value: int = 42) -> float:
    """Seed ``count`` synthetic logs. Returns elapsed seconds."""
    print(f"  seeding {count} logs (seed={seed_value}) ...")
    t0 = time.perf_counter()
    resp = await client.post(
        f"/api/logs/generate?count={count}&seed={seed_value}",
        timeout=httpx.Timeout(60.0, connect=5.0),
    )
    resp.raise_for_status()
    elapsed = time.perf_counter() - t0
    body = resp.json()
    print(f"  seeded {body.get('generated_count', count)} rows in {elapsed:.2f}s")
    return elapsed


async def worker(
    client: httpx.AsyncClient,
    stop_event: asyncio.Event,
    metrics: Metrics,
) -> None:
    """Hot loop: pick a query shape, fire, record latency, repeat until stop."""
    while not stop_event.is_set():
        body = pick_query_body()
        t0 = time.perf_counter()
        try:
            resp = await client.post("/api/search", json=body)
            latency_ms = (time.perf_counter() - t0) * 1000.0
            if resp.status_code >= 300:
                metrics.record_error(f"HTTP {resp.status_code}: {resp.text[:120]}")
            else:
                metrics.record(latency_ms)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — surface any transport error
            metrics.record_error(f"{type(exc).__name__}: {exc}")


async def run_phase(
    client: httpx.AsyncClient,
    concurrency: int,
    duration_s: float,
    metrics: Metrics,
    label: str,
) -> float:
    """Spawn ``concurrency`` workers, run for ``duration_s``, return actual elapsed."""
    stop_event = asyncio.Event()
    print(f"  {label}: {concurrency} workers x {duration_s:.1f}s ...")
    t0 = time.perf_counter()
    tasks = [
        asyncio.create_task(worker(client, stop_event, metrics))
        for _ in range(concurrency)
    ]
    try:
        await asyncio.sleep(duration_s)
    finally:
        stop_event.set()
        # Give workers a brief grace period to finish their in-flight
        # request; if they don't, cancel hard.
        done, pending = await asyncio.wait(tasks, timeout=5.0)
        for t in pending:
            t.cancel()
        # Drain cancellations so exceptions don't get "never retrieved" warnings.
        for t in pending:
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
    elapsed = time.perf_counter() - t0
    return elapsed


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def percentile(sorted_vals: List[float], p: float) -> float:
    """Linear-interp percentile from a pre-sorted list. ``p`` in [0,1]."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    # Nearest-rank is fine for load-test reporting and matches most tooling.
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


def summarize(
    metrics: Metrics,
    elapsed_s: float,
    concurrency: int,
    duration_s: float,
    p95_budget_ms: float,
    min_qps: float,
) -> bool:
    """Print the ASCII report. Returns True if all criteria pass."""
    latencies = sorted(metrics.latencies_ms)
    total_ok = len(latencies)
    total_requests = total_ok + metrics.errors
    qps = total_ok / elapsed_s if elapsed_s > 0 else 0.0

    p50 = percentile(latencies, 0.50)
    p95 = percentile(latencies, 0.95)
    p99 = percentile(latencies, 0.99)
    mean = statistics.fmean(latencies) if latencies else 0.0

    pass_p95 = p95 < p95_budget_ms
    pass_qps = qps >= min_qps
    pass_err = metrics.errors == 0
    verdict = pass_p95 and pass_qps and pass_err

    def mark(ok: bool) -> str:
        return "PASS" if ok else "FAIL"

    print()
    print("=== Load Test Result ===")
    print(f"Concurrency:            {concurrency}")
    print(f"Duration:               {duration_s:.1f}s (actual {elapsed_s:.1f}s)")
    print(f"Total requests:         {total_requests}")
    print(f"Errors:                 {metrics.errors}")
    print(
        "Latency p50 / p95 / p99 (ms): "
        f"{p50:.1f} / {p95:.1f} / {p99:.1f}"
    )
    print(f"Mean latency (ms):      {mean:.1f}")
    print(f"Throughput:             {qps:.1f} qps")
    print("Success criteria:")
    print(
        f"  p95 < {p95_budget_ms:.1f}ms          "
        f"{mark(pass_p95)}  ({p95:.1f} {'<' if pass_p95 else '>='} {p95_budget_ms:.1f})"
    )
    print(
        f"  qps >= {min_qps:.1f}             "
        f"{mark(pass_qps)}  ({qps:.1f} {'>=' if pass_qps else '<'} {min_qps:.1f})"
    )
    print(
        f"  errors == 0             {mark(pass_err)}  ({metrics.errors})"
    )
    if metrics.error_samples:
        print("  Sample error messages:")
        for sample in metrics.error_samples[:5]:
            print(f"    - {sample}")
    print()
    print(f"=== {'PASSED' if verdict else 'FAILED'} ===")
    return verdict


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments with the documented defaults."""
    p = argparse.ArgumentParser(description="Async load test for the faceted log search engine.")
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--duration", type=float, default=30.0, help="Main run duration in seconds.")
    p.add_argument("--warmup", type=float, default=5.0, help="Warmup duration in seconds.")
    p.add_argument("--seed-count", type=int, default=50000, help="Synthetic rows to ingest.")
    p.add_argument(
        "--target",
        type=str,
        default=os.getenv("APP_URL", "http://app:8000"),
        help="Base URL of the app (default: APP_URL env or http://app:8000).",
    )
    p.add_argument("--p95-budget-ms", type=float, default=100.0)
    p.add_argument("--min-qps", type=float, default=65.0)
    return p.parse_args()


async def amain(args: argparse.Namespace) -> int:
    """Run the harness and return the process exit code."""
    print("=== Faceted Log Search — Load Test ===")
    print(f"target:        {args.target}")
    print(f"concurrency:   {args.concurrency}")
    print(f"duration:      {args.duration}s")
    print(f"warmup:        {args.warmup}s")
    print(f"seed_count:    {args.seed_count}")
    print(f"p95 budget:    {args.p95_budget_ms:.1f}ms")
    print(f"min qps:       {args.min_qps:.1f}")
    print()

    # Enough pooled connections to actually saturate ``concurrency``.
    limits = httpx.Limits(
        max_keepalive_connections=args.concurrency * 2,
        max_connections=args.concurrency * 2,
    )
    # Per-request timeout: 10s is generous for a normally <100ms endpoint.
    timeout = httpx.Timeout(10.0, connect=5.0)
    # retries=0 (transport default) — measure raw latency, don't hide errors.
    async with httpx.AsyncClient(
        base_url=args.target,
        timeout=timeout,
        limits=limits,
    ) as client:
        print("Stage: health check")
        await wait_for_health(client)

        print("Stage: seed")
        await seed(client, count=args.seed_count)

        warm_metrics = Metrics()
        print("Stage: warmup (metrics discarded)")
        await run_phase(
            client,
            concurrency=args.concurrency,
            duration_s=args.warmup,
            metrics=warm_metrics,
            label="warmup",
        )
        if warm_metrics.errors:
            print(
                f"  warmup saw {warm_metrics.errors} errors "
                f"(first: {warm_metrics.error_samples[0] if warm_metrics.error_samples else '?'})"
            )

        main_metrics = Metrics()
        print("Stage: main")
        elapsed = await run_phase(
            client,
            concurrency=args.concurrency,
            duration_s=args.duration,
            metrics=main_metrics,
            label="main",
        )

    verdict = summarize(
        metrics=main_metrics,
        elapsed_s=elapsed,
        concurrency=args.concurrency,
        duration_s=args.duration,
        p95_budget_ms=args.p95_budget_ms,
        min_qps=args.min_qps,
    )
    return 0 if verdict else 1


def main() -> int:
    """Synchronous wrapper so ``python scripts/load_test.py`` works directly."""
    args = parse_args()
    try:
        return asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
