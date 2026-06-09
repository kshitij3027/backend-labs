"""Containerized load test for the bloom-filter membership service (C13).

Run by the compose ``loadtest`` profile service (``Dockerfile.test``), this
script proves the project's throughput/latency criteria in three phases and
exits 1 if any gate fails:

Phase 1 — in-process criteria gates (no HTTP)
    The spec's ">=10,000 ops/sec" and "<1 ms query" criteria are about the
    membership engine, so they are measured where the engine lives — in
    process, importing :mod:`src` directly (the loadtest container ships the
    same code as the app image):

    * a raw :class:`~src.scalable.ScalableBloomFilter` (1M @ p=0.01) under a
      200k mixed 60/40 add/query workload — reported, millions expected;
    * the MANAGED path (``FilterManager.add``/``query`` — per-filter lock +
      metrics recording, exactly the per-op cost every API handler pays)
      under a 100k mixed workload — **gate: >= 10,000 ops/sec**;
    * a dedicated 50k timed query batch through the managed path (stricter
      than raw ``might_contain``: it includes the lock and metrics) —
      **gate: avg query < 1.0 ms**.

Phase 2 — HTTP load over the compose network
    ``LOAD_CONCURRENCY`` async workers hammer the live ``app`` container by
    service name for ``LOAD_DURATION_SECONDS`` of wall clock with a
    read-heavy mix: 50% ``POST /logs/query`` (random, mostly-absent keys —
    the dedup-miss traffic the service exists for), 30% ``POST /logs/add``,
    20% ``POST /pipeline/lookup`` (mostly absent → the bloom-negative
    short-circuit path). Per-request wall latency and success/error are
    recorded. **Gates: qps >= 1,000 and error rate <= 1%**; p50/p90/p99 are
    reported.

    **This phase is CLIENT-bound, by measurement.** The single-process
    asyncio + httpx generator saturates ~1 CPU core near ~1.4k qps while the
    single-worker server idles at ~6% CPU — measured server headroom is
    >= 10x the gate. So this phase smoke-gates the HTTP plumbing (the stack
    answers real network traffic at >= 1k qps with <= 1% errors); the
    project's 10k+ ops/sec success criterion is gated in phase 1, where the
    filter itself is measured without the generator in the way. Raising
    ``LOAD_CONCURRENCY`` does NOT raise qps — past the generator's core
    ceiling extra workers only deepen client-side queueing and DEGRADE the
    measured number (observed: c=16 → ~1.37k qps, c=32 → ~0.95k, c=64 →
    ~0.67k, c=96 → ~0.56k). Prefer 8-16.

Phase 3 — memory ratio report
    ``GET /stats`` per-filter ``memory_bytes`` against a 64-bytes-per-key
    full-storage estimate at each filter's design capacity (the same 64-byte
    reference key the demo benchmark uses). **Gate: ratio < 0.05 for
    ``error_logs`` and ``sessions``** (the p=0.01 filters the <5% criterion
    is judged at, ~2.6% expected); ``access_logs`` (p=0.05) and
    ``security_logs`` (p=0.001) are reported, not gated — their per-key bit
    budgets are a property of their own FP targets, not of this criterion.

Environment:
    APP_URL                base URL of the API service (default http://app:8001)
    LOAD_DURATION_SECONDS  phase-2 measured wall clock (default 8)
    LOAD_CONCURRENCY       phase-2 async workers       (default 16; prefer
                           8-16 — higher values degrade measured qps, see
                           the phase-2 note above)

Exit code 0 and ``LOAD: all gates passed`` only when every gate holds.
"""
from __future__ import annotations

import asyncio
import math
import os
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import uuid4

import httpx

from src.manager import FilterManager
from src.scalable import ScalableBloomFilter
from src.settings import Settings

APP_URL = os.environ.get("APP_URL", "http://app:8001").rstrip("/")
DURATION_SECONDS = float(os.environ.get("LOAD_DURATION_SECONDS", "8"))
# Default 16: the generator (one asyncio loop) tops out near ~1.4k qps around
# this level; more workers only queue client-side and lower the measured qps.
CONCURRENCY = int(os.environ.get("LOAD_CONCURRENCY", "16"))

#: Per-run nonce: phase-1 keys never collide across runs, and phase-2 keys
#: stay disjoint from whatever the bind-mounted server already holds.
NONCE = uuid4().hex[:8]

LOG_TYPES: tuple[str, ...] = ("error_logs", "access_logs", "security_logs")

# Phase-1 workload sizes.
RAW_OPS = 200_000
MANAGED_OPS = 100_000
QUERY_BATCH = 50_000

# Phase-1 gates (project success criteria, measured at the engine).
MIN_MANAGED_OPS_PER_SEC = 10_000.0
MAX_AVG_QUERY_MS = 1.0

# Phase-2 gates (end-to-end over Docker NAT — see module docstring).
MIN_HTTP_QPS = 1_000.0
MAX_ERROR_RATE = 0.01

# Phase-3 memory comparison: a stored key is modelled at 64 bytes (request
# ids / sha256-hashed lines — same reference as /demo/performance-test).
FULL_KEY_BYTES = 64
MAX_MEMORY_RATIO = 0.05
GATED_RATIO_FILTERS = ("error_logs", "sessions")  # the p=0.01 filters

_RANDOM_SEED = 0x5EEDB10C  # the project's house seed, for a reproducible mix


def pct(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile of an already-sorted list (``p`` in [0, 1])."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    rank = max(1, min(math.ceil(p * n), n))
    return sorted_values[rank - 1]


# --------------------------------------------------------------------- #
# phase 1 — in-process engine gates                                     #
# --------------------------------------------------------------------- #


def _mixed_ops(
    add: Callable[[str], object],
    query: Callable[[str], object],
    ops: int,
    prefix: str,
) -> float:
    """Drive ``ops`` mixed operations (60% add / 40% query); return seconds.

    Deterministic 5-op cycle: positions 0-2 add a unique key, position 3
    queries a key added 3 ops earlier (present), position 4 queries a
    never-added key (absent) — a 60/40 add/query mix whose queries split
    present/absent without RNG overhead inside the timed loop.
    """
    start = time.perf_counter()
    for i in range(ops):
        r = i % 5
        if r < 3:
            add(f"{prefix}-{NONCE}-{i}")
        elif r == 3:
            query(f"{prefix}-{NONCE}-{i - 3}")  # i-3 ≡ 0 (mod 5) → was added
        else:
            query(f"{prefix}-absent-{NONCE}-{i}")
    return time.perf_counter() - start


def run_phase1(failures: list[str]) -> dict[str, float]:
    """Measure raw-SBF and managed-path throughput plus managed query latency."""
    print(f"PHASE 1: in-process engine ({RAW_OPS} raw + {MANAGED_OPS} managed ops)")

    # Raw engine: the data structure alone, no locks, no metrics.
    sbf = ScalableBloomFilter(initial_capacity=1_000_000, target_fp_rate=0.01)
    raw_elapsed = _mixed_ops(sbf.add, sbf.might_contain, RAW_OPS, "raw")
    raw_ops_sec = RAW_OPS / raw_elapsed

    # Managed path: FilterManager from the default Settings — per-filter lock
    # acquisition + metrics recording, the true per-op cost the API pays.
    # Construction never touches disk (only save_all/load_all do), so the
    # container needs no data dir.
    manager = FilterManager(Settings())
    managed_elapsed = _mixed_ops(
        lambda key: manager.add("error_logs", key),
        lambda key: manager.query("error_logs", key),
        MANAGED_OPS,
        "managed",
    )
    managed_ops_sec = MANAGED_OPS / managed_elapsed

    # Dedicated timed query batch (alternating present/absent) for the <1ms
    # gate — through the managed path, which subsumes the raw might_contain.
    added_count = math.ceil(MANAGED_OPS / 5)  # keys at indices ≡ 0 (mod 5)
    start = time.perf_counter()
    for i in range(QUERY_BATCH):
        if i % 2 == 0:
            manager.query(
                "error_logs", f"managed-{NONCE}-{5 * (i % added_count)}"
            )
        else:
            manager.query("error_logs", f"qb-absent-{NONCE}-{i}")
    avg_query_ms = (time.perf_counter() - start) * 1000.0 / QUERY_BATCH

    print(f"  raw SBF mixed ops/sec:     {raw_ops_sec:,.0f}")
    print(f"  managed mixed ops/sec:     {managed_ops_sec:,.0f}   "
          f"(gate >= {MIN_MANAGED_OPS_PER_SEC:,.0f})")
    print(f"  managed avg query ms:      {avg_query_ms:.6f}   "
          f"(gate < {MAX_AVG_QUERY_MS})")

    if managed_ops_sec < MIN_MANAGED_OPS_PER_SEC:
        failures.append(
            f"managed ops/sec {managed_ops_sec:,.0f} < {MIN_MANAGED_OPS_PER_SEC:,.0f}"
        )
    if avg_query_ms >= MAX_AVG_QUERY_MS:
        failures.append(
            f"managed avg query {avg_query_ms:.6f} ms >= {MAX_AVG_QUERY_MS} ms"
        )
    return {
        "raw_ops_sec": raw_ops_sec,
        "managed_ops_sec": managed_ops_sec,
        "avg_query_ms": avg_query_ms,
    }


# --------------------------------------------------------------------- #
# phase 2 — HTTP load by service name                                   #
# --------------------------------------------------------------------- #


@dataclass
class HttpResults:
    """Per-request outcomes accumulated by every phase-2 worker."""

    latencies_ms: list[float] = field(default_factory=list)
    ok: int = 0
    errors: int = 0

    @property
    def total(self) -> int:
        return self.ok + self.errors

    def record(self, *, success: bool, latency_ms: float) -> None:
        if success:
            self.ok += 1
            self.latencies_ms.append(latency_ms)
        else:
            self.errors += 1


async def _wait_for_health(attempts: int = 60, delay: float = 1.0) -> bool:
    """Poll ``GET /health`` until 200 (compose's depends_on makes this quick)."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        for _ in range(attempts):
            try:
                resp = await client.get(f"{APP_URL}/health")
                if resp.status_code == 200:
                    return True
            except Exception:  # noqa: BLE001 — still booting, retry
                pass
            await asyncio.sleep(delay)
    return False


async def _worker(
    client: httpx.AsyncClient, deadline: float, rng: random.Random, results: HttpResults
) -> None:
    """One worker: 50% /logs/query, 30% /logs/add, 20% /pipeline/lookup.

    Keys are drawn from a 10M space under this run's nonce — query and
    pipeline probes are therefore mostly absent (the realistic dedup-miss
    mix, and the bloom-negative short-circuit path for the pipeline).
    """
    while time.perf_counter() < deadline:
        roll = rng.random()
        log_type = LOG_TYPES[rng.randrange(len(LOG_TYPES))]
        if roll < 0.50:
            path = "/logs/query"
            key = f"lt-{NONCE}-q-{rng.randrange(10_000_000)}"
        elif roll < 0.80:
            path = "/logs/add"
            key = f"lt-{NONCE}-a-{rng.randrange(10_000_000)}"
        else:
            path = "/pipeline/lookup"
            key = f"lt-{NONCE}-p-{rng.randrange(10_000_000)}"
        t0 = time.perf_counter()
        try:
            resp = await client.post(path, json={"log_type": log_type, "log_key": key})
            success = resp.status_code == 200
        except Exception:  # noqa: BLE001 — transport failure is an error
            success = False
        results.record(
            success=success, latency_ms=(time.perf_counter() - t0) * 1000.0
        )


async def run_phase2(failures: list[str]) -> dict[str, float]:
    """Drive the measured HTTP phase and evaluate the qps / error-rate gates."""
    print(
        f"PHASE 2: HTTP load on {APP_URL} "
        f"({CONCURRENCY} workers x {DURATION_SECONDS:.0f}s, "
        f"50% query / 30% add / 20% pipeline-lookup)"
    )
    if not await _wait_for_health():
        failures.append(f"{APP_URL}/health never became ready")
        return {"qps": 0.0, "error_rate": 1.0, "p50": 0.0, "p90": 0.0, "p99": 0.0}

    results = HttpResults()
    master = random.Random(_RANDOM_SEED)
    limits = httpx.Limits(
        max_connections=CONCURRENCY * 2, max_keepalive_connections=CONCURRENCY * 2
    )
    async with httpx.AsyncClient(
        base_url=APP_URL, timeout=30.0, limits=limits
    ) as client:
        start = time.perf_counter()
        deadline = start + DURATION_SECONDS
        await asyncio.gather(
            *(
                _worker(client, deadline, random.Random(master.random()), results)
                for _ in range(CONCURRENCY)
            )
        )
        elapsed = time.perf_counter() - start

    qps = results.ok / elapsed if elapsed > 0 else 0.0
    error_rate = results.errors / results.total if results.total else 1.0
    latencies = sorted(results.latencies_ms)
    p50, p90, p99 = pct(latencies, 0.50), pct(latencies, 0.90), pct(latencies, 0.99)

    print(f"  requests ok / errors:      {results.ok} / {results.errors}")
    print(f"  qps:                       {qps:,.1f}   (gate >= {MIN_HTTP_QPS:,.0f})")
    print(f"  error rate:                {error_rate:.2%}   "
          f"(gate <= {MAX_ERROR_RATE:.0%})")
    print(f"  latency p50/p90/p99 ms:    {p50:.2f} / {p90:.2f} / {p99:.2f}")
    print(
        "  note: this phase is CLIENT-bound — the single-process asyncio+httpx\n"
        "        generator saturates ~1 CPU core near ~1.4k qps while the\n"
        "        single-worker server idles (~6% CPU, >=10x headroom). It\n"
        "        smoke-gates the HTTP plumbing; the 10k+ ops/sec criterion is\n"
        "        gated in phase 1 at the filter itself. Higher LOAD_CONCURRENCY\n"
        "        degrades measured qps via client-side queueing — prefer 8-16."
    )

    if results.ok == 0:
        failures.append("no successful HTTP requests recorded")
    if qps < MIN_HTTP_QPS:
        failures.append(f"HTTP qps {qps:,.1f} < {MIN_HTTP_QPS:,.0f}")
    if error_rate > MAX_ERROR_RATE:
        failures.append(f"HTTP error rate {error_rate:.2%} > {MAX_ERROR_RATE:.0%}")
    return {"qps": qps, "error_rate": error_rate, "p50": p50, "p90": p90, "p99": p99}


# --------------------------------------------------------------------- #
# phase 3 — memory vs full-key storage                                  #
# --------------------------------------------------------------------- #


def run_phase3(failures: list[str]) -> None:
    """Report each filter's bytes-per-capacity ratio vs 64-byte stored keys."""
    print(f"PHASE 3: memory vs {FULL_KEY_BYTES}B/key full storage (from /stats)")
    try:
        stats = httpx.get(f"{APP_URL}/stats", timeout=10.0).json()
    except Exception as exc:  # noqa: BLE001 — without /stats the gate fails
        failures.append(f"could not read /stats for the memory ratio: {exc}")
        return
    for name, f in stats.get("filters", {}).items():
        capacity = f.get("capacity", 0)
        memory_bytes = f.get("memory_bytes", 0)
        full_bytes = capacity * FULL_KEY_BYTES
        ratio = memory_bytes / full_bytes if full_bytes else float("inf")
        gated = name in GATED_RATIO_FILTERS
        verdict = f"(gate < {MAX_MEMORY_RATIO})" if gated else "(report only)"
        print(
            f"  {name:<14} memory={memory_bytes:>9,}B capacity={capacity:>9,} "
            f"ratio={ratio:.4f} {verdict}"
        )
        if gated and ratio >= MAX_MEMORY_RATIO:
            failures.append(
                f"{name} memory ratio {ratio:.4f} >= {MAX_MEMORY_RATIO} "
                f"({memory_bytes}B for {capacity} keys vs {FULL_KEY_BYTES}B/key)"
            )


# --------------------------------------------------------------------- #
# entrypoint                                                            #
# --------------------------------------------------------------------- #


def main() -> int:
    """Run all three phases; print the gate verdicts; return the exit code."""
    print(
        f"LOAD test target: {APP_URL} "
        f"(duration={DURATION_SECONDS:.0f}s, concurrency={CONCURRENCY}, "
        f"nonce={NONCE})"
    )
    failures: list[str] = []
    run_phase1(failures)
    asyncio.run(run_phase2(failures))
    run_phase3(failures)

    bar = "=" * 60
    print(bar)
    if failures:
        for failure in failures:
            print(f"GATE FAILED: {failure}")
        print("LOAD: FAILED")
        return 1
    print("LOAD: all gates passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
