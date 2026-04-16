"""Performance benchmark driver.

Indexes a modest corpus then pounds the coordinator's ``/search`` endpoint
with ``CONCURRENCY`` async workers for ``DURATION_SEC`` seconds. Prints a
JSON summary with ``qps``, ``p50_ms``, ``p95_ms``, ``p99_ms`` and fails if
thresholds are not met.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sys
import time

import httpx

COORDINATOR = os.getenv("COORDINATOR_URL", "http://coordinator:8000")
N_DOCS = int(os.getenv("N_DOCS", "400"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "50"))
DURATION_SEC = float(os.getenv("DURATION_SEC", "10"))
P50_MAX_MS = float(os.getenv("P50_MAX_MS", "100"))
MIN_QPS = float(os.getenv("MIN_QPS", "100"))

WORDS = [
    "error", "timeout", "login", "database", "request", "user", "service",
    "cache", "queue", "retry", "connection", "socket", "disk", "memory",
    "network", "auth", "token", "session", "payload", "shard",
    "primary", "replica", "coordinator", "worker", "commit", "rollback",
    "latency", "throughput", "heartbeat", "quorum",
]

DOCS = [
    {
        "doc_id": f"d{i:05d}",
        "content": " ".join(random.Random(i).sample(WORDS, 8))
        + f" host-{i} region-{i % 4}",
    }
    for i in range(N_DOCS)
]


async def wait_healthy(client: httpx.AsyncClient) -> None:
    for _ in range(60):
        try:
            r = await client.get(f"{COORDINATOR}/health")
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise SystemExit("coordinator not healthy")


async def index_all(client: httpx.AsyncClient) -> None:
    sem = asyncio.Semaphore(50)

    async def idx(d: dict) -> None:
        async with sem:
            r = await client.post(f"{COORDINATOR}/index", json=d, timeout=10)
            r.raise_for_status()

    await asyncio.gather(*(idx(d) for d in DOCS))


async def run_worker(
    client: httpx.AsyncClient,
    q_pool: list[str],
    latencies: list[float],
    stop_at: float,
    counter: list[int],
) -> None:
    while time.monotonic() < stop_at:
        q = random.choice(q_pool)
        t0 = time.perf_counter()
        try:
            r = await client.post(
                f"{COORDINATOR}/search",
                json={"query": q, "op": "OR", "limit": 20},
                timeout=10,
            )
            r.raise_for_status()
            latencies.append((time.perf_counter() - t0) * 1000)
            counter[0] += 1
        except Exception:
            counter[1] += 1


async def main() -> None:
    limits = httpx.Limits(
        max_connections=CONCURRENCY * 2,
        max_keepalive_connections=CONCURRENCY * 2,
    )
    async with httpx.AsyncClient(limits=limits, timeout=15) as client:
        await wait_healthy(client)
        await index_all(client)
        print(f"indexed {len(DOCS)} docs", flush=True)

        # 200 varied single-term queries — varied inputs defeat the
        # coordinator result cache so we measure real scatter-gather cost.
        q_pool = [random.Random(i).choice(WORDS) for i in range(200)]
        latencies: list[float] = []
        counter = [0, 0]  # [ok, fail]
        stop_at = time.monotonic() + DURATION_SEC
        workers = [
            asyncio.create_task(
                run_worker(client, q_pool, latencies, stop_at, counter)
            )
            for _ in range(CONCURRENCY)
        ]
        await asyncio.gather(*workers)

        if not latencies:
            print("FAIL: no successful queries", file=sys.stderr)
            sys.exit(1)

        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        qps = counter[0] / DURATION_SEC
        summary = {
            "ok": counter[0],
            "fail": counter[1],
            "duration_sec": DURATION_SEC,
            "qps": round(qps, 1),
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "p99_ms": round(p99, 2),
        }
        print(json.dumps(summary), flush=True)

        failed = False
        if p50 > P50_MAX_MS:
            print(f"FAIL: p50 {p50:.1f}ms > {P50_MAX_MS}ms", file=sys.stderr)
            failed = True
        if qps < MIN_QPS:
            print(f"FAIL: qps {qps:.1f} < {MIN_QPS}", file=sys.stderr)
            failed = True
        if failed:
            sys.exit(1)
        print("PASS")


if __name__ == "__main__":
    asyncio.run(main())
