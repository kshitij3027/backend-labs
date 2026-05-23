"""Throughput harness for /v1/audit/append.

Drives the live app from inside the tester container over the compose
network and reports p50/p95/p99 + RPS. Exits 1 if the assertion bounds
are violated.

Run via:
    docker compose --profile test run --rm -e BASE_URL=http://app:8000 \
        tester python scripts/throughput.py

Tuning knobs (env vars):
    TOTAL_REQUESTS  default 500
    CONCURRENCY     default 20
    RPS_MIN         default 100
    P50_MAX_MS      default 100.0
"""
from __future__ import annotations

import asyncio
import os
import random
import string
import statistics
import sys
import time

import httpx


BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
TOTAL_REQUESTS = int(os.environ.get("TOTAL_REQUESTS", "500"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "20"))
RPS_MIN = float(os.environ.get("RPS_MIN", "100"))
P50_MAX_MS = float(os.environ.get("P50_MAX_MS", "100"))


def _digest() -> str:
    return "".join(random.choices(string.hexdigits.lower()[:16], k=64))


def _body(i: int) -> dict:
    return {
        "actor": f"loadtest_user_{i % 20}",
        "action": random.choice(["read", "search", "export"]),
        "resource": f"LOG_throughput_{i % 5}",
        "success": True,
        "args_digest": _digest(),
        "result_digest": _digest(),
        "processing_ms": round(random.uniform(0.1, 3.0), 2),
    }


async def _one_request(client: httpx.AsyncClient, body: dict) -> float:
    """Issue one POST, return its end-to-end latency in ms. Raises on non-201."""
    t0 = time.perf_counter()
    r = await client.post(f"{BASE_URL}/v1/audit/append", json=body)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    if r.status_code != 201:
        raise RuntimeError(f"unexpected status {r.status_code}: {r.text[:200]}")
    return elapsed_ms


async def _worker(
    client: httpx.AsyncClient,
    queue: asyncio.Queue,
    latencies: list[float],
) -> None:
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        try:
            latency = await _one_request(client, item)
            latencies.append(latency)
        except Exception as exc:  # noqa: BLE001
            print(f"  request failed: {exc}", file=sys.stderr)
        finally:
            queue.task_done()


async def main() -> int:
    print(
        f"Throughput harness: {TOTAL_REQUESTS} requests, "
        f"concurrency={CONCURRENCY}, base_url={BASE_URL}"
    )
    queue: asyncio.Queue = asyncio.Queue()
    latencies: list[float] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Sanity ping
        r = await client.get(f"{BASE_URL}/api/health")
        if r.status_code != 200:
            print(f"health check failed: {r.status_code}", file=sys.stderr)
            return 1

        workers = [
            asyncio.create_task(_worker(client, queue, latencies))
            for _ in range(CONCURRENCY)
        ]

        t0 = time.perf_counter()
        for i in range(TOTAL_REQUESTS):
            await queue.put(_body(i))
        # Sentinel to stop each worker.
        for _ in range(CONCURRENCY):
            await queue.put(None)
        await queue.join()
        for w in workers:
            await w
        elapsed_sec = time.perf_counter() - t0

    n = len(latencies)
    if n == 0:
        print("no successful requests", file=sys.stderr)
        return 1

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[int(0.95 * n) - 1]
    p99 = latencies[int(0.99 * n) - 1]
    mean = statistics.mean(latencies)
    rps = n / elapsed_sec

    print()
    print(f"Completed: {n}/{TOTAL_REQUESTS} requests in {elapsed_sec:.2f}s")
    print(f"  RPS:    {rps:.1f}")
    print(f"  mean:   {mean:.2f}ms")
    print(f"  p50:    {p50:.2f}ms")
    print(f"  p95:    {p95:.2f}ms")
    print(f"  p99:    {p99:.2f}ms")
    print()

    # Hard assertions.
    failures: list[str] = []
    if rps < RPS_MIN:
        failures.append(f"RPS {rps:.1f} < required {RPS_MIN}")
    if p50 > P50_MAX_MS:
        failures.append(f"p50 {p50:.2f}ms > required <= {P50_MAX_MS}ms")
    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1

    print("PASS")

    # Bonus: confirm chain still verifies after the burst.
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{BASE_URL}/v1/verify")
        verify = r.json()
        if not verify["ok"]:
            print(f"WARN: chain reported BROKEN after load (first_break_seq={verify.get('first_break_seq')})", file=sys.stderr)
            return 1
        print(f"Chain verified after load: head_seq={verify['head_seq']}, total={verify['total_records']}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
