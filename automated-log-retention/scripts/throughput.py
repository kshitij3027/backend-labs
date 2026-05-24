#!/usr/bin/env python3
"""Throughput harness for the Automated Log Retention service.

Phase 1: ingest TOTAL_REQUESTS records via POST /v1/logs/ingest using
         a configurable async concurrency level. Asserts ingest_rps >=
         RPS_MIN.
Phase 2: trigger POST /v1/evaluate and time the response. Asserts
         eval_seconds < EVAL_MAX_S.
Phase 3: print summary (p50/p95/p99 latency, total elapsed, RPS).

Exits 0 on PASS, 1 on any miss. Env vars:
  BASE_URL          (default http://localhost:8000)
  TOTAL_REQUESTS    (default 10000)
  BATCH_SIZE        (default 100)
  CONCURRENCY       (default 20)
  RPS_MIN           (default 1000)
  EVAL_MAX_S        (default 30.0)
  P50_MAX_MS        (default 100.0)
"""
import asyncio
import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
TOTAL_REQUESTS = int(os.environ.get("TOTAL_REQUESTS", "10000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "20"))
RPS_MIN = float(os.environ.get("RPS_MIN", "1000"))
EVAL_MAX_S = float(os.environ.get("EVAL_MAX_S", "30.0"))
P50_MAX_MS = float(os.environ.get("P50_MAX_MS", "100.0"))


def _make_batch(start_idx: int, count: int, now: datetime) -> dict:
    records = []
    for i in range(count):
        # Spread ts across a 400-day window so the demo policies' phases
        # eventually have something to do.
        ts = now - timedelta(days=(start_idx + i) % 400, seconds=(start_idx + i))
        records.append({
            "ts": ts.isoformat(),
            "level": "INFO",
            "source": "throughput",
            "category": "user_activity",
            "message": f"perf record {start_idx + i}",
        })
    return {"records": records}


async def _post_batch(client: httpx.AsyncClient, payload: dict) -> tuple[bool, float]:
    t0 = time.perf_counter()
    try:
        r = await client.post(f"{BASE_URL}/v1/logs/ingest", json=payload, timeout=30.0)
        ok = r.status_code == 200
    except httpx.HTTPError:
        ok = False
    dt = (time.perf_counter() - t0) * 1000  # ms
    return ok, dt


async def main() -> int:
    print(
        f"throughput: BASE_URL={BASE_URL} TOTAL={TOTAL_REQUESTS} "
        f"BATCH={BATCH_SIZE} CONCURRENCY={CONCURRENCY}"
    )

    # Phase 0: confirm the service is up before piling load on it.
    now = datetime.now(timezone.utc).replace(microsecond=0)
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BASE_URL}/api/health", timeout=10.0)
        r.raise_for_status()

    # Phase 1: ingest
    n_batches = (TOTAL_REQUESTS + BATCH_SIZE - 1) // BATCH_SIZE
    payloads = [
        _make_batch(
            i * BATCH_SIZE,
            min(BATCH_SIZE, TOTAL_REQUESTS - i * BATCH_SIZE),
            now,
        )
        for i in range(n_batches)
    ]
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies: list[float] = []

    async def _bound(client: httpx.AsyncClient, payload: dict) -> bool:
        async with sem:
            ok, dt = await _post_batch(client, payload)
            latencies.append(dt)
            return ok

    t_ingest_start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*(_bound(client, p) for p in payloads))
    t_ingest_elapsed = time.perf_counter() - t_ingest_start
    errors = sum(1 for ok in results if not ok)
    ingest_rps = TOTAL_REQUESTS / t_ingest_elapsed if t_ingest_elapsed > 0 else 0

    p50 = statistics.median(latencies)
    p95 = statistics.quantiles(latencies, n=20)[-1] if len(latencies) > 20 else max(latencies)
    p99 = statistics.quantiles(latencies, n=100)[-1] if len(latencies) > 100 else max(latencies)

    print(
        f"throughput: ingest {TOTAL_REQUESTS} records in {t_ingest_elapsed:.2f}s "
        f"-> {ingest_rps:.1f} RPS (errors={errors})"
    )
    print(f"throughput: latency p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms")

    # Phase 2: evaluate
    async with httpx.AsyncClient(timeout=120.0) as client:
        t_eval_start = time.perf_counter()
        r = await client.post(f"{BASE_URL}/v1/evaluate")
        eval_elapsed = time.perf_counter() - t_eval_start
        r.raise_for_status()
        eval_body = r.json()
        print(f"throughput: /v1/evaluate took {eval_elapsed:.2f}s body={eval_body}")

    # Assertions
    failures: list[str] = []
    if ingest_rps < RPS_MIN:
        failures.append(f"ingest_rps {ingest_rps:.1f} < {RPS_MIN}")
    if eval_elapsed > EVAL_MAX_S:
        failures.append(f"eval_seconds {eval_elapsed:.2f} > {EVAL_MAX_S}")
    if p50 > P50_MAX_MS:
        failures.append(f"p50_ms {p50:.1f} > {P50_MAX_MS}")
    if errors > 0:
        failures.append(f"{errors} ingest errors")

    if failures:
        print(f"throughput: FAIL - {'; '.join(failures)}", file=sys.stderr)
        return 1
    print("throughput: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
