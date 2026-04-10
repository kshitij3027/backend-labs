"""Diagnostic load driver for the sliding-window analytics engine.

Runs inside the ``loadtest`` docker-compose profile (never on the host
machine). Fires POST /api/metric at a configurable rate for a
configurable duration, then prints the final ingest counters and the
client-side p50/p99 latency.

Always exits 0 — this is a diagnostic tool, not a pass/fail test. The
``scripts/verify_e2e.py`` harness is the authoritative E2E check.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import time

import httpx

DEFAULT_URL = os.environ.get("APP_URL", "http://app:8000")
BATCH_SIZE = 25  # number of POSTs per "batch" tick


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    idx = max(0, min(len(values) - 1, int(len(values) * pct)))
    return sorted(values)[idx]


async def _wait_for_health(client: httpx.AsyncClient, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = await client.get("/api/health", timeout=2.0)
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return
        except Exception:
            pass
        await asyncio.sleep(1.0)
    raise RuntimeError("app never became healthy")


async def _fire(client: httpx.AsyncClient, latencies: list[float]) -> None:
    payload = {
        "metric": random.choice(("response_time", "throughput", "error_rate")),
        "value": random.uniform(1.0, 500.0),
        "metadata": {"source": "loadtest"},
    }
    t0 = _now_ms()
    try:
        await client.post("/api/metric", json=payload, timeout=5.0)
    except Exception:
        # Diagnostic tool: swallow and keep firing so the counters
        # printed at the end still reflect real throughput.
        return
    latencies.append(_now_ms() - t0)


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=1200, help="target events/sec")
    parser.add_argument("--duration", type=float, default=10.0, help="seconds to run")
    parser.add_argument("--url", type=str, default=DEFAULT_URL, help="APP_URL base")
    args = parser.parse_args()

    print(
        f"loadtest: targeting {args.rate} evt/s for {args.duration}s "
        f"against {args.url}",
        flush=True,
    )

    async with httpx.AsyncClient(base_url=args.url) as client:
        await _wait_for_health(client)
        print("loadtest: health OK, firing...", flush=True)

        latencies: list[float] = []
        deadline = time.monotonic() + args.duration
        tick_interval = BATCH_SIZE / args.rate

        while time.monotonic() < deadline:
            tick_start = time.monotonic()
            await asyncio.gather(*(_fire(client, latencies) for _ in range(BATCH_SIZE)))
            elapsed = time.monotonic() - tick_start
            if elapsed < tick_interval:
                await asyncio.sleep(tick_interval - elapsed)

        stats = (await client.get("/api/stats", timeout=5.0)).json()
        ingest = stats.get("ingest", {})

    p50 = _percentile(latencies, 0.50)
    p99 = _percentile(latencies, 0.99)
    total = len(latencies)

    print("\n=== loadtest results ===", flush=True)
    print(f"requests attempted:  {total}", flush=True)
    print(f"p50 latency (ms):    {p50:.2f}", flush=True)
    print(f"p99 latency (ms):    {p99:.2f}", flush=True)
    print(f"queue_depth:         {ingest.get('queue_depth', 'n/a')}", flush=True)
    print(f"queue_maxsize:       {ingest.get('queue_maxsize', 'n/a')}", flush=True)
    print(f"enqueued:            {ingest.get('enqueued', 'n/a')}", flush=True)
    print(f"processed:           {ingest.get('processed', 'n/a')}", flush=True)
    print(f"dropped:             {ingest.get('dropped', 'n/a')}", flush=True)
    print(f"sampled:             {ingest.get('sampled', 'n/a')}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
