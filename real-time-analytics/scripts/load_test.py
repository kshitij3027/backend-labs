#!/usr/bin/env python3
"""Load test for the Real-Time Analytics Dashboard."""

import os
import sys
import time
import asyncio
import argparse
import statistics

import httpx

APP_URL = os.getenv("APP_URL", "http://localhost:8000")


async def send_request(client, endpoint, method="GET", json_data=None, params=None):
    start = time.monotonic()
    try:
        if method == "POST":
            resp = await client.post(endpoint, json=json_data, params=params)
        else:
            resp = await client.get(endpoint, params=params)
        elapsed = (time.monotonic() - start) * 1000  # ms
        return elapsed, resp.status_code
    except Exception:
        elapsed = (time.monotonic() - start) * 1000
        return elapsed, 0


async def run_load_test(rate: int, duration: int):
    print(f"\nLoad Test: {rate} req/s for {duration}s against {APP_URL}")
    print("=" * 60)

    # Seed some data first
    async with httpx.AsyncClient(base_url=APP_URL, timeout=30) as client:
        await client.post("/api/generate-sample-data", params={"count": 100})

    latencies = []
    errors = 0
    total = 0
    interval = 1.0 / rate

    async with httpx.AsyncClient(base_url=APP_URL, timeout=30) as client:
        tasks = []
        start_time = time.monotonic()

        while time.monotonic() - start_time < duration:
            # Mix of requests: 50% ingest, 30% query, 20% other
            r = total % 10
            if r < 5:
                # Ingest batch
                log_data = {
                    "logs": [
                        {
                            "timestamp": time.time(),
                            "service": "load-test",
                            "response_time": 100.0 + (total % 50),
                            "method": "GET",
                            "endpoint": "/api/test",
                            "level": "INFO",
                        }
                    ]
                }
                task = asyncio.create_task(
                    send_request(
                        client, "/api/ingest", "POST", json_data=log_data
                    )
                )
            elif r < 8:
                task = asyncio.create_task(
                    send_request(
                        client,
                        "/api/metrics/load-test/response_time",
                        params={"minutes": 5},
                    )
                )
            else:
                task = asyncio.create_task(
                    send_request(client, "/health")
                )
            tasks.append(task)
            total += 1
            await asyncio.sleep(interval)

        results = await asyncio.gather(*tasks)

    for elapsed, status in results:
        latencies.append(elapsed)
        if status == 0 or status >= 500:
            errors += 1

    # Stats
    latencies.sort()
    n = len(latencies)
    p50 = latencies[int(n * 0.5)] if n else 0
    p95 = latencies[int(n * 0.95)] if n else 0
    p99 = latencies[int(n * 0.99)] if n else 0
    avg = statistics.mean(latencies) if latencies else 0

    print(f"\nRequests:  {total}")
    if total:
        print(f"Errors:    {errors} ({errors/total*100:.1f}%)")
    print(f"Throughput: {total/duration:.0f} req/s")
    print(f"\nLatency (ms):")
    print(f"  Avg:  {avg:.1f}")
    print(f"  P50:  {p50:.1f}")
    print(f"  P95:  {p95:.1f}")
    print(f"  P99:  {p99:.1f}")
    if latencies:
        print(f"  Min:  {min(latencies):.1f}")
        print(f"  Max:  {max(latencies):.1f}")
    print("=" * 60)

    if errors / max(total, 1) > 0.05:
        print("\nLOAD TEST FAILED: error rate > 5%")
        return 1
    if p99 > 1000:
        print("\nLOAD TEST WARNING: P99 > 1000ms")
    print("\nLOAD TEST PASSED")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--duration", type=int, default=10)
    args = parser.parse_args()
    sys.exit(asyncio.run(run_load_test(args.rate, args.duration)))
