"""Load test for the Log Pattern Alerting System.

Sends concurrent POST /test/inject_log requests with varied log messages
and measures throughput, response times (p50/p95/p99), and success rate.

Configuration via environment variables:
  APP_URL          - Target base URL (default: http://app:8000)
  CONCURRENCY      - Max parallel requests (default: 20)
  TOTAL_REQUESTS   - Total requests to send (default: 200)
  RAMP_UP          - Ramp-up period in seconds (default: 5)
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import time

import httpx

APP_URL = os.environ.get("APP_URL", "http://app:8000")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "20"))
TOTAL_REQUESTS = int(os.environ.get("TOTAL_REQUESTS", "200"))
RAMP_UP = int(os.environ.get("RAMP_UP", "5"))


def make_log_message(index: int) -> dict:
    """Generate a varied log message payload for request *index*."""
    templates = [
        {"message": f"Authentication failed for user_{index}", "level": "ERROR", "source": "auth-service"},
        {"message": f"Database error on query #{index}", "level": "ERROR", "source": "db-service"},
        {"message": f"API error on endpoint /api/v{index}", "level": "ERROR", "source": "api-gateway"},
        {"message": f"Connection timeout to db replica {index}", "level": "ERROR", "source": "db-service"},
        {"message": f"Normal log message #{index}", "level": "INFO", "source": "app-service"},
    ]
    return random.choice(templates)


# ------------------------------------------------------------------
# Phase 1: Warm-up -- wait for the health endpoint
# ------------------------------------------------------------------

async def wait_for_health(client: httpx.AsyncClient, timeout: int = 60) -> bool:
    """Poll /health until the app is ready."""
    print("Phase 1: Warm-up -- waiting for health endpoint", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = await client.get(f"{APP_URL}/health", timeout=5)
            data = resp.json()
            if (
                data.get("status") == "healthy"
                and data.get("database") == "connected"
                and data.get("redis") == "connected"
            ):
                print(f"  App healthy after {time.time() - start:.1f}s", flush=True)
                return True
        except Exception:
            pass
        await asyncio.sleep(1)
    return False


# ------------------------------------------------------------------
# Phase 2: Concurrent log injection
# ------------------------------------------------------------------

async def send_request(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    index: int,
    results: list[dict],
):
    """Send a single inject_log request, recording its outcome."""
    payload = make_log_message(index)
    async with semaphore:
        start = time.monotonic()
        try:
            resp = await client.post(
                f"{APP_URL}/test/inject_log",
                json=payload,
                timeout=30,
            )
            elapsed_ms = (time.monotonic() - start) * 1000
            results.append({
                "index": index,
                "status": resp.status_code,
                "elapsed_ms": elapsed_ms,
                "success": resp.status_code == 200,
            })
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            results.append({
                "index": index,
                "status": 0,
                "elapsed_ms": elapsed_ms,
                "success": False,
                "error": str(exc),
            })

    # Print progress every 25 requests
    completed = len(results)
    if completed % 25 == 0 or completed == TOTAL_REQUESTS:
        print(f"  Progress: {completed}/{TOTAL_REQUESTS} requests sent", flush=True)


async def run_load_test(client: httpx.AsyncClient) -> list[dict]:
    """Fire TOTAL_REQUESTS concurrent inject_log requests with ramp-up."""
    print(f"\nPhase 2: Concurrent log injection", flush=True)
    print(f"  Total requests: {TOTAL_REQUESTS}", flush=True)
    print(f"  Concurrency:    {CONCURRENCY}", flush=True)
    print(f"  Ramp-up:        {RAMP_UP}s", flush=True)

    semaphore = asyncio.Semaphore(CONCURRENCY)
    results: list[dict] = []
    tasks: list[asyncio.Task] = []

    # Calculate delay between task spawns during ramp-up
    ramp_delay = RAMP_UP / max(TOTAL_REQUESTS, 1)

    for i in range(TOTAL_REQUESTS):
        task = asyncio.create_task(send_request(client, semaphore, i, results))
        tasks.append(task)
        if ramp_delay > 0 and i < TOTAL_REQUESTS - 1:
            await asyncio.sleep(ramp_delay)

    await asyncio.gather(*tasks)
    return results


# ------------------------------------------------------------------
# Phase 3: Measure and report results
# ------------------------------------------------------------------

def percentile(sorted_data: list[float], pct: float) -> float:
    """Return the *pct*-th percentile from an already-sorted list."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (pct / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


async def measure_results(
    client: httpx.AsyncClient,
    results: list[dict],
    wall_time: float,
) -> bool:
    """Compute and print a formatted summary. Return True if pass criteria met."""
    print("\nPhase 3: Measure results", flush=True)

    total = len(results)
    successes = sum(1 for r in results if r["success"])
    failures = total - successes
    success_rate = (successes / total * 100) if total > 0 else 0.0

    latencies = sorted(r["elapsed_ms"] for r in results if r["success"])
    p50 = percentile(latencies, 50) if latencies else 0.0
    p95 = percentile(latencies, 95) if latencies else 0.0
    p99 = percentile(latencies, 99) if latencies else 0.0
    min_lat = min(latencies) if latencies else 0.0
    max_lat = max(latencies) if latencies else 0.0

    throughput = total / wall_time if wall_time > 0 else 0.0

    # Fetch alert count
    alert_count = 0
    try:
        resp = await client.get(f"{APP_URL}/alerts", timeout=10)
        if resp.status_code == 200:
            alert_count = len(resp.json())
    except Exception:
        pass

    # Print formatted summary
    print("\n" + "=" * 60, flush=True)
    print("  LOAD TEST SUMMARY", flush=True)
    print("=" * 60, flush=True)
    print(f"  Target URL:          {APP_URL}", flush=True)
    print(f"  Concurrency:         {CONCURRENCY}", flush=True)
    print(f"  Total Requests:      {total}", flush=True)
    print(f"  Successful:          {successes}", flush=True)
    print(f"  Failed:              {failures}", flush=True)
    print(f"  Success Rate:        {success_rate:.1f}%", flush=True)
    print("-" * 60, flush=True)
    print(f"  Wall Time:           {wall_time:.2f}s", flush=True)
    print(f"  Throughput:          {throughput:.1f} req/s", flush=True)
    print("-" * 60, flush=True)
    print(f"  Response Time (ms):", flush=True)
    print(f"    Min:               {min_lat:.1f}", flush=True)
    print(f"    p50:               {p50:.1f}", flush=True)
    print(f"    p95:               {p95:.1f}", flush=True)
    print(f"    p99:               {p99:.1f}", flush=True)
    print(f"    Max:               {max_lat:.1f}", flush=True)
    print("-" * 60, flush=True)
    print(f"  Alerts Created:      {alert_count}", flush=True)
    print("=" * 60, flush=True)

    # Pass criteria: success rate >= 95% and p95 < 5000ms
    passed = success_rate >= 95.0 and p95 < 5000.0
    if passed:
        print("\n  RESULT: PASS", flush=True)
    else:
        reasons = []
        if success_rate < 95.0:
            reasons.append(f"success rate {success_rate:.1f}% < 95%")
        if p95 >= 5000.0:
            reasons.append(f"p95 {p95:.1f}ms >= 5000ms")
        print(f"\n  RESULT: FAIL ({', '.join(reasons)})", flush=True)

    print("=" * 60 + "\n", flush=True)
    return passed


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def main():
    print(f"Load Test -- Log Pattern Alerting System", flush=True)
    print(f"Target: {APP_URL}", flush=True)

    async with httpx.AsyncClient() as client:
        # Phase 1: warm-up
        healthy = await wait_for_health(client)
        if not healthy:
            print("FAIL: App did not become healthy within timeout", flush=True)
            sys.exit(1)

        # Phase 2: concurrent injection
        wall_start = time.monotonic()
        results = await run_load_test(client)
        wall_time = time.monotonic() - wall_start

        # Phase 3: measure
        passed = await measure_results(client, results, wall_time)

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    asyncio.run(main())
