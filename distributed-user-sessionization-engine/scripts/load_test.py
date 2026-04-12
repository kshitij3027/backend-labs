"""Load test script for the sessionization engine."""
import argparse
import asyncio
import random
import time
import statistics

import httpx

PAGES = ["/home", "/products", "/search", "/cart", "/checkout"]
EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "purchase"]
DEVICES = ["desktop", "mobile", "tablet"]


async def run_load_test(url: str, rate: int, duration: int):
    """Send events at target rate for duration seconds."""
    print(f"Load test: {rate} events/sec for {duration}s -> {url}/api/events")

    latencies = []
    errors = 0
    total = 0
    start = time.monotonic()
    interval = 1.0 / rate

    async with httpx.AsyncClient(base_url=url, timeout=10.0) as client:
        while time.monotonic() - start < duration:
            event = {
                "user_id": f"load_user_{random.randint(0, 99):03d}",
                "event_type": random.choice(EVENT_TYPES),
                "page_url": random.choice(PAGES),
                "device_type": random.choice(DEVICES),
            }
            t0 = time.monotonic()
            try:
                resp = await client.post("/api/events", json=event)
                latency = (time.monotonic() - t0) * 1000  # ms
                latencies.append(latency)
                total += 1
                if resp.status_code != 200:
                    errors += 1
            except Exception:
                errors += 1
                total += 1

            # Pace to target rate
            elapsed = time.monotonic() - start
            expected = total * interval
            if expected > elapsed:
                await asyncio.sleep(expected - elapsed)

    elapsed = time.monotonic() - start
    throughput = total / elapsed if elapsed > 0 else 0

    print(f"\n{'='*50}")
    print(f"Load Test Results")
    print(f"{'='*50}")
    print(f"Duration:    {elapsed:.1f}s")
    print(f"Total:       {total} events")
    print(f"Errors:      {errors} ({100*errors/max(total,1):.1f}%)")
    print(f"Throughput:  {throughput:.0f} events/sec")
    if latencies:
        latencies.sort()
        print(f"Latency p50: {statistics.median(latencies):.1f}ms")
        p99_idx = int(len(latencies) * 0.99)
        print(f"Latency p99: {latencies[p99_idx]:.1f}ms")
    print(f"{'='*50}")

    # Exit with error if too many failures
    error_rate = errors / max(total, 1)
    if error_rate > 0.01:
        print(f"FAIL: Error rate {error_rate:.1%} > 1%")
        exit(1)
    else:
        print("PASS: Error rate within acceptable limits")


def main():
    parser = argparse.ArgumentParser(description="Load test the sessionization engine")
    parser.add_argument("--url", default="http://engine:8000", help="Base URL")
    parser.add_argument("--rate", type=int, default=100, help="Events per second")
    parser.add_argument("--duration", type=int, default=10, help="Duration in seconds")
    args = parser.parse_args()

    # Wait for server to be ready
    import time as _time
    for attempt in range(30):
        try:
            resp = httpx.get(f"{args.url}/health", timeout=5.0)
            if resp.status_code == 200:
                break
        except Exception:
            pass
        _time.sleep(1)

    asyncio.run(run_load_test(args.url, args.rate, args.duration))


if __name__ == "__main__":
    main()
