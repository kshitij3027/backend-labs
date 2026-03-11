#!/usr/bin/env python3
"""End-to-end verification: generate logs -> consume -> verify stats."""

import os
import sys
import time

import httpx
import redis

APP_HOST = os.environ.get("APP_HOST", "app")
APP_PORT = os.environ.get("DASHBOARD_PORT", "8000")
BASE_URL = f"http://{APP_HOST}:{APP_PORT}"
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
STREAM_KEY = os.environ.get("STREAM_KEY", "logs:access")
NUM_LOGS = int(os.environ.get("NUM_LOGS", "100"))


def wait_for_health(max_wait=30):
    """Poll /health until healthy or timeout."""
    print(f"Waiting for app at {BASE_URL}/health...")
    start = time.time()
    while time.time() - start < max_wait:
        try:
            resp = httpx.get(f"{BASE_URL}/health", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                print(f"  Health: {data.get('status', 'unknown')}")
                if data.get("status") in ("healthy", "degraded"):
                    return True
        except Exception:
            pass
        time.sleep(1)
    print("FAIL: App did not become healthy in time.")
    return False


def generate_logs():
    """Generate test logs directly to Redis."""
    from scripts.generate_demo_logs import generate_log_line
    r = redis.from_url(REDIS_URL, decode_responses=True)
    print(f"Generating {NUM_LOGS} logs...")
    for _ in range(NUM_LOGS):
        line = generate_log_line()
        r.xadd(STREAM_KEY, {"log": line})
    print(f"  Generated {NUM_LOGS} logs. Stream length: {r.xlen(STREAM_KEY)}")


def wait_for_processing(expected_min, max_wait=30):
    """Poll /api/stats until total_processed >= expected_min."""
    print(f"Waiting for at least {expected_min} messages to be processed...")
    processed = 0
    start = time.time()
    while time.time() - start < max_wait:
        try:
            resp = httpx.get(f"{BASE_URL}/api/stats", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                processed = data.get("total_processed", 0)
                print(f"  Processed: {processed}")
                if processed >= expected_min:
                    return data
        except Exception:
            pass
        time.sleep(2)
    print(f"FAIL: Only processed {processed} of expected {expected_min}")
    return None


def validate_stats(stats):
    """Validate the stats response has expected structure."""
    checks = []

    checks.append(("total_processed > 0", stats.get("total_processed", 0) > 0))
    checks.append(("has endpoints", len(stats.get("endpoints", {})) > 0))
    checks.append(("has status_code_distribution", len(stats.get("status_code_distribution", {})) > 0))
    checks.append(("has top_paths", len(stats.get("top_paths", [])) > 0))
    checks.append(("has top_ips", len(stats.get("top_ips", [])) > 0))
    checks.append(("has latency_percentiles", len(stats.get("latency_percentiles", {})) > 0))
    checks.append(("has consumers", len(stats.get("consumers", [])) > 0))
    checks.append(("uptime_seconds > 0", stats.get("uptime_seconds", 0) > 0))

    all_pass = True
    for name, passed in checks:
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}")
        if not passed:
            all_pass = False

    return all_pass


def main():
    print("=" * 60)
    print("LOG CONSUMER SYSTEM -- E2E VERIFICATION")
    print("=" * 60)

    if not wait_for_health():
        sys.exit(1)

    generate_logs()

    # Wait for ~90% of non-malformed logs to be processed (5% are malformed)
    expected = int(NUM_LOGS * 0.85)
    stats = wait_for_processing(expected)
    if stats is None:
        sys.exit(1)

    print("\nValidating stats...")
    if validate_stats(stats):
        print("\nALL E2E CHECKS PASSED")
    else:
        print("\nSOME E2E CHECKS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
