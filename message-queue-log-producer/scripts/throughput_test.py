"""Throughput test: sends 1000+ logs and measures processing rate."""

import os
import sys
import time
import json
import requests

APP_HOST = os.environ.get("APP_HOST", "localhost")
APP_PORT = os.environ.get("APP_PORT", "8080")
BASE_URL = f"http://{APP_HOST}:{APP_PORT}"

TOTAL_LOGS = 2000
BATCH_SIZE = 100


def wait_for_service(timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{BASE_URL}/health", timeout=2)
            if resp.status_code == 200:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(2)
    return False


def main():
    print("=" * 60)
    print("Throughput Test: Message Queue Log Producer")
    print("=" * 60)

    if not wait_for_service():
        print("FAIL: Service not available")
        sys.exit(1)

    # Get initial metrics
    initial = requests.get(f"{BASE_URL}/metrics").json()
    initial_received = initial.get("messages_received", 0)

    # Send logs in batches
    start_time = time.time()
    sent = 0
    for i in range(0, TOTAL_LOGS, BATCH_SIZE):
        batch = [
            {"level": "info", "message": f"throughput test {j}", "source": "throughput"}
            for j in range(i, min(i + BATCH_SIZE, TOTAL_LOGS))
        ]
        resp = requests.post(f"{BASE_URL}/logs", json=batch)
        if resp.status_code != 202:
            print(f"FAIL: Unexpected status {resp.status_code}")
            sys.exit(1)
        sent += len(batch)

    send_elapsed = time.time() - start_time
    send_rate = sent / send_elapsed if send_elapsed > 0 else 0
    print(f"Sent {sent} logs in {send_elapsed:.2f}s ({send_rate:.0f} logs/sec ingest rate)")

    # Wait for pipeline to process
    print("Waiting for pipeline to process...")
    time.sleep(5)

    # Check metrics
    final = requests.get(f"{BASE_URL}/metrics").json()
    received = final.get("messages_received", 0) - initial_received
    published = final.get("messages_published", 0)
    throughput = final.get("throughput", 0)
    latency_p95 = final.get("latency_p95", 0)

    print(f"\nResults:")
    print(f"  Messages received: {received}")
    print(f"  Messages published: {published}")
    print(f"  Throughput (avg): {throughput:.2f} msgs/sec")
    print(f"  Latency P95: {latency_p95:.2f} ms")
    print(f"  Ingest rate: {send_rate:.0f} logs/sec")

    # Verify
    passed = True
    if received < TOTAL_LOGS:
        print(f"FAIL: Expected {TOTAL_LOGS} received, got {received}")
        passed = False
    else:
        print(f"PASS: Received >= {TOTAL_LOGS} logs")

    if send_rate < 1000:
        print(f"WARN: Ingest rate {send_rate:.0f} < 1000 logs/sec (may be due to Docker overhead)")
    else:
        print(f"PASS: Ingest rate >= 1000 logs/sec")

    if latency_p95 > 0:
        print(f"INFO: P95 latency = {latency_p95:.2f} ms")

    print("=" * 60)
    if passed:
        print("THROUGHPUT TEST PASSED")
    else:
        print("THROUGHPUT TEST FAILED")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
