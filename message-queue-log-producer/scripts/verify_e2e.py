"""End-to-end verification script for the log producer pipeline."""

import os
import sys
import time
import json
import requests

APP_HOST = os.environ.get("APP_HOST", "localhost")
APP_PORT = os.environ.get("APP_PORT", "8080")
BASE_URL = f"http://{APP_HOST}:{APP_PORT}"


def wait_for_service(url, timeout=60):
    """Wait for the service to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{url}/health", timeout=2)
            if resp.status_code == 200:
                print(f"Service is up: {resp.json()}")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(2)
    return False


def test_health():
    """Verify health endpoint returns expected fields."""
    resp = requests.get(f"{BASE_URL}/health")
    assert resp.status_code == 200, f"Health check failed: {resp.status_code}"
    data = resp.json()
    assert "healthy" in data, "Missing 'healthy' field"
    assert "circuit_breaker" in data, "Missing 'circuit_breaker' field"
    print(f"PASS: Health check: {data}")
    return True


def test_single_log():
    """Post a single log entry."""
    payload = {"level": "info", "message": "E2E test single", "source": "verify_e2e"}
    resp = requests.post(f"{BASE_URL}/logs", json=payload)
    assert resp.status_code == 202, f"Single log failed: {resp.status_code}"
    data = resp.json()
    assert data["accepted"] == 1, f"Expected 1 accepted, got {data['accepted']}"
    print("PASS: Single log accepted")
    return True


def test_batch_logs():
    """Post a batch of log entries."""
    payload = [
        {"level": "info", "message": f"E2E batch {i}", "source": "verify_e2e"}
        for i in range(10)
    ]
    resp = requests.post(f"{BASE_URL}/logs", json=payload)
    assert resp.status_code == 202, f"Batch log failed: {resp.status_code}"
    data = resp.json()
    assert data["accepted"] == 10, f"Expected 10 accepted, got {data['accepted']}"
    print("PASS: Batch of 10 logs accepted")
    return True


def test_invalid_log():
    """Post invalid log entry, expect 400."""
    resp = requests.post(f"{BASE_URL}/logs", json={"bad": "data"})
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    print("PASS: Invalid log rejected with 400")
    return True


def test_metrics():
    """Verify metrics endpoint returns data after sending logs."""
    # Give pipeline time to process
    time.sleep(3)
    resp = requests.get(f"{BASE_URL}/metrics")
    assert resp.status_code == 200, f"Metrics failed: {resp.status_code}"
    data = resp.json()
    assert data["messages_received"] >= 11, f"Expected >= 11 received, got {data['messages_received']}"
    print(f"PASS: Metrics: received={data['messages_received']}, published={data['messages_published']}")
    return True


def main():
    print("=" * 60)
    print("E2E Verification: Message Queue Log Producer")
    print("=" * 60)

    if not wait_for_service(BASE_URL):
        print("FAIL: Service did not start within timeout")
        sys.exit(1)

    tests = [test_health, test_single_log, test_batch_logs, test_invalid_log, test_metrics]
    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"FAIL: {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR: {test.__name__}: {e}")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
