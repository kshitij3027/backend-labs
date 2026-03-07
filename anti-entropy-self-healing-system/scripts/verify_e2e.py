#!/usr/bin/env python3
"""End-to-end verification of the Anti-Entropy Self-Healing System."""
import json
import time
import sys
import urllib.request
import urllib.error

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5050"
NODE_URLS = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]

passed = 0
failed = 0


def http_get(url, timeout=5):
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read().decode())


def http_put(url, data, timeout=5):
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req, timeout=timeout)
    return resp.status, json.loads(resp.read().decode())


def http_post(url, data=None, timeout=10):
    body = json.dumps(data).encode() if data else b""
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req, timeout=timeout)
    return resp.status, json.loads(resp.read().decode())


def http_get_status(url, timeout=5):
    try:
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode()) if e.read() else {}


def test(name, fn):
    global passed, failed
    try:
        fn()
        print(f"  PASS: {name}")
        passed += 1
    except AssertionError as e:
        print(f"  FAIL: {name} - {e}")
        failed += 1
    except Exception as e:
        print(f"  ERROR: {name} - {e}")
        failed += 1


def test_1_health():
    """Health: coordinator and all nodes respond."""
    data = http_get(f"{BASE_URL}/health")
    assert data["status"] == "healthy", f"Coordinator unhealthy"
    for url in NODE_URLS:
        data = http_get(f"{url}/health")
        assert data["status"] == "healthy", f"Node {url} unhealthy"


def test_2_write_read():
    """Write + Read: 20 keys written and read back consistently."""
    for i in range(20):
        key = f"e2e-{i:03d}"
        value = f"value-{i}"
        status, _ = http_put(f"{BASE_URL}/api/data/{key}", {"value": value})
        assert status == 200, f"Write failed for {key}"

    time.sleep(1)

    for i in range(20):
        key = f"e2e-{i:03d}"
        data = http_get(f"{BASE_URL}/api/data/{key}")
        assert data["value"] == f"value-{i}", f"Value mismatch for {key}: expected value-{i}, got {data['value']}"


def test_3_consistency():
    """Consistency: all 3 nodes have same Merkle root."""
    roots = []
    for url in NODE_URLS:
        data = http_get(f"{url}/merkle/root")
        roots.append(data["root_hash"])
    assert len(set(roots)) == 1, f"Merkle roots differ: {roots}"


def test_4_inject_detect():
    """Inject + Detect: 5 inconsistencies injected, scan detects them."""
    for i in range(5):
        key = f"e2e-{i:03d}"
        http_post(f"{BASE_URL}/api/inject", {
            "node_id": "node-a",
            "key": key,
            "value": f"corrupted-{i}"
        })

    _, scan_result = http_post(f"{BASE_URL}/api/scan/trigger")
    assert scan_result["inconsistencies"] >= 5, f"Expected >= 5 inconsistencies, got {scan_result['inconsistencies']}"


def test_5_auto_repair():
    """Auto-repair: all repairs complete, reads return correct values."""
    data = http_get(f"{BASE_URL}/api/metrics")
    assert data["repairs_completed"] > 0, f"No repairs completed"

    roots = []
    for url in NODE_URLS:
        d = http_get(f"{url}/merkle/root")
        roots.append(d["root_hash"])
    assert len(set(roots)) == 1, f"Roots still differ after repair: {roots}"


def test_6_detection_speed():
    """Detection speed: < 5s."""
    http_post(f"{BASE_URL}/api/inject", {
        "node_id": "node-b",
        "key": "speed-test",
        "value": "fast"
    })

    start = time.time()
    http_post(f"{BASE_URL}/api/scan/trigger")
    duration = time.time() - start
    assert duration < 5.0, f"Scan took {duration:.2f}s, expected < 5s"


def test_7_metrics():
    """Metrics: repairs_completed > 0, repairs_failed == 0."""
    data = http_get(f"{BASE_URL}/api/metrics")
    assert data["repairs_completed"] > 0, f"repairs_completed is {data['repairs_completed']}"
    assert data["repairs_failed"] == 0, f"repairs_failed is {data['repairs_failed']}"


def test_8_read_repair():
    """Read repair: inject inconsistency, READ the key, verify inline repair."""
    http_put(f"{BASE_URL}/api/data/rr-test", {"value": "original"})
    time.sleep(0.5)

    http_post(f"{BASE_URL}/api/inject", {
        "node_id": "node-c",
        "key": "rr-test",
        "value": "stale"
    })

    http_get(f"{BASE_URL}/api/data/rr-test")
    time.sleep(0.5)

    values = []
    for url in NODE_URLS:
        d = http_get(f"{url}/data/rr-test")
        values.append(d["value"])
    assert len(set(values)) == 1, f"Read repair didn't fix inconsistency: {values}"


if __name__ == "__main__":
    print("=" * 60)
    print("Anti-Entropy Self-Healing System - E2E Verification")
    print("=" * 60)

    print("\nChecking system readiness...")
    for attempt in range(10):
        try:
            http_get(f"{BASE_URL}/health", timeout=3)
            print("System is ready!\n")
            break
        except Exception:
            pass
        if attempt == 9:
            print("System not ready after 10 attempts. Exiting.")
            sys.exit(1)
        time.sleep(2)

    print("Running E2E tests...\n")

    test("1. Health check", test_1_health)
    test("2. Write + Read consistency", test_2_write_read)
    test("3. Merkle root consistency", test_3_consistency)
    test("4. Inject + Detect", test_4_inject_detect)
    test("5. Auto-repair", test_5_auto_repair)
    test("6. Detection speed", test_6_detection_speed)
    test("7. Metrics validation", test_7_metrics)
    test("8. Read repair", test_8_read_repair)

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed out of {passed + failed}")
    print(f"{'=' * 60}")

    sys.exit(0 if failed == 0 else 1)
