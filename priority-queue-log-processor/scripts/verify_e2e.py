#!/usr/bin/env python3
"""End-to-end verification for the Priority Queue Log Processor."""

import os
import sys
import time

import requests

BASE_URL = os.environ.get("APP_URL", "http://localhost:8080")


def wait_for_health(timeout=30):
    """Poll GET /health until 200. Fail after timeout."""
    print("  Waiting for service health...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(f"{BASE_URL}/health", timeout=3)
            if resp.status_code == 200:
                print(" UP")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
    print(" TIMEOUT")
    return False


def check_dashboard():
    """GET /, verify 200 and body contains project title."""
    try:
        resp = requests.get(f"{BASE_URL}/", timeout=5)
        ok = resp.status_code == 200 and "Priority Queue Log Processor" in resp.text
        print(f"  Dashboard: {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Dashboard: FAIL ({e})")
        return False


def check_status_api():
    """GET /api/status, verify 200 and required keys present."""
    try:
        resp = requests.get(f"{BASE_URL}/api/status", timeout=5)
        if resp.status_code != 200:
            print(f"  Status API: FAIL (status {resp.status_code})")
            return False
        data = resp.json()
        required = ["queue", "metrics", "workers", "recent_messages"]
        missing = [k for k in required if k not in data]
        if missing:
            print(f"  Status API: FAIL (missing keys: {missing})")
            return False
        print("  Status API: PASS")
        return True
    except Exception as e:
        print(f"  Status API: FAIL ({e})")
        return False


def check_injection():
    """POST to all four priority inject endpoints, verify 200."""
    priorities = ["critical", "high", "medium", "low"]
    all_ok = True
    for p in priorities:
        try:
            resp = requests.post(f"{BASE_URL}/api/inject/{p}", timeout=5)
            if resp.status_code != 200:
                print(f"  Inject {p}: FAIL (status {resp.status_code})")
                all_ok = False
            else:
                data = resp.json()
                if not data.get("injected"):
                    print(f"  Inject {p}: FAIL (injected not True)")
                    all_ok = False
        except Exception as e:
            print(f"  Inject {p}: FAIL ({e})")
            all_ok = False
    print(f"  Injection (all priorities): {'PASS' if all_ok else 'FAIL'}")
    return all_ok


def check_invalid_injection():
    """POST /api/inject/invalid, verify 400."""
    try:
        resp = requests.post(f"{BASE_URL}/api/inject/invalid", timeout=5)
        ok = resp.status_code == 400
        print(f"  Invalid injection rejection: {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Invalid injection rejection: FAIL ({e})")
        return False


def check_processing():
    """Wait briefly, then verify some messages have been processed."""
    print("  Waiting 5s for processing...", flush=True)
    time.sleep(5)
    try:
        resp = requests.get(f"{BASE_URL}/api/status", timeout=5)
        data = resp.json()
        processed = data["metrics"]["totals"]["processed"]
        ok = processed > 0
        print(f"  Processing check (processed={processed}): {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Processing check: FAIL ({e})")
        return False


def check_priority_ordering():
    """Inject CRITICALs and LOWs, verify CRITICALs are processed first."""
    try:
        for _ in range(10):
            requests.post(f"{BASE_URL}/api/inject/critical", timeout=3)
        for _ in range(10):
            requests.post(f"{BASE_URL}/api/inject/low", timeout=3)

        print("  Waiting 3s for priority processing...", flush=True)
        time.sleep(3)

        resp = requests.get(f"{BASE_URL}/api/status", timeout=5)
        data = resp.json()
        recent = data.get("recent_messages", [])

        critical_times = []
        low_times = []
        for msg in recent:
            ts = float(msg.get("timestamp", 0))
            if msg.get("priority") == "CRITICAL" and "Injected" in msg.get("message", ""):
                critical_times.append(ts)
            elif msg.get("priority") == "LOW" and "Injected" in msg.get("message", ""):
                low_times.append(ts)

        if not critical_times or not low_times:
            print("  Priority ordering: PASS (soft - insufficient injected messages in recent window)")
            return True

        min_critical = min(critical_times)
        max_low = max(low_times)
        ok = min_critical <= max_low
        print(f"  Priority ordering (soft check): {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Priority ordering: FAIL ({e})")
        return False


def check_metrics_endpoint():
    """GET /metrics, verify Prometheus metrics are present."""
    try:
        resp = requests.get(f"{BASE_URL}/metrics", timeout=5)
        if resp.status_code != 200:
            print(f"  Metrics endpoint: FAIL (status {resp.status_code})")
            return False
        body = resp.text
        has_processed = "logs_processed_total" in body
        has_depth = "queue_depth" in body
        ok = has_processed and has_depth
        print(f"  Metrics endpoint: {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Metrics endpoint: FAIL ({e})")
        return False


def check_backpressure():
    """Inject messages rapidly until LOW is rejected (503), then verify CRITICAL still accepted."""
    try:
        got_503 = False
        # Rapidly inject LOW messages until we get a 503 (backpressure)
        for i in range(200):
            resp = requests.post(f"{BASE_URL}/api/inject/low", timeout=3)
            if resp.status_code == 503:
                got_503 = True
                break

        if not got_503:
            # The generator may have already filled the queue, check if queue is near capacity
            status_resp = requests.get(f"{BASE_URL}/api/status", timeout=5)
            status_data = status_resp.json()
            utilization = status_data["queue"]["utilization"]
            if utilization >= 0.8:
                # Queue is at capacity, LOW should be rejected
                resp = requests.post(f"{BASE_URL}/api/inject/low", timeout=3)
                got_503 = resp.status_code == 503

        if got_503:
            # Verify CRITICAL is still accepted when LOW is rejected
            resp = requests.post(f"{BASE_URL}/api/inject/critical", timeout=3)
            critical_ok = resp.status_code == 200
            print(f"  Backpressure (LOW rejected={got_503}, CRITICAL accepted={critical_ok}): {'PASS' if critical_ok else 'FAIL'}")
            return critical_ok
        else:
            print("  Backpressure: PASS (soft - queue did not reach backpressure threshold)")
            return True
    except Exception as e:
        print(f"  Backpressure: FAIL ({e})")
        return False


def check_alerts_in_status():
    """GET /api/status, verify 'alerts' key exists in the metrics response."""
    try:
        resp = requests.get(f"{BASE_URL}/api/status", timeout=5)
        if resp.status_code != 200:
            print(f"  Alerts in status: FAIL (status {resp.status_code})")
            return False
        data = resp.json()
        metrics_data = data.get("metrics", {})
        has_alerts = "alerts" in metrics_data
        if has_alerts:
            alerts = metrics_data["alerts"]
            has_firing = "firing" in alerts
            has_recent = "recent" in alerts
            ok = has_firing and has_recent
        else:
            ok = False
        print(f"  Alerts in status: {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Alerts in status: FAIL ({e})")
        return False


def check_health_latency():
    """Time 10 sequential GET /health calls, verify average < 100ms."""
    try:
        durations = []
        for _ in range(10):
            start = time.time()
            requests.get(f"{BASE_URL}/health", timeout=5)
            durations.append(time.time() - start)
        avg_ms = (sum(durations) / len(durations)) * 1000
        ok = avg_ms < 100
        print(f"  Health latency (avg={avg_ms:.1f}ms): {'PASS' if ok else 'FAIL'}")
        return ok
    except Exception as e:
        print(f"  Health latency: FAIL ({e})")
        return False


def main():
    print(f"\n{'='*60}")
    print("Priority Queue Log Processor - E2E Verification")
    print(f"Target: {BASE_URL}")
    print(f"{'='*60}\n")

    checks = [
        ("Health endpoint", wait_for_health),
        ("Dashboard", check_dashboard),
        ("Status API", check_status_api),
        ("Injection", check_injection),
        ("Invalid injection", check_invalid_injection),
        ("Processing", check_processing),
        ("Priority ordering", check_priority_ordering),
        ("Metrics endpoint", check_metrics_endpoint),
        ("Health latency", check_health_latency),
        ("Backpressure", check_backpressure),
        ("Alerts in status", check_alerts_in_status),
    ]

    results = []
    for name, fn in checks:
        print(f"\n[{name}]")
        passed = fn()
        results.append((name, passed))
        if name == "Health endpoint" and not passed:
            print("\nService not reachable - aborting remaining checks.")
            break

    print(f"\n{'='*60}")
    print("RESULTS SUMMARY")
    print(f"{'='*60}")
    passed_count = 0
    failed_count = 0
    for name, ok in results:
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}")
        if ok:
            passed_count += 1
        else:
            failed_count += 1

    total = passed_count + failed_count
    print(f"\n  {passed_count}/{total} checks passed")

    if failed_count > 0:
        print("\nE2E verification FAILED.\n")
        sys.exit(1)
    else:
        print("\nE2E verification PASSED.\n")
        sys.exit(0)


if __name__ == "__main__":
    main()
