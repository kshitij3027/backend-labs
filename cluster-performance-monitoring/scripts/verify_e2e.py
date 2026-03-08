#!/usr/bin/env python3
"""End-to-end verification script for the cluster performance monitoring system.

Run against a live server at http://localhost:8080.
Usage: python scripts/verify_e2e.py
"""

import sys
import time
import urllib.request
import urllib.error
import json

BASE_URL = "http://localhost:8080"
PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} — {detail}")

def get(path):
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=10)
    return json.loads(resp.read().decode())

def post(path, data=None):
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, method="POST")
    if data:
        req.data = json.dumps(data).encode()
        req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req, timeout=10)
    return json.loads(resp.read().decode())

def get_html(path):
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=10)
    return resp.read().decode()

def wait_for_ready(max_wait=30):
    """Wait for the server to be ready."""
    print("Waiting for server readiness...")
    for i in range(max_wait):
        try:
            data = get("/health")
            if data.get("status") == "ok":
                print(f"  Server ready after {i+1}s")
                return True
        except Exception:
            pass
        time.sleep(1)
    print("  Server not ready after waiting")
    return False

def main():
    global PASS, FAIL

    if not wait_for_ready():
        print("\nFAILED: Server not reachable")
        sys.exit(1)

    # Wait for metrics to accumulate
    print("Waiting 10s for metrics to accumulate...")
    time.sleep(10)

    # Test 1: Health endpoint
    print("\n1. Health Endpoint")
    data = get("/health")
    check("status is ok", data.get("status") == "ok")
    check("nodes count is 3", data.get("nodes") == 3)

    # Test 2: Metrics endpoint
    print("\n2. Metrics Endpoint")
    data = get("/api/metrics")
    check("avg_cpu_usage present", "avg_cpu_usage" in data)
    check("avg_memory_usage present", "avg_memory_usage" in data)
    check("total_throughput > 0", data.get("total_throughput", 0) > 0)
    check("active_nodes is 3", data.get("active_nodes") == 3)

    # Test 3: Nodes endpoint
    print("\n3. Nodes Endpoint")
    data = get("/api/nodes")
    check("3 nodes returned", len(data) == 3)
    roles = {n["role"] for n in data}
    check("primary role exists", "primary" in roles)
    check("replica role exists", "replica" in roles)

    # Test 4: Node metrics endpoint
    print("\n4. Node Metrics")
    data = get("/api/nodes/node-1/metrics")
    check("node_id is node-1", data.get("node_id") == "node-1")
    check("metrics dict non-empty", len(data.get("metrics", {})) > 0)

    # Test 5: Alerts endpoint (before degradation — may or may not have alerts)
    print("\n5. Alerts Endpoint")
    data = get("/api/alerts")
    check("alerts key present", "alerts" in data)
    check("count key present", "count" in data)

    # Test 6: Degradation
    print("\n6. Degradation Simulation")
    data = post("/api/simulate/degrade?scenario=high_load")
    check("degradation injected", data.get("status") == "degradation_injected")

    # Wait for degraded metrics to accumulate (need several collection cycles
    # so that the windowed average crosses thresholds)
    time.sleep(20)

    data = get("/api/alerts")
    check("alerts generated after degradation", data.get("count", 0) > 0, f"count={data.get('count')}")

    # Test 7: Recovery
    print("\n7. Recovery")
    data = post("/api/simulate/recover")
    check("recovery confirmed", data.get("status") == "recovered")

    # Test 8: Report generation
    print("\n8. Report Generation")
    data = post("/api/report/generate")
    check("report_id present", "report_id" in data)
    check("report_id starts with perf_report_", data.get("report_id", "").startswith("perf_report_"))
    check("cluster_health in report", "cluster_health" in data)

    # Test 9: Get report
    print("\n9. Get Latest Report")
    data = get("/api/report")
    check("report retrieved", "report_id" in data)

    # Test 10: Dashboard
    print("\n10. Dashboard")
    html = get_html("/dashboard")
    check("HTML returned", "<!DOCTYPE html>" in html or "<html" in html)
    check("Chart.js referenced", "chart.js" in html.lower())
    check("Performance Monitor in title", "performance" in html.lower())

    # Summary
    total = PASS + FAIL
    print(f"\n{'='*50}")
    print(f"Results: {PASS}/{total} passed, {FAIL}/{total} failed")
    print(f"{'='*50}")

    sys.exit(0 if FAIL == 0 else 1)

if __name__ == "__main__":
    main()
