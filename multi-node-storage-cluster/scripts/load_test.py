#!/usr/bin/env python3
"""Load test for the multi-node storage cluster.

Usage:
    python scripts/load_test.py --host localhost --port 5001 --requests 100 --concurrency 10
    python scripts/load_test.py --dashboard --port 8080 --requests 200 --concurrency 20
"""

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def write_log(url, i):
    """Send a single write request and return timing info."""
    payload = {
        "message": f"load test log entry {i}",
        "level": "info",
        "test_id": i,
        "timestamp": time.time(),
    }
    start = time.time()
    try:
        resp = requests.post(url, json=payload, timeout=10)
        elapsed = (time.time() - start) * 1000  # ms
        return {
            "status": resp.status_code,
            "elapsed_ms": elapsed,
            "success": resp.status_code in (200, 201),
            "file_path": resp.json().get("file_path") if resp.status_code in (200, 201) else None,
        }
    except requests.RequestException as e:
        elapsed = (time.time() - start) * 1000
        return {
            "status": 0,
            "elapsed_ms": elapsed,
            "success": False,
            "error": str(e),
        }


def run_load_test(url, num_requests, concurrency):
    """Run the load test and return results."""
    print(f"\nLoad Test Configuration:")
    print(f"  Target URL: {url}")
    print(f"  Total Requests: {num_requests}")
    print(f"  Concurrency: {concurrency}")
    print(f"  Starting...\n")

    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(write_log, url, i): i
            for i in range(num_requests)
        }

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            if completed % 50 == 0 or completed == num_requests:
                print(f"  Progress: {completed}/{num_requests}")

    total_time = time.time() - start_time

    # Calculate stats
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]
    latencies = [r["elapsed_ms"] for r in successes]

    print(f"\n{'=' * 50}")
    print(f"LOAD TEST RESULTS")
    print(f"{'=' * 50}")
    print(f"Total Time:      {total_time:.2f}s")
    print(f"Total Requests:  {num_requests}")
    print(f"Successful:      {len(successes)}")
    print(f"Failed:          {len(failures)}")
    print(f"Success Rate:    {len(successes)/num_requests*100:.1f}%")
    print(f"Throughput:      {num_requests/total_time:.1f} req/s")

    if latencies:
        latencies.sort()
        print(f"\nLatency (ms):")
        print(f"  Min:    {min(latencies):.1f}")
        print(f"  Max:    {max(latencies):.1f}")
        print(f"  Mean:   {statistics.mean(latencies):.1f}")
        print(f"  Median: {statistics.median(latencies):.1f}")
        print(f"  p50:    {latencies[int(len(latencies)*0.5)]:.1f}")
        print(f"  p95:    {latencies[int(len(latencies)*0.95)]:.1f}")
        print(f"  p99:    {latencies[int(len(latencies)*0.99)]:.1f}")

    if failures:
        print(f"\nFailure Details:")
        error_counts = {}
        for f in failures:
            key = f.get("error", f"HTTP {f['status']}")
            error_counts[key] = error_counts.get(key, 0) + 1
        for error, count in sorted(error_counts.items(), key=lambda x: -x[1]):
            print(f"  {error}: {count}")

    print(f"{'=' * 50}")

    # Return summary for verification
    return {
        "total_requests": num_requests,
        "successful": len(successes),
        "failed": len(failures),
        "success_rate": len(successes) / num_requests * 100,
        "throughput_rps": num_requests / total_time,
        "total_time_s": total_time,
        "p50_ms": latencies[int(len(latencies) * 0.5)] if latencies else 0,
        "p95_ms": latencies[int(len(latencies) * 0.95)] if latencies else 0,
        "p99_ms": latencies[int(len(latencies) * 0.99)] if latencies else 0,
    }


def verify_data(cluster_nodes, expected_files):
    """Verify that files are accessible across the cluster."""
    print(f"\nVerification: checking {len(expected_files)} files across cluster...")
    found = 0
    for fp in expected_files[:20]:  # Check first 20
        for node in cluster_nodes:
            try:
                url = f"http://{node['host']}:{node['port']}/read/{fp}"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    found += 1
                    break
            except requests.RequestException:
                pass
    print(f"  Verified: {found}/{min(len(expected_files), 20)} files readable")


def main():
    parser = argparse.ArgumentParser(description="Load test for storage cluster")
    parser.add_argument("--host", default="localhost", help="Target host")
    parser.add_argument("--port", type=int, default=5001, help="Target port")
    parser.add_argument("--requests", type=int, default=100, help="Number of requests")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent workers")
    parser.add_argument("--dashboard", action="store_true", help="Target dashboard /api/write instead of node /write")
    args = parser.parse_args()

    if args.dashboard:
        url = f"http://{args.host}:{args.port}/api/write"
    else:
        url = f"http://{args.host}:{args.port}/write"

    summary = run_load_test(url, args.requests, args.concurrency)

    # Exit with error if success rate < 95%
    if summary["success_rate"] < 95:
        print(f"\nFAILED: Success rate {summary['success_rate']:.1f}% is below 95% threshold")
        sys.exit(1)
    else:
        print(f"\nPASSED: Success rate {summary['success_rate']:.1f}%")
        sys.exit(0)


if __name__ == "__main__":
    main()
