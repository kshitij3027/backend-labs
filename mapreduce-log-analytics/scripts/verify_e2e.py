"""End-to-end verification of the MapReduce Log Analytics API."""

import json
import os
import sys
import time

import httpx

SERVER_URL = os.environ.get("SERVER_URL", "http://app:8080")
DATA_DIR = os.environ.get("DATA_DIR", "/data")


def wait_for_server(timeout=60):
    """Wait for server to be healthy."""
    print(f"Waiting for server at {SERVER_URL}...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = httpx.get(f"{SERVER_URL}/health", timeout=5)
            if resp.status_code == 200:
                print("Server is healthy!")
                return True
        except Exception:
            pass
        time.sleep(2)
    print("ERROR: Server did not become healthy")
    return False


def submit_and_wait(client, analysis_type, input_files, timeout=120):
    """Submit a job and poll until completion."""
    resp = client.post(f"{SERVER_URL}/api/jobs/submit", json={
        "analysis_type": analysis_type,
        "input_files": input_files,
    })
    assert resp.status_code == 201, f"Submit failed: {resp.text}"
    job_id = resp.json()["job_id"]
    print(f"  Submitted {analysis_type} job: {job_id}")

    start = time.time()
    while time.time() - start < timeout:
        resp = client.get(f"{SERVER_URL}/api/jobs/{job_id}")
        assert resp.status_code == 200
        data = resp.json()
        status = data["status"]
        if status == "COMPLETED":
            print(f"  Completed in {data['execution_time']:.2f}s")
            return data
        elif status == "FAILED":
            print(f"  FAILED: {data.get('error_message')}")
            return data
        time.sleep(1)

    print(f"  TIMEOUT after {timeout}s")
    return None


def main():
    if not wait_for_server():
        sys.exit(1)

    client = httpx.Client(timeout=30)
    passed = 0
    failed = 0

    # Check functions endpoint
    print("\n--- Test: List functions ---")
    resp = client.get(f"{SERVER_URL}/api/functions")
    assert resp.status_code == 200
    funcs = resp.json()
    assert "word_count" in funcs
    assert "security" in funcs
    print(f"  Available: {list(funcs.keys())}")
    passed += 1

    # Find generated log files
    json_log = os.path.join(DATA_DIR, "sample-logs.jsonl")
    apache_log = os.path.join(DATA_DIR, "sample-apache.log")

    if not os.path.isfile(json_log):
        print(f"ERROR: {json_log} not found. Run generate-logs first.")
        sys.exit(1)

    # Test each analysis type
    for analysis_type in ["WORD_COUNT", "PATTERN_FREQUENCY", "SERVICE_DISTRIBUTION", "SECURITY"]:
        print(f"\n--- Test: {analysis_type} ---")
        result = submit_and_wait(client, analysis_type, [json_log])
        if result and result["status"] == "COMPLETED":
            assert result["results"] is not None
            if analysis_type == "SECURITY":
                assert "top_ips" in result["results"]
                assert "peak_hours" in result["results"]
            print(f"  PASS: {len(result.get('results', {}))} result keys")
            passed += 1
        else:
            print(f"  FAIL")
            failed += 1

    # Test with Apache logs
    if os.path.isfile(apache_log):
        print(f"\n--- Test: WORD_COUNT on Apache logs ---")
        result = submit_and_wait(client, "WORD_COUNT", [apache_log])
        if result and result["status"] == "COMPLETED":
            print(f"  PASS: {len(result.get('results', {}))} result keys")
            passed += 1
        else:
            print(f"  FAIL")
            failed += 1

    # Test concurrent jobs
    print(f"\n--- Test: 3 concurrent jobs ---")
    job_ids = []
    for atype in ["WORD_COUNT", "PATTERN_FREQUENCY", "SERVICE_DISTRIBUTION"]:
        resp = client.post(f"{SERVER_URL}/api/jobs/submit", json={
            "analysis_type": atype,
            "input_files": [json_log],
        })
        assert resp.status_code == 201
        job_ids.append(resp.json()["job_id"])

    print(f"  Submitted {len(job_ids)} concurrent jobs")

    # Wait for all to complete
    all_done = False
    start = time.time()
    while time.time() - start < 120:
        statuses = []
        for jid in job_ids:
            resp = client.get(f"{SERVER_URL}/api/jobs/{jid}")
            statuses.append(resp.json()["status"])
        if all(s in ("COMPLETED", "FAILED") for s in statuses):
            all_done = True
            break
        time.sleep(2)

    if all_done and all(s == "COMPLETED" for s in statuses):
        print(f"  PASS: All 3 concurrent jobs completed")
        passed += 1
    else:
        print(f"  FAIL: Statuses = {statuses}")
        failed += 1

    # Summary
    print(f"\n{'='*40}")
    print(f"E2E Results: {passed} passed, {failed} failed")
    print(f"{'='*40}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
