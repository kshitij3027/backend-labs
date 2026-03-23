#!/usr/bin/env python3
"""End-to-end verification of the MapReduce Log Processor.

Runs against the coordinator API at localhost:8000, submits MapReduce jobs,
waits for completion, and validates results against independently computed
expected values.

This script can run either on the host or inside a Docker container.
It regenerates the same deterministic log data (seed=42) locally to compute
expected results, so it does not need direct access to the Docker volume.
"""

import json
import os
import sys
import tempfile
import time
from collections import Counter, defaultdict

import httpx

COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
LOG_FILE_CONTAINER = "/data/sample-logs.jsonl"  # Path inside the container


# ---------------------------------------------------------------------------
# Log generation (mirrors scripts/generate_logs.py with same seed)
# ---------------------------------------------------------------------------

def regenerate_logs_locally() -> str:
    """Regenerate the same deterministic logs locally and return the file path."""
    # Import the generation logic so we stay in sync with the actual generator
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from scripts.generate_logs import generate_logs, NUM_LINES

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    tmp.close()
    generate_logs(tmp.name, NUM_LINES)
    return tmp.name


# ---------------------------------------------------------------------------
# Expected-value computation
# ---------------------------------------------------------------------------

def compute_expected_word_counts(log_file: str) -> dict[str, int]:
    """Independently compute expected word counts using the same logic as
    word_count map + sum reduce."""
    counts: Counter = Counter()
    with open(log_file) as f:
        for line in f:
            entry = json.loads(line.strip())
            message = entry.get("message", "")
            for word in message.lower().split():
                word = word.strip(".,!?;:\"'()[]{}").lower()
                if word:
                    counts[word] += 1
    return dict(counts)


def compute_expected_error_counts(log_file: str) -> dict[str, int]:
    """Independently compute expected error code counts using the same logic
    as error_code map + sum reduce."""
    counts: Counter = Counter()
    with open(log_file) as f:
        for line in f:
            entry = json.loads(line.strip())
            code = entry.get("error_code")
            if code:
                counts[str(code)] += 1
    return dict(counts)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def wait_for_healthy(timeout: int = 90) -> None:
    """Wait for the coordinator to be healthy."""
    print(f"Waiting for coordinator at {COORDINATOR_URL} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{COORDINATOR_URL}/health", timeout=5)
            if resp.status_code == 200:
                print("  Coordinator is healthy.")
                return
        except httpx.ConnectError:
            pass
        except httpx.ReadTimeout:
            pass
        time.sleep(2)
    print("ERROR: Coordinator did not become healthy within timeout.")
    sys.exit(1)


def submit_job(
    map_fn: str,
    reduce_fn: str,
    num_mappers: int = 2,
    num_reducers: int = 2,
) -> str:
    """Submit a MapReduce job and return the job_id."""
    resp = httpx.post(
        f"{COORDINATOR_URL}/jobs",
        json={
            "input_path": LOG_FILE_CONTAINER,
            "map_fn": map_fn,
            "reduce_fn": reduce_fn,
            "num_mappers": num_mappers,
            "num_reducers": num_reducers,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    job_id = data["id"]
    print(f"  Submitted job {job_id} (map={map_fn}, reduce={reduce_fn})")
    return job_id


def wait_for_completion(job_id: str, timeout: int = 180) -> dict:
    """Poll until the job completes or times out."""
    deadline = time.time() + timeout
    terminal = {"COMPLETED", "FAILED", "CANCELLED"}
    while time.time() < deadline:
        resp = httpx.get(f"{COORDINATOR_URL}/jobs/{job_id}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        status = data["status"]
        if status in terminal:
            print(f"  Job {job_id} finished with status: {status}")
            return data
        time.sleep(3)
    print(f"ERROR: Job {job_id} did not complete within {timeout}s.")
    sys.exit(1)


def get_results(job_id: str) -> dict:
    """Get job results."""
    resp = httpx.get(f"{COORDINATOR_URL}/jobs/{job_id}/result", timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_word_count_pipeline(local_log_file: str) -> None:
    """Test 1: word_count/sum pipeline with 10K+ events."""
    print("\n=== Test 1: Word Count Pipeline ===")

    job_id = submit_job("word_count", "sum", num_mappers=4, num_reducers=2)
    job_data = wait_for_completion(job_id)
    assert job_data["status"] == "COMPLETED", f"Job failed: {job_data['status']}"

    result = get_results(job_id)
    actual_dict = {r["key"]: int(r["value"]) for r in result["results"]}
    assert len(actual_dict) > 0, "No results returned"

    expected = compute_expected_word_counts(local_log_file)
    assert len(expected) > 0, "Expected word counts are empty (bad test data)"

    # Compare all keys
    mismatches = []
    for key in expected:
        actual_val = actual_dict.get(key)
        if actual_val is None:
            mismatches.append(f"  MISSING key '{key}': expected={expected[key]}")
        elif actual_val != expected[key]:
            mismatches.append(
                f"  MISMATCH key '{key}': expected={expected[key]}, actual={actual_val}"
            )

    extra_keys = set(actual_dict) - set(expected)
    for key in extra_keys:
        mismatches.append(f"  EXTRA key '{key}': actual={actual_dict[key]}")

    if mismatches:
        print("  FAIL: mismatches found:")
        for m in mismatches[:20]:
            print(m)
        if len(mismatches) > 20:
            print(f"  ... and {len(mismatches) - 20} more")
        assert False, f"{len(mismatches)} key mismatches found"

    print(f"  PASS: {len(actual_dict)} unique words, all counts match expected values")


def test_error_code_pipeline(local_log_file: str) -> None:
    """Test 2: error_code/sum pipeline."""
    print("\n=== Test 2: Error Code Pipeline ===")

    job_id = submit_job("error_code", "sum", num_mappers=4, num_reducers=2)
    job_data = wait_for_completion(job_id)
    assert job_data["status"] == "COMPLETED", f"Job failed: {job_data['status']}"

    result = get_results(job_id)
    actual_dict = {r["key"]: int(r["value"]) for r in result["results"]}
    assert len(actual_dict) > 0, "No results returned"

    expected = compute_expected_error_counts(local_log_file)
    assert len(expected) > 0, "Expected error counts are empty (bad test data)"

    # Compare
    mismatches = []
    for key in expected:
        actual_val = actual_dict.get(key)
        if actual_val is None:
            mismatches.append(f"  MISSING key '{key}': expected={expected[key]}")
        elif actual_val != expected[key]:
            mismatches.append(
                f"  MISMATCH key '{key}': expected={expected[key]}, actual={actual_val}"
            )

    extra_keys = set(actual_dict) - set(expected)
    for key in extra_keys:
        mismatches.append(f"  EXTRA key '{key}': actual={actual_dict[key]}")

    if mismatches:
        print("  FAIL: mismatches found:")
        for m in mismatches:
            print(m)
        assert False, f"{len(mismatches)} key mismatches found"

    print(f"  PASS: {len(actual_dict)} error codes, all counts match expected values")
    for code, count in sorted(actual_dict.items()):
        print(f"    {code}: {count}")


def test_metrics() -> None:
    """Test 3: Metrics endpoint has valid data after jobs."""
    print("\n=== Test 3: Metrics Endpoint ===")

    resp = httpx.get(f"{COORDINATOR_URL}/metrics", timeout=10)
    resp.raise_for_status()
    m = resp.json()

    assert m["jobs_completed"] >= 2, (
        f"Expected at least 2 completed jobs, got {m['jobs_completed']}"
    )
    assert m["jobs_failed"] == 0, (
        f"Expected 0 failed jobs, got {m['jobs_failed']}"
    )
    assert m["avg_job_duration_seconds"] > 0, "Average job duration should be > 0"

    print(f"  PASS: {m['jobs_completed']} jobs completed, 0 failed")
    print(f"  Avg job duration: {m['avg_job_duration_seconds']:.2f}s")
    print(f"  Total shuffle volume: {m['total_shuffle_volume_bytes']} bytes")


def test_stats() -> None:
    """Test 4: Stats endpoint returns valid system state."""
    print("\n=== Test 4: Stats Endpoint ===")

    resp = httpx.get(f"{COORDINATOR_URL}/stats", timeout=10)
    resp.raise_for_status()
    s = resp.json()

    assert s["total_jobs"] >= 2, f"Expected >= 2 total jobs, got {s['total_jobs']}"
    assert s["workers_alive"] >= 1, f"Expected >= 1 alive worker, got {s['workers_alive']}"

    print(f"  PASS: {s['total_jobs']} total jobs, {s['workers_alive']} alive workers")


def main() -> int:
    print("=" * 60)
    print("MapReduce Log Processor - E2E Verification")
    print("=" * 60)

    wait_for_healthy()

    # Regenerate logs locally with same deterministic seed
    print("\nRegenerating logs locally for expected-value computation...")
    local_log_file = regenerate_logs_locally()
    print(f"  Local log file: {local_log_file}")

    try:
        test_word_count_pipeline(local_log_file)
        test_error_code_pipeline(local_log_file)
        test_metrics()
        test_stats()
    except AssertionError as e:
        print(f"\nFAILED: {e}")
        return 1
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Cleanup temp file
        try:
            os.unlink(local_log_file)
        except OSError:
            pass

    print("\n" + "=" * 60)
    print("ALL E2E TESTS PASSED")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
