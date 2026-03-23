"""
Pytest-based E2E tests for the MapReduce Log Processor.

These tests run against a live coordinator API (inside Docker).
They submit real MapReduce jobs and validate results against independently
computed expected values.

Usage:
    docker compose run --rm e2e python -m pytest tests/test_e2e.py -v
"""

import json
import os
import sys
import tempfile
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
LOG_FILE_CONTAINER = "/data/sample-logs.jsonl"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def local_log_file():
    """Regenerate deterministic logs locally for expected-value computation."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from scripts.generate_logs import generate_logs, NUM_LINES

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    tmp.close()
    generate_logs(tmp.name, NUM_LINES)
    yield tmp.name
    os.unlink(tmp.name)


@pytest.fixture(scope="session", autouse=True)
def wait_for_coordinator():
    """Wait for the coordinator to be healthy before running tests."""
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{COORDINATOR_URL}/health", timeout=5)
            if resp.status_code == 200:
                return
        except (httpx.ConnectError, httpx.ReadTimeout):
            pass
        time.sleep(2)
    pytest.fail("Coordinator did not become healthy within timeout")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def submit_job(map_fn: str, reduce_fn: str, num_mappers: int = 4, num_reducers: int = 2) -> str:
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
    return resp.json()["id"]


def wait_for_completion(job_id: str, timeout: int = 180) -> dict:
    deadline = time.time() + timeout
    terminal = {"COMPLETED", "FAILED", "CANCELLED"}
    while time.time() < deadline:
        resp = httpx.get(f"{COORDINATOR_URL}/jobs/{job_id}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data["status"] in terminal:
            return data
        time.sleep(3)
    pytest.fail(f"Job {job_id} did not complete within {timeout}s")


def get_results(job_id: str) -> dict:
    resp = httpx.get(f"{COORDINATOR_URL}/jobs/{job_id}/result", timeout=30)
    resp.raise_for_status()
    return resp.json()


def compute_expected_word_counts(log_file: str) -> dict[str, int]:
    from collections import Counter
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
    from collections import Counter
    counts: Counter = Counter()
    with open(log_file) as f:
        for line in f:
            entry = json.loads(line.strip())
            code = entry.get("error_code")
            if code:
                counts[str(code)] += 1
    return dict(counts)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWordCountPipeline:
    def test_word_count_returns_results(self, local_log_file):
        job_id = submit_job("word_count", "sum")
        data = wait_for_completion(job_id)
        assert data["status"] == "COMPLETED"

        result = get_results(job_id)
        actual = {r["key"]: int(r["value"]) for r in result["results"]}
        assert len(actual) > 0

    def test_word_count_matches_expected(self, local_log_file):
        job_id = submit_job("word_count", "sum")
        data = wait_for_completion(job_id)
        assert data["status"] == "COMPLETED"

        result = get_results(job_id)
        actual = {r["key"]: int(r["value"]) for r in result["results"]}
        expected = compute_expected_word_counts(local_log_file)

        assert set(actual.keys()) == set(expected.keys()), (
            f"Key sets differ: extra={set(actual) - set(expected)}, "
            f"missing={set(expected) - set(actual)}"
        )
        for key in expected:
            assert actual[key] == expected[key], (
                f"Mismatch for '{key}': expected={expected[key]}, actual={actual[key]}"
            )


class TestErrorCodePipeline:
    def test_error_code_returns_results(self, local_log_file):
        job_id = submit_job("error_code", "sum")
        data = wait_for_completion(job_id)
        assert data["status"] == "COMPLETED"

        result = get_results(job_id)
        actual = {r["key"]: int(r["value"]) for r in result["results"]}
        assert len(actual) > 0

    def test_error_code_matches_expected(self, local_log_file):
        job_id = submit_job("error_code", "sum")
        data = wait_for_completion(job_id)
        assert data["status"] == "COMPLETED"

        result = get_results(job_id)
        actual = {r["key"]: int(r["value"]) for r in result["results"]}
        expected = compute_expected_error_counts(local_log_file)

        assert set(actual.keys()) == set(expected.keys())
        for key in expected:
            assert actual[key] == expected[key], (
                f"Mismatch for '{key}': expected={expected[key]}, actual={actual[key]}"
            )


class TestMetrics:
    def test_metrics_after_jobs(self):
        resp = httpx.get(f"{COORDINATOR_URL}/metrics", timeout=10)
        resp.raise_for_status()
        m = resp.json()
        assert m["jobs_completed"] >= 1
        assert m["jobs_failed"] == 0


class TestStats:
    def test_stats_endpoint(self):
        resp = httpx.get(f"{COORDINATOR_URL}/stats", timeout=10)
        resp.raise_for_status()
        s = resp.json()
        assert s["total_jobs"] >= 1
        assert s["workers_alive"] >= 1
