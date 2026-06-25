#!/usr/bin/env python3
"""Throughput / load gate (Commit 13).

A **bounded** black-box load test that runs *inside Docker* against the live ``app``
service over HTTP (:mod:`requests` only; it never imports the API). It imports
:func:`src.log_generator.generate_logs` purely to fabricate deterministic request
bodies (``PYTHONPATH=/app`` in the tester image).

It drives a sustained, concurrent load of ``POST /cluster/batch`` calls with large
batches from a small thread pool, measures the wall-clock throughput, and enforces the
spec's criterion (``project_requirements.md`` §5):

    throughput **>= 1000 logs/second**

How it works
------------
* Pre-builds ``NUM_BATCHES`` payloads of ``BATCH_SIZE`` logs each (default 20 × 500 =
  10,000 logs total — bounded so the run finishes in a few seconds and never hammers
  the host).
* A thread pool of ``WORKERS`` (default 4) workers each ``POST /cluster/batch`` (sessions
  are not thread-safe, so each worker uses its own :class:`requests.Session`).
* Throughput is ``total_logs_from_successful_batches / wall_clock`` measured around the
  whole concurrent phase (first send to last response).

Exit status
-----------
* Exits **0** if throughput ``>= MIN_LOGS_PER_SEC`` (default 1000) and at least one batch
  succeeded.
* Exits **1** if throughput is below the gate (prints ``FAIL``).
* Exits **2** on a health-check timeout or warm-up failure.

Configuration (environment)
---------------------------
* ``APP_URL`` — base URL of the live app (default ``http://app:8000``).
* ``LOAD_WORKERS`` — thread-pool size (default 4, clamped).
* ``LOAD_BATCH_SIZE`` — logs per ``/cluster/batch`` request (default 500, clamped).
* ``LOAD_NUM_BATCHES`` — total number of batch requests (default 20, clamped).
* ``MIN_LOGS_PER_SEC`` — hard throughput gate (default 1000).
* ``HEALTH_TIMEOUT_SEC`` — bounded wait for the app to become healthy (default 90s).
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

from src.log_generator import generate_logs

# --- configuration (all overridable via env, all bounded) --------------------

APP_URL: str = os.environ.get("APP_URL", "http://app:8000").rstrip("/")
WORKERS: int = max(1, min(int(os.environ.get("LOAD_WORKERS", "4")), 16))
BATCH_SIZE: int = max(100, min(int(os.environ.get("LOAD_BATCH_SIZE", "500")), 2000))
NUM_BATCHES: int = max(2, min(int(os.environ.get("LOAD_NUM_BATCHES", "20")), 100))
MIN_LOGS_PER_SEC: float = float(os.environ.get("MIN_LOGS_PER_SEC", "1000"))
HEALTH_TIMEOUT_SEC: float = float(os.environ.get("HEALTH_TIMEOUT_SEC", "90"))
WARMUP_REQUESTS: int = 5


def _wait_for_health(session: requests.Session) -> None:
    """Block until ``GET /health`` reports ``status == "ok"``, or exit non-zero."""
    deadline = time.time() + HEALTH_TIMEOUT_SEC
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            resp = session.get(f"{APP_URL}/health", timeout=5)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                print(f"[load] app healthy at {APP_URL}: {resp.json()}")
                return
            last_err = (
                f"HTTP {resp.status_code}"
                if resp.status_code != 200
                else f"status={resp.json().get('status')}"
            )
        except requests.RequestException as exc:
            last_err = repr(exc)
        time.sleep(2)
    print(f"[load] FATAL: app at {APP_URL} never became ready ({last_err})")
    sys.exit(2)


def _warm_up(session: requests.Session, sample: dict) -> bool:
    """Issue a few throwaway requests to prime the threadpool. False on failure."""
    print(f"[load] warming up with {WARMUP_REQUESTS} requests ...")
    for _ in range(WARMUP_REQUESTS):
        try:
            session.post(f"{APP_URL}/cluster", json=sample, timeout=10)
        except requests.RequestException as exc:
            print(f"[load] FATAL: warmup request failed: {exc!r}")
            return False
    return True


def _post_batch(batch: list[dict]) -> int:
    """POST one ``/cluster/batch`` and return the count of logs successfully processed.

    Each call builds its own :class:`requests.Session` (sessions are not thread-safe).
    Returns ``0`` on any error or non-200 so a partial failure simply doesn't count
    toward throughput rather than crashing the whole run.
    """
    sess = requests.Session()
    try:
        resp = sess.post(f"{APP_URL}/cluster/batch", json={"logs": batch}, timeout=120)
        if resp.status_code != 200:
            print(f"[load] batch returned HTTP {resp.status_code}: {resp.text[:160]}")
            return 0
        body = resp.json()
        return len(body) if isinstance(body, list) else 0
    except requests.RequestException as exc:
        print(f"[load] batch request failed: {exc!r}")
        return 0
    finally:
        sess.close()


def main() -> int:
    """Run the concurrent throughput phase and return an exit code (0 = pass)."""
    session = requests.Session()
    _wait_for_health(session)

    # Pre-build all batch payloads up front (deterministic; never touches the app), so
    # generation time is excluded from the measured throughput window.
    print(f"[load] building {NUM_BATCHES} batches of {BATCH_SIZE} logs ...")
    batches = [
        [log.model_dump(mode="json") for log in generate_logs(BATCH_SIZE, seed=100 + b)]
        for b in range(NUM_BATCHES)
    ]
    total_logs = NUM_BATCHES * BATCH_SIZE

    if not _warm_up(session, batches[0][0]):
        return 2

    print(
        f"[load] concurrent phase: {WORKERS} workers POSTing {NUM_BATCHES} batches "
        f"({total_logs} logs total) ..."
    )
    processed = 0
    failed_batches = 0
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(_post_batch, batch) for batch in batches]
        for fut in as_completed(futures):
            n = fut.result()
            if n > 0:
                processed += n
            else:
                failed_batches += 1
    elapsed = max(time.perf_counter() - start, 1e-9)

    throughput = processed / elapsed

    print("\n=== throughput summary ===")
    print(f"  workers      : {WORKERS}")
    print(f"  batches      : {NUM_BATCHES} x {BATCH_SIZE} logs ({total_logs} total)")
    print(f"  processed    : {processed} logs ({failed_batches} failed batch(es))")
    print(f"  elapsed      : {elapsed:.3f} s")
    print(f"  throughput   : {throughput:.0f} logs/s")
    print(f"  gate         : >= {MIN_LOGS_PER_SEC:.0f} logs/s")
    print(f"throughput={throughput:.0f} logs/s")

    if processed == 0:
        print("\n[load] FAIL: no batches succeeded")
        return 1
    if throughput < MIN_LOGS_PER_SEC:
        print(
            f"\n[load] FAIL: {throughput:.0f} logs/s < required {MIN_LOGS_PER_SEC:.0f} logs/s"
        )
        return 1

    print(
        f"\n[load] PASS: {throughput:.0f} logs/s >= required {MIN_LOGS_PER_SEC:.0f} logs/s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
