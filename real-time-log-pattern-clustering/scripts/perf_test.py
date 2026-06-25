#!/usr/bin/env python3
"""Latency benchmark + gates (Commit 13).

A small, **bounded**, black-box performance probe that runs *inside Docker* against
the live ``app`` service. It only speaks HTTP via :mod:`requests` (it never imports the
API), but it *does* import :func:`src.log_generator.generate_logs` to fabricate varied,
realistic request bodies (``PYTHONPATH=/app`` in the tester image) — pure data
generation that never touches the running service.

It measures two complementary latency numbers and enforces both as hard gates
(``project_requirements.md`` §5):

1. **Single-request latency** — time ``N`` (default 200) sequential ``POST /cluster``
   calls with *varied* logs and compute p50 / p95. Gate: **p95 < 150 ms**. This is the
   full HTTP round-trip inside Docker (network + FastAPI threadpool + engine), so the
   threshold is wider than the engine's per-log budget.
2. **Engine amortized latency** — one ``POST /cluster/batch`` of 500 logs; the server
   time per log is ``round_trip_seconds / 500 * 1000`` ms. Gate: **< 10 ms/log**, which
   reflects the spec's *"< 10 ms per log"* engine criterion (amortizing the fixed
   per-request HTTP overhead across the whole batch).

The load is deliberately bounded (~700 logs total, one batch) so the probe finishes in
a few seconds and never hammers the host — it runs in a container against the app.

Exit status
-----------
* Exits **0** only if BOTH gates pass.
* Exits **1** if either gate fails (prints ``FAIL: <which gate>``).
* Exits **2** on any connection / HTTP failure (so a broken service fails loudly).

Configuration (environment)
---------------------------
* ``APP_URL`` — base URL of the live app (default ``http://app:8000``).
* ``PERF_REQUESTS`` — number of timed single-cluster requests (default 200, clamped).
* ``PERF_P95_MS`` — single-request p95 gate in ms (default 150).
* ``PERF_BATCH_SIZE`` — logs in the amortized-latency batch (default 500, clamped).
* ``PERF_PER_LOG_MS`` — amortized per-log gate in ms (default 10).
* ``HEALTH_TIMEOUT_SEC`` — bounded wait for the app to become healthy (default 90s).
"""

from __future__ import annotations

import os
import sys
import time
from statistics import mean
from typing import Optional

import requests

from src.log_generator import generate_logs

# --- configuration (all overridable via env, with bounded defaults) ----------

APP_URL: str = os.environ.get("APP_URL", "http://app:8000").rstrip("/")
#: Number of timed single-cluster requests. Bounded so the probe stays quick.
NUM_REQUESTS: int = max(50, min(int(os.environ.get("PERF_REQUESTS", "200")), 1000))
#: Single-request p95 latency gate (ms) — the full HTTP round trip inside Docker.
P95_GATE_MS: float = float(os.environ.get("PERF_P95_MS", "150"))
#: Logs in the single amortized-latency batch.
BATCH_SIZE: int = max(100, min(int(os.environ.get("PERF_BATCH_SIZE", "500")), 2000))
#: Amortized per-log latency gate (ms) — the spec's engine criterion.
PER_LOG_GATE_MS: float = float(os.environ.get("PERF_PER_LOG_MS", "10"))
#: How long to wait for the app to report healthy before giving up.
HEALTH_TIMEOUT_SEC: float = float(os.environ.get("HEALTH_TIMEOUT_SEC", "90"))
#: Throwaway requests issued before timing, to warm the threadpool / import paths.
WARMUP_REQUESTS: int = 10


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Return the ``pct`` percentile (0–100) of an already-sorted ascending list.

    Uses the nearest-rank method (simple, dependency-free, and stable for the few
    hundred samples collected here).
    """
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = max(1, min(len(sorted_vals), int(round(pct / 100.0 * len(sorted_vals)))))
    return sorted_vals[rank - 1]


def _wait_for_health(session: requests.Session) -> None:
    """Block until ``GET /health`` reports ``status == "ok"``, or exit non-zero."""
    deadline = time.time() + HEALTH_TIMEOUT_SEC
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            resp = session.get(f"{APP_URL}/health", timeout=5)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                print(f"[perf] app healthy at {APP_URL}: {resp.json()}")
                return
            last_err = (
                f"HTTP {resp.status_code}"
                if resp.status_code != 200
                else f"status={resp.json().get('status')}"
            )
        except requests.RequestException as exc:
            last_err = repr(exc)
        time.sleep(2)
    print(f"[perf] FATAL: app at {APP_URL} never became ready ({last_err})")
    sys.exit(2)


def _single_request_latency(session: requests.Session, payloads: list[dict]) -> list[float]:
    """Time one ``POST /cluster`` per payload; return per-request latencies in ms.

    Exits the process (non-zero) on the first connection error or non-200 response so a
    broken service fails the gate loudly rather than reporting a bogus latency.
    """
    cluster_url = f"{APP_URL}/cluster"
    latencies_ms: list[float] = []
    for i, payload in enumerate(payloads):
        start = time.perf_counter()
        try:
            resp = session.post(cluster_url, json=payload, timeout=15)
        except requests.RequestException as exc:
            print(f"[perf] FATAL: request {i} failed: {exc!r}")
            sys.exit(2)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if resp.status_code != 200:
            print(f"[perf] FATAL: request {i} returned HTTP {resp.status_code}: {resp.text[:200]}")
            sys.exit(2)
        latencies_ms.append(elapsed_ms)
    return latencies_ms


def _amortized_per_log_ms(session: requests.Session, logs: list[dict]) -> float:
    """POST one ``/cluster/batch`` of ``logs`` and return server ms per log.

    The amortized per-log time is the whole round-trip wall time divided by the batch
    size, so the fixed per-request HTTP overhead is spread across all 500 logs — a fair
    reflection of the engine's per-log cost. Exits non-zero on any failure.
    """
    print(f"[perf] amortized phase: POST /cluster/batch with {len(logs)} logs ...")
    start = time.perf_counter()
    try:
        resp = session.post(f"{APP_URL}/cluster/batch", json={"logs": logs}, timeout=120)
    except requests.RequestException as exc:
        print(f"[perf] FATAL: batch request failed: {exc!r}")
        sys.exit(2)
    elapsed = max(time.perf_counter() - start, 1e-9)
    if resp.status_code != 200:
        print(f"[perf] FATAL: batch returned HTTP {resp.status_code}: {resp.text[:200]}")
        sys.exit(2)
    body = resp.json()
    count = len(body) if isinstance(body, list) else 0
    if count != len(logs):
        print(f"[perf] FATAL: batch returned {count} assignments, expected {len(logs)}")
        sys.exit(2)
    per_log_ms = elapsed / len(logs) * 1000.0
    print(
        f"[perf] amortized phase done: {len(logs)} logs in {elapsed:.3f}s -> "
        f"{per_log_ms:.3f} ms/log"
    )
    return per_log_ms


def main() -> int:
    """Run both latency gates and return a process exit code (0 = both pass)."""
    session = requests.Session()
    _wait_for_health(session)

    # Build a pool of varied request bodies once (deterministic, never touches the app).
    varied = [log.model_dump(mode="json") for log in generate_logs(NUM_REQUESTS, seed=11)]
    batch = [log.model_dump(mode="json") for log in generate_logs(BATCH_SIZE, seed=23)]

    # --- warm up (not timed): prime the threadpool + import paths. ---
    print(f"[perf] warming up with {WARMUP_REQUESTS} requests ...")
    for i in range(WARMUP_REQUESTS):
        try:
            session.post(f"{APP_URL}/cluster", json=varied[i % len(varied)], timeout=10)
        except requests.RequestException as exc:
            print(f"[perf] FATAL: warmup request failed: {exc!r}")
            return 2

    # --- gate 1: single-request latency (p95). ---
    print(f"[perf] timing {NUM_REQUESTS} sequential POST /cluster requests ...")
    latencies_ms = _single_request_latency(session, varied)
    latencies_ms.sort()
    p50 = _percentile(latencies_ms, 50)
    p95 = _percentile(latencies_ms, 95)
    avg = mean(latencies_ms)

    print("\n=== POST /cluster single-request latency (ms) ===")
    print(f"  samples : {len(latencies_ms)}")
    print(f"  mean    : {avg:.2f}")
    print(f"  p50     : {p50:.2f}")
    print(f"  p95     : {p95:.2f}")
    print(f"  min/max : {latencies_ms[0]:.2f} / {latencies_ms[-1]:.2f}")
    print(f"  gate    : p95 < {P95_GATE_MS:.0f} ms")

    # --- gate 2: engine amortized per-log latency. ---
    per_log_ms = _amortized_per_log_ms(session, batch)
    print("\n=== engine amortized latency (ms/log) ===")
    print(f"  batch size : {BATCH_SIZE}")
    print(f"  per-log    : {per_log_ms:.3f} ms")
    print(f"  gate       : < {PER_LOG_GATE_MS:.0f} ms/log  (spec engine criterion)")

    # --- verdict: both gates must pass. ---
    failures: list[str] = []
    if p95 >= P95_GATE_MS:
        failures.append(f"single-request p95 {p95:.2f} ms >= gate {P95_GATE_MS:.0f} ms")
    if per_log_ms >= PER_LOG_GATE_MS:
        failures.append(
            f"amortized {per_log_ms:.3f} ms/log >= gate {PER_LOG_GATE_MS:.0f} ms/log"
        )

    print("\n=== perf verdict ===")
    if failures:
        for f in failures:
            print(f"[perf] FAIL: {f}")
        return 1

    print(
        f"[perf] PASS: p95 {p95:.2f} ms < {P95_GATE_MS:.0f} ms  and  "
        f"{per_log_ms:.3f} ms/log < {PER_LOG_GATE_MS:.0f} ms/log"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
