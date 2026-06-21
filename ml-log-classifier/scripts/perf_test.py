#!/usr/bin/env python3
"""Single-``/classify`` latency benchmark (Commit 16).

A small, **bounded**, black-box performance probe that runs *inside Docker*
against the live ``app`` service (it only speaks HTTP via :mod:`requests`; it
never imports the app). It measures the per-request latency of the synchronous
``POST /classify`` endpoint and checks the spec's success criterion:

    inference latency **under 100 ms** per classification

(``project_requirements.md`` §5; the threshold mirrors ``cfg.target_latency_ms``).

What it does
------------
1. Wait for the app to become healthy (``GET /health``), then **warm up** the
   model + JIT/import paths with a handful of throwaway requests (also priming the
   prediction cache for the repeated patterns).
2. Time ``N`` (default 500) sequential ``POST /classify`` calls, deliberately
   **mixing repeated logs** (to exercise cache *hits*) with **unique logs** (cache
   *misses*) so the number is representative of real traffic rather than an
   all-hit or all-miss extreme.
3. Compute and print latency percentiles — p50 / p95 / p99 / mean (milliseconds).
4. Print the prediction-cache stats (``GET /cache/stats``) **before and after** the
   timed run so the cache warming is visible.

Exit status
-----------
* Exits **non-zero** if p95 latency ``>= TARGET_LATENCY_MS`` (default 100 ms) — this
  is the hard gate ``make load`` enforces.
* Exits non-zero on any connection/HTTP failure (so a broken service fails loudly).

Configuration (environment)
---------------------------
* ``APP_URL`` — base URL of the live app (default ``http://app:8000``).
* ``PERF_REQUESTS`` — number of timed requests (default 500, clamped to a sane cap).
* ``TARGET_LATENCY_MS`` — p95 gate in milliseconds (default 100).
"""

from __future__ import annotations

import os
import sys
import time
from statistics import mean
from typing import Any, Optional

import requests

# --- configuration (all overridable via env, with bounded defaults) ----------

APP_URL: str = os.environ.get("APP_URL", "http://app:8000").rstrip("/")
#: Number of timed single-classify requests. Bounded so the probe stays quick on
#: Docker-for-Mac; a few hundred samples is plenty for stable percentiles.
NUM_REQUESTS: int = max(50, min(int(os.environ.get("PERF_REQUESTS", "500")), 2000))
#: p95 latency gate (ms). Mirrors ``Settings.target_latency_ms``.
TARGET_LATENCY_MS: float = float(os.environ.get("TARGET_LATENCY_MS", "100"))
#: How long to wait for the app to report healthy before giving up.
HEALTH_TIMEOUT_SEC: float = float(os.environ.get("HEALTH_TIMEOUT_SEC", "120"))
#: Throwaway requests issued before timing, to warm the model and caches.
WARMUP_REQUESTS: int = 25

#: A small pool of representative raw logs. The first few are reused *often*
#: (cache hits); ``_unique_log`` below fabricates never-before-seen lines (misses).
SAMPLE_LOGS: tuple[str, ...] = (
    "Database connection failed with timeout error after 5000ms",
    "User authentication succeeded for session token",
    "GET /api/v1/orders returned 200 in 12ms",
    "WARN disk usage at 82% on /var/log partition",
    "Connection pool exhausted; rejecting new requests",
    "Cache miss for key user_profile; falling back to database",
    "TLS handshake failed with upstream peer",
    "Scheduled job 'nightly-rollup' completed successfully",
)


def _unique_log(i: int) -> str:
    """Return a log line guaranteed unique per ``i`` (forces a cache miss).

    The trailing high-cardinality id is stripped by ``preprocess`` *except* for the
    distinct ordinal word, so each line normalizes to a different pattern and never
    hits the cache — the half of the mix that measures cold-path latency.
    """
    return f"Request {i} processing widget batch shard number {i} failed unexpectedly"


def _payload_for(i: int) -> dict[str, str]:
    """Build the ``/classify`` body for iteration ``i`` (≈50% repeats, 50% unique)."""
    if i % 2 == 0:
        # Repeated traffic: cycle a few hot lines so the cache serves them.
        return {"raw_log": SAMPLE_LOGS[i % len(SAMPLE_LOGS)]}
    return {"raw_log": _unique_log(i)}


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Return the ``pct`` percentile (0–100) of an already-sorted list.

    Uses the nearest-rank method (simple, dependency-free, and stable for the few
    hundred samples we collect). ``sorted_vals`` must be non-empty and ascending.
    """
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    # nearest-rank: rank in [1, n]
    rank = max(1, min(len(sorted_vals), int(round(pct / 100.0 * len(sorted_vals)))))
    return sorted_vals[rank - 1]


def _wait_for_health(session: requests.Session) -> None:
    """Block until ``GET /health`` returns 200, or exit non-zero on timeout."""
    deadline = time.time() + HEALTH_TIMEOUT_SEC
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            resp = session.get(f"{APP_URL}/health", timeout=5)
            if resp.status_code == 200:
                print(f"[perf] app healthy at {APP_URL}: {resp.json()}")
                return
            last_err = f"status {resp.status_code}"
        except requests.RequestException as exc:
            last_err = repr(exc)
        time.sleep(2)
    print(f"[perf] FATAL: app at {APP_URL} never became healthy ({last_err})")
    sys.exit(2)


def _fetch_cache_stats(session: requests.Session) -> Optional[dict[str, Any]]:
    """Return ``GET /cache/stats`` as a dict, or ``None`` if unavailable."""
    try:
        resp = session.get(f"{APP_URL}/cache/stats", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException:
        pass
    return None


def main() -> int:
    """Run the latency benchmark and return a process exit code (0 = pass)."""
    session = requests.Session()
    _wait_for_health(session)

    classify_url = f"{APP_URL}/classify"

    # --- warm up (not timed): prime the model + cache for the hot lines. ---
    print(f"[perf] warming up with {WARMUP_REQUESTS} requests ...")
    for i in range(WARMUP_REQUESTS):
        try:
            session.post(
                classify_url, json={"raw_log": SAMPLE_LOGS[i % len(SAMPLE_LOGS)]},
                timeout=10,
            )
        except requests.RequestException as exc:
            print(f"[perf] FATAL: warmup request failed: {exc!r}")
            return 2

    before = _fetch_cache_stats(session)
    if before is not None:
        print(f"[perf] cache stats BEFORE timed run: {before}")

    # --- timed run: N sequential calls, mixing repeated + unique logs. ---
    print(f"[perf] timing {NUM_REQUESTS} sequential POST /classify requests ...")
    latencies_ms: list[float] = []
    for i in range(NUM_REQUESTS):
        payload = _payload_for(i)
        start = time.perf_counter()
        try:
            resp = session.post(classify_url, json=payload, timeout=10)
        except requests.RequestException as exc:
            print(f"[perf] FATAL: request {i} failed: {exc!r}")
            return 2
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if resp.status_code != 200:
            print(f"[perf] FATAL: request {i} returned HTTP {resp.status_code}: {resp.text[:200]}")
            return 2
        latencies_ms.append(elapsed_ms)

    after = _fetch_cache_stats(session)
    if after is not None:
        print(f"[perf] cache stats AFTER timed run:  {after}")

    # --- summarize. ---
    latencies_ms.sort()
    p50 = _percentile(latencies_ms, 50)
    p95 = _percentile(latencies_ms, 95)
    p99 = _percentile(latencies_ms, 99)
    avg = mean(latencies_ms)

    print("\n=== /classify latency (ms) ===")
    print(f"  samples : {len(latencies_ms)}")
    print(f"  mean    : {avg:.2f}")
    print(f"  p50     : {p50:.2f}")
    print(f"  p95     : {p95:.2f}")
    print(f"  p99     : {p99:.2f}")
    print(f"  min/max : {latencies_ms[0]:.2f} / {latencies_ms[-1]:.2f}")
    print(f"  target  : p95 < {TARGET_LATENCY_MS:.0f} ms")

    if p95 >= TARGET_LATENCY_MS:
        print(f"\n[perf] FAIL: p95 {p95:.2f} ms >= target {TARGET_LATENCY_MS:.0f} ms")
        return 1

    print(f"\n[perf] PASS: p95 {p95:.2f} ms < target {TARGET_LATENCY_MS:.0f} ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
