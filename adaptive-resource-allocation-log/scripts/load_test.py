#!/usr/bin/env python3
"""Black-box concurrent load test for the Adaptive Resource Allocation System.

Runs INSIDE Docker (the ``loadtest`` compose service) against the live ``app``
service. It is a pure external client — it never imports ``src.*`` — and answers a
different question than ``verify_e2e.py``: not "is each behaviour correct?" but
"does the server stay fast and reliable under concurrent traffic?".

Design:
  * Wait for ``/health`` to report healthy.
  * Inject one load ramp (40000 msgs/s) so the autoscaler is exercised during the
    run; whether the pool grew is recorded as a SOFT check (informational — it does
    not by itself fail the run, since the hard gates are about server behaviour).
  * Spawn ``LOAD_CONCURRENCY`` worker threads that, for ``LOAD_DURATION`` seconds,
    repeatedly hit a mix of ``GET /health`` and ``GET /api/status``, recording each
    request's success flag and latency.
  * Evaluate three hard gates and exit non-zero if any fails, so the Makefile
    ``load`` target propagates the failure:
        - error_rate <= 2%
        - throughput >= 50 req/s
        - p95 latency <= 500 ms

Tunables (env): ``APP_URL`` (default http://app:8080), ``LOAD_DURATION`` (8s),
``LOAD_CONCURRENCY`` (16). Kept deliberately modest so the test is repeatable and
not self-throttling on a developer laptop.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

APP_URL = os.environ.get("APP_URL", "http://app:8080").rstrip("/")


def _env_float(name: str, default: float) -> float:
    """Read a float env var, falling back to ``default`` on absence/garbage."""
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    """Read an int env var, falling back to ``default`` on absence/garbage."""
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


LOAD_DURATION = _env_float("LOAD_DURATION", 8.0)
LOAD_CONCURRENCY = _env_int("LOAD_CONCURRENCY", 16)

# Per-request timeout. Comfortably above the p95 gate so a slow-but-served request
# still counts as a (high-latency) success rather than a timeout error.
REQUEST_TIMEOUT = 5.0

# Hard gates.
MAX_ERROR_RATE = 0.02     # 2%
MIN_THROUGHPUT = 50.0     # requests/second
MAX_P95_MS = 500.0        # milliseconds

# Endpoints hammered during the run, alternated per request by each worker.
_ENDPOINTS = ("/health", "/api/status")


# ---------------------------------------------------------------------------
# Health gate
# ---------------------------------------------------------------------------

def _wait_healthy(timeout: int = 40) -> None:
    """Poll ``GET /health`` up to ~``timeout``×1s until ``status == "healthy"``.

    Raises :class:`SystemExit` (via the caller) only indirectly — here we just
    raise ``RuntimeError`` on timeout so ``main`` can report and exit 1.
    """
    deadline = time.time() + timeout
    last_err = "no response"
    while time.time() < deadline:
        try:
            r = requests.get(f"{APP_URL}/health", timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return
            last_err = f"status={r.status_code}"
        except requests.RequestException as exc:
            last_err = str(exc)
        except ValueError as exc:
            last_err = f"non-JSON health body: {exc}"
        time.sleep(1)
    raise RuntimeError(f"app never became healthy within {timeout}s (last: {last_err})")


# ---------------------------------------------------------------------------
# Load injection (soft check)
# ---------------------------------------------------------------------------

def _inject_load() -> int | None:
    """Read the worker count, POST a 40000 msgs/s ramp, return workers_before.

    40000 msgs/s against the ~800 msgs/s base capacity drives effective utilization
    to ~5000% — far past the scale-up threshold — so the autoscaler should grow the
    pool during the run. Returns ``workers_before`` (to compare afterwards), or
    ``None`` if the snapshot/POST could not be read (the soft check then degrades to
    "unknown" without failing the gates).
    """
    workers_before: int | None = None
    try:
        r = requests.get(f"{APP_URL}/api/status", timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            workers_before = int((r.json().get("workers") or {}).get("current"))
    except (requests.RequestException, ValueError, TypeError):
        workers_before = None

    try:
        requests.post(
            f"{APP_URL}/api/load",
            json={"arrival_rate": 40000, "ramp_seconds": 5},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        pass  # soft: a failed injection just leaves the autoscaler check inconclusive

    return workers_before


def _workers_now() -> int | None:
    """Best-effort read of the current worker count (``None`` on any error)."""
    try:
        r = requests.get(f"{APP_URL}/api/status", timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return int((r.json().get("workers") or {}).get("current"))
    except (requests.RequestException, ValueError, TypeError):
        return None
    return None


# ---------------------------------------------------------------------------
# Concurrent load phase
# ---------------------------------------------------------------------------

class _Results:
    """Thread-safe accumulator for per-request outcomes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.latencies_ms: list[float] = []
        self.ok = 0
        self.errors = 0

    def record(self, success: bool, latency_ms: float) -> None:
        with self._lock:
            self.latencies_ms.append(latency_ms)
            if success:
                self.ok += 1
            else:
                self.errors += 1

    @property
    def total(self) -> int:
        return self.ok + self.errors


def _worker(stop_at: float, results: _Results) -> None:
    """Hammer the endpoint mix until ``stop_at``, recording each outcome.

    Uses a per-thread :class:`requests.Session` for connection reuse (so we measure
    the server, not TCP/TLS setup) and alternates the two endpoints to spread load
    across a cheap liveness route and the heavier status snapshot.
    """
    session = requests.Session()
    i = 0
    while time.time() < stop_at:
        path = _ENDPOINTS[i % len(_ENDPOINTS)]
        i += 1
        start = time.perf_counter()
        try:
            r = session.get(f"{APP_URL}{path}", timeout=REQUEST_TIMEOUT)
            latency_ms = (time.perf_counter() - start) * 1000.0
            results.record(r.status_code == 200, latency_ms)
        except requests.RequestException:
            latency_ms = (time.perf_counter() - start) * 1000.0
            results.record(False, latency_ms)
    session.close()


def _percentile(values: list[float], pct: float) -> float:
    """Return the ``pct`` percentile (0–100) of ``values`` via nearest-rank.

    Returns ``0.0`` for an empty list. Nearest-rank keeps this dependency-free
    (no numpy) and is more than precise enough for a latency gate.
    """
    if not values:
        return 0.0
    ordered = sorted(values)
    k = max(1, int(round(pct / 100.0 * len(ordered))))
    k = min(k, len(ordered))
    return ordered[k - 1]


def _run_load() -> _Results:
    """Run the concurrent load phase for ``LOAD_DURATION`` seconds."""
    results = _Results()
    stop_at = time.time() + LOAD_DURATION
    with ThreadPoolExecutor(max_workers=LOAD_CONCURRENCY) as pool:
        futures = [
            pool.submit(_worker, stop_at, results) for _ in range(LOAD_CONCURRENCY)
        ]
        for f in futures:
            f.result()  # propagate any unexpected worker exception
    return results


# ---------------------------------------------------------------------------
# Gates + reporting
# ---------------------------------------------------------------------------

def _gate(name: str, passed: bool, detail: str) -> bool:
    """Print a single gate line and return its pass flag."""
    tag = "PASS" if passed else "FAIL"
    print(f"{tag}: {name} — {detail}")
    return passed


def main() -> int:
    """Run the load test and return 0 if all hard gates pass else 1."""
    print("=" * 70)
    print("Adaptive Resource Allocation — load test (black box)")
    print(f"  APP_URL     : {APP_URL}")
    print(f"  duration    : {LOAD_DURATION:.0f}s")
    print(f"  concurrency : {LOAD_CONCURRENCY}")
    print("=" * 70)

    try:
        _wait_healthy(timeout=40)
    except RuntimeError as exc:
        print(f"FATAL: {exc}")
        return 1

    # Kick the autoscaler before the load phase so any growth overlaps the run.
    workers_before = _inject_load()

    elapsed_start = time.perf_counter()
    results = _run_load()
    wall = time.perf_counter() - elapsed_start

    total = results.total
    if total == 0:
        print("FATAL: no requests were issued during the load phase")
        return 1

    error_rate = results.errors / total
    throughput = total / wall if wall > 0 else 0.0
    p95 = _percentile(results.latencies_ms, 95.0)
    avg = sum(results.latencies_ms) / len(results.latencies_ms)

    print()
    print(
        f"requests={total} ok={results.ok} errors={results.errors} "
        f"wall={wall:.2f}s avg={avg:.1f}ms p95={p95:.1f}ms"
    )

    # Soft check: did the autoscaler grow the pool under injected demand? Reported
    # but never fails the run — the hard gates below own pass/fail.
    workers_after = _workers_now()
    if workers_before is not None and workers_after is not None:
        grew = workers_after > workers_before
        note = "scaled up" if grew else "no growth observed"
        print(f"SOFT: autoscaler workers {workers_before}→{workers_after} ({note})")
    else:
        print("SOFT: autoscaler worker counts unavailable (skipped)")

    print()
    g1 = _gate(
        "error_rate <= 2%",
        error_rate <= MAX_ERROR_RATE,
        f"{error_rate * 100:.2f}%",
    )
    g2 = _gate(
        "throughput >= 50 req/s",
        throughput >= MIN_THROUGHPUT,
        f"{throughput:.1f} req/s",
    )
    g3 = _gate(
        "p95 latency <= 500ms",
        p95 <= MAX_P95_MS,
        f"{p95:.1f}ms",
    )

    passed = g1 and g2 and g3
    print("=" * 70)
    print(f"LOAD: {'PASSED' if passed else 'FAILED'} "
          f"({sum((g1, g2, g3))}/3 gates)")
    print("=" * 70)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
