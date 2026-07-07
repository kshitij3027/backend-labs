"""Performance + concurrent load gates for the Correlation Analysis System (C8).

Runs **inside Docker** (the profile-gated ``loadtest`` compose service) against
the LIVE backend over HTTP only — no ``src`` imports, no Redis access. Where
``scripts/verify_e2e.py`` answers "is the pipeline CORRECT end to end?", this
script answers "does it stay FAST while the pipeline keeps ingesting?" in four
hard-gated phases (fail-fast: the first breached gate exits non-zero so
``make load`` propagates it):

* **Phase A — ingest throughput**: ``/health`` ``events_processed`` delta over
  15 s must sustain >= ``MIN_EVENTS_PER_SEC`` while everything else runs.
* **Phase B — stats latency**: ``STATS_SEQUENTIAL`` sequential
  ``GET /api/v1/correlations/stats`` calls; avg < ``STATS_AVG_MS_MAX`` ms AND
  max < ``STATS_MAX_MS`` ms (the endpoint serves from in-memory accumulators,
  so double-digit milliseconds means something regressed).
* **Phase C — concurrency**: ``LOAD_REQUESTS`` GETs through an
  ``asyncio.Semaphore(LOAD_CONCURRENCY)`` over a deterministic 40/30/20/10 mix
  of /correlations?limit=50, /correlations/stats, /logs/recent?count=50 and
  /dashboard (the URL list is built up front — no randomness, runs are
  comparable); RPS = completed/wall >= ``LOAD_MIN_RPS`` and error rate
  (non-200 or exception) <= ``LOAD_MAX_ERROR_RATE``.
* **Phase D — memory**: server-reported ``/health`` ``memory_mb`` (backend
  RSS, not this client) < ``MAX_BACKEND_MEM_MB``.

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``BASE_URL``             backend base URL (default ``http://backend:8000``)
* ``LOAD_READY_TIMEOUT``   seconds to wait for /health (default 90)
* ``MIN_EVENTS_PER_SEC``   Phase A ingest gate (default 100)
* ``STATS_SEQUENTIAL``     Phase B sample size (default 50)
* ``STATS_AVG_MS_MAX``     Phase B average-latency gate, ms (default 50)
* ``STATS_MAX_MS``         Phase B max-latency gate, ms (default 100)
* ``LOAD_REQUESTS``        Phase C total requests (default 500)
* ``LOAD_CONCURRENCY``     Phase C max in-flight (default 100)
* ``LOAD_MIN_RPS``         Phase C throughput gate (default 50)
* ``LOAD_MAX_ERROR_RATE``  Phase C error-rate gate (default 0.0 — none may fail)
* ``MAX_BACKEND_MEM_MB``   Phase D memory gate, MB (default 200)

Output ends with machine-readable ``RESULT key=value`` lines (for the README's
measured-numbers table) and ``LOAD PASSED``, or ``FAIL: <reason>`` on stderr
with a non-zero exit.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
import time
from typing import Any

import httpx

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("BASE_URL", "http://backend:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "90"))
MIN_EVENTS_PER_SEC = float(os.environ.get("MIN_EVENTS_PER_SEC", "100"))
STATS_SEQUENTIAL = int(os.environ.get("STATS_SEQUENTIAL", "50"))
STATS_AVG_MS_MAX = float(os.environ.get("STATS_AVG_MS_MAX", "50"))
STATS_MAX_MS = float(os.environ.get("STATS_MAX_MS", "100"))
REQUESTS = int(os.environ.get("LOAD_REQUESTS", "500"))
CONCURRENCY = max(1, int(os.environ.get("LOAD_CONCURRENCY", "100")))
MIN_RPS = float(os.environ.get("LOAD_MIN_RPS", "50"))
MAX_ERROR_RATE = float(os.environ.get("LOAD_MAX_ERROR_RATE", "0.0"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "200"))

#: Phase A sampling window (seconds) and Phase-B/C per-request client timeout.
INGEST_SAMPLE_SECONDS = 15.0
REQUEST_TIMEOUT = 30.0
#: Discarded /health GETs before timing anything (connection pools, first-hit
#: lazy costs) so the measured phases see a warm service.
WARMUP_REQUESTS = 5

#: Phase C's deterministic request mix: a repeating 10-slot block interleaving
#: 4x correlations (40%), 3x stats (30%), 2x recent logs (20%) and 1x
#: dashboard (10%). ``urls[i] = _MIX_BLOCK[i % 10]`` — no randomness, so every
#: run drives an identical, comparable workload.
_URL_CORRELATIONS = "/api/v1/correlations?limit=50"
_URL_STATS = "/api/v1/correlations/stats"
_URL_LOGS = "/api/v1/logs/recent?count=50"
_URL_DASHBOARD = "/api/v1/dashboard"
_MIX_BLOCK = (
    _URL_CORRELATIONS,
    _URL_STATS,
    _URL_LOGS,
    _URL_CORRELATIONS,
    _URL_STATS,
    _URL_DASHBOARD,
    _URL_CORRELATIONS,
    _URL_STATS,
    _URL_LOGS,
    _URL_CORRELATIONS,
)


class CheckError(AssertionError):
    """Raised to fail a load gate with a clear, single-line message."""


def check(cond: bool, msg: str) -> None:
    """Assert ``cond``; raise :class:`CheckError` with ``msg`` when it is falsy."""
    if not cond:
        raise CheckError(msg)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live)."""
    print(f"[load] {msg}", flush=True)


def result(key: str, value: Any) -> None:
    """Print one machine-readable summary line (scraped for the README)."""
    print(f"RESULT {key}={value}", flush=True)


def percentile(values: list[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)]


# --------------------------------------------------------------------------- #
# Setup: readiness + warm-up
# --------------------------------------------------------------------------- #
def wait_ready(client: httpx.Client, timeout: float = READY_TIMEOUT) -> None:
    """Poll GET /health until it answers 200, within the timeout."""
    info(f"waiting for {BASE_URL}/health (up to {timeout:.0f}s)...")
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            resp = client.get("/health", timeout=5.0)
            if resp.status_code == 200:
                info("backend is ready")
                return
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 — the service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    raise CheckError(f"/health not ready after {timeout:.0f}s (last: {last})")


def warm_up(client: httpx.Client) -> None:
    """Fire WARMUP_REQUESTS discarded /health GETs before timing anything."""
    for i in range(WARMUP_REQUESTS):
        resp = client.get("/health", timeout=REQUEST_TIMEOUT)
        check(resp.status_code == 200, f"warm-up GET /health #{i + 1} -> HTTP {resp.status_code}")
    info(f"warm-up done ({WARMUP_REQUESTS} discarded /health GETs)")


# --------------------------------------------------------------------------- #
# Phase A — ingest throughput (server-side counter delta)
# --------------------------------------------------------------------------- #
def _events_processed(client: httpx.Client) -> int:
    resp = client.get("/health", timeout=REQUEST_TIMEOUT)
    check(resp.status_code == 200, f"GET /health -> HTTP {resp.status_code}")
    return int(resp.json()["components"]["events_processed"])


def phase_ingest(client: httpx.Client) -> float:
    """Gate: pipeline ingest sustained over the sampling window."""
    info(f"phase A: sampling events_processed over {INGEST_SAMPLE_SECONDS:.0f}s...")
    first = _events_processed(client)
    t0 = time.perf_counter()
    time.sleep(INGEST_SAMPLE_SECONDS)
    second = _events_processed(client)
    elapsed = time.perf_counter() - t0
    eps = (second - first) / elapsed
    info(f"phase A: {eps:.1f} events/s ({first} -> {second} in {elapsed:.1f}s)")
    result("ingest_eps", f"{eps:.1f}")
    check(
        eps >= MIN_EVENTS_PER_SEC,
        f"ingest {eps:.1f} events/s below gate {MIN_EVENTS_PER_SEC:.0f} "
        f"(events_processed {first} -> {second} over {elapsed:.1f}s)",
    )
    return eps


# --------------------------------------------------------------------------- #
# Phase B — sequential stats latency
# --------------------------------------------------------------------------- #
def phase_stats_latency(client: httpx.Client) -> dict[str, float]:
    """Gate: /api/v1/correlations/stats stays fast, one request at a time."""
    info(f"phase B: {STATS_SEQUENTIAL} sequential GET {_URL_STATS}...")
    samples_ms: list[float] = []
    for i in range(STATS_SEQUENTIAL):
        t0 = time.perf_counter()
        resp = client.get(_URL_STATS, timeout=REQUEST_TIMEOUT)
        samples_ms.append((time.perf_counter() - t0) * 1000.0)
        check(resp.status_code == 200, f"sequential stats GET #{i + 1} -> HTTP {resp.status_code}")
    avg = sum(samples_ms) / len(samples_ms)
    p95 = percentile(samples_ms, 95)
    worst = max(samples_ms)
    info(f"phase B: avg {avg:.1f}ms / p95 {p95:.1f}ms / max {worst:.1f}ms")
    result("stats_avg_ms", f"{avg:.1f}")
    result("stats_p95_ms", f"{p95:.1f}")
    result("stats_max_ms", f"{worst:.1f}")
    check(avg < STATS_AVG_MS_MAX, f"stats avg {avg:.1f}ms >= gate {STATS_AVG_MS_MAX:.0f}ms")
    check(worst < STATS_MAX_MS, f"stats max {worst:.1f}ms >= gate {STATS_MAX_MS:.0f}ms")
    return {"avg": avg, "p95": p95, "max": worst}


# --------------------------------------------------------------------------- #
# Phase C — concurrent mixed-endpoint load
# --------------------------------------------------------------------------- #
async def _concurrent_load() -> dict[str, float]:
    """Fire the deterministic URL mix through a bounded semaphore; measure it."""
    urls = [_MIX_BLOCK[i % len(_MIX_BLOCK)] for i in range(REQUESTS)]
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies_ms: list[float] = []
    errors = 0

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as client:

        async def one(url: str) -> None:
            nonlocal errors
            async with sem:
                t0 = time.perf_counter()
                try:
                    resp = await client.get(url)
                    latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                    if resp.status_code != 200:
                        errors += 1
                except Exception:  # noqa: BLE001 — any failure counts as an error
                    errors += 1

        wall0 = time.perf_counter()
        await asyncio.gather(*(one(url) for url in urls))
        wall = time.perf_counter() - wall0

    completed = len(latencies_ms)  # requests that produced an HTTP response
    return {
        "wall_s": wall,
        "completed": float(completed),
        "errors": float(errors),
        "rps": (completed / wall) if wall > 0 else 0.0,
        "error_rate": (errors / REQUESTS) if REQUESTS else 0.0,
        "p50": percentile(latencies_ms, 50),
        "p95": percentile(latencies_ms, 95),
        "max": max(latencies_ms) if latencies_ms else 0.0,
    }


def phase_concurrency() -> dict[str, float]:
    """Gates: concurrent RPS floor and error-rate ceiling over the mixed load."""
    info(
        f"phase C: {REQUESTS} GETs (40% correlations / 30% stats / 20% logs / "
        f"10% dashboard) at concurrency {CONCURRENCY}..."
    )
    stats = asyncio.run(_concurrent_load())
    info(
        f"phase C: {stats['completed']:.0f}/{REQUESTS} responses in {stats['wall_s']:.2f}s "
        f"-> {stats['rps']:.1f} req/s (errors={stats['errors']:.0f}); "
        f"latency p50 {stats['p50']:.1f}ms / p95 {stats['p95']:.1f}ms / max {stats['max']:.1f}ms"
    )
    result("load_requests", REQUESTS)
    result("load_concurrency", CONCURRENCY)
    result("load_rps", f"{stats['rps']:.1f}")
    result("load_error_rate", f"{stats['error_rate']:.4f}")
    result("load_p50_ms", f"{stats['p50']:.1f}")
    result("load_p95_ms", f"{stats['p95']:.1f}")
    result("load_max_ms", f"{stats['max']:.1f}")
    check(
        stats["rps"] >= MIN_RPS,
        f"throughput {stats['rps']:.1f} req/s below gate {MIN_RPS:.0f} "
        f"({stats['completed']:.0f}/{REQUESTS} in {stats['wall_s']:.2f}s)",
    )
    check(
        stats["error_rate"] <= MAX_ERROR_RATE,
        f"error_rate {stats['error_rate']:.4f} exceeds gate {MAX_ERROR_RATE} "
        f"({stats['errors']:.0f} errors over {REQUESTS} requests)",
    )
    return stats


# --------------------------------------------------------------------------- #
# Phase D — server-side memory
# --------------------------------------------------------------------------- #
def phase_memory(client: httpx.Client) -> float:
    """Gate: the BACKEND's own reported RSS (never this client's)."""
    resp = client.get("/health", timeout=REQUEST_TIMEOUT)
    check(resp.status_code == 200, f"GET /health -> HTTP {resp.status_code}")
    memory_mb = resp.json().get("memory_mb")
    check(
        isinstance(memory_mb, (int, float)) and not isinstance(memory_mb, bool),
        f"/health memory_mb is {memory_mb!r} (want a number)",
    )
    info(f"phase D: backend memory {memory_mb:.1f} MB (gate {MAX_BACKEND_MEM_MB:.0f} MB)")
    result("memory_mb", f"{memory_mb:.1f}")
    check(
        memory_mb < MAX_BACKEND_MEM_MB,
        f"backend memory {memory_mb:.1f} MB >= gate {MAX_BACKEND_MEM_MB:.0f} MB",
    )
    return float(memory_mb)


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== load test against {BASE_URL} ==")
    info(
        f"gates: ingest >= {MIN_EVENTS_PER_SEC:.0f} events/s; stats avg < "
        f"{STATS_AVG_MS_MAX:.0f}ms & max < {STATS_MAX_MS:.0f}ms; {REQUESTS} reqs @ "
        f"conc {CONCURRENCY} >= {MIN_RPS:.0f} rps with error_rate <= {MAX_ERROR_RATE}; "
        f"memory < {MAX_BACKEND_MEM_MB:.0f} MB"
    )
    with httpx.Client(base_url=BASE_URL) as client:
        wait_ready(client)
        warm_up(client)
        eps = phase_ingest(client)
        stats_ms = phase_stats_latency(client)
        load = phase_concurrency()
        memory_mb = phase_memory(client)

    print("", flush=True)
    print("LOAD PASSED", flush=True)
    info(
        f"summary: ingest {eps:.1f} events/s; stats avg {stats_ms['avg']:.1f}ms "
        f"(max {stats_ms['max']:.1f}ms); concurrent {load['rps']:.1f} rps "
        f"(p95 {load['p95']:.1f}ms, error_rate {load['error_rate']:.4f}); "
        f"memory {memory_mb:.1f} MB"
    )


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print("", flush=True)
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
        print("LOAD FAILED", file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 — any unexpected error is a hard failure
        print("", flush=True)
        print(f"FAIL: unexpected {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        print("LOAD FAILED", file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
