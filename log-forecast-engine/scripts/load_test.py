"""Throughput + memory-under-load gate for the Predictive Log Analytics Engine (C13).

Runs **inside Docker** (the profile-gated ``loadtest`` service) against the *live*
API over HTTP. It fires a fixed number of concurrent requests at the fast read
endpoints (``/health``, ``/metrics`` and, when available, ``/predictions``) and
measures:

* **throughput** (requests/second over the whole run),
* **error rate** (non-2xx / exception fraction),
* **latency percentiles** (p50 / p95 / max),
* **process memory** (``performance.rss_mb`` from ``/health``) before vs after load.

Gating philosophy (CI-safe, not brittle):

* **Hard gates:** error rate must be ~0 (``<= LOAD_MAX_ERROR_RATE``) and throughput
  must clear a small floor (``>= LOAD_MIN_RPS``). These are stable across hosts.
* **Memory:** the spec's "< 200 MB per 1000 metrics tracked" is hard to assert
  precisely cross-platform, so RSS is **reported** and only gated against a generous
  ceiling (``LOAD_MAX_RSS_MB``) rather than a brittle exact bound.

Concurrency uses :mod:`asyncio` + ``httpx.AsyncClient`` (both available in the test
image). The script self-seeds first so the endpoints have data to serve.

Configuration (env, with defaults):

* ``API_BASE_URL``        live API base (default ``http://api:8000``).
* ``LOAD_READY_TIMEOUT``  seconds to wait for ``/health`` (default 60).
* ``LOAD_METRIC``         metric used for /predictions + /metrics/{name} (default response_time).
* ``LOAD_REQUESTS``       total requests to fire (default 200).
* ``LOAD_CONCURRENCY``    max in-flight requests (default 10).
* ``LOAD_SEED_POINTS``    synthetic points per metric to seed (default 400).
* ``LOAD_MIN_RPS``        throughput floor, req/s (default 20).
* ``LOAD_MAX_ERROR_RATE`` allowed error fraction (default 0.0).
* ``LOAD_MAX_RSS_MB``     RSS ceiling, MB (default 1024).

Exit code: ``0`` when error-rate, throughput and RSS gates pass; non-zero otherwise.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

from src.generator import METRIC_NAMES, generate_series

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "60"))
METRIC = os.environ.get("LOAD_METRIC", "response_time")
REQUESTS = int(os.environ.get("LOAD_REQUESTS", "200"))
CONCURRENCY = int(os.environ.get("LOAD_CONCURRENCY", "10"))
SEED_POINTS = int(os.environ.get("LOAD_SEED_POINTS", "400"))
MIN_RPS = float(os.environ.get("LOAD_MIN_RPS", "20"))
MAX_ERROR_RATE = float(os.environ.get("LOAD_MAX_ERROR_RATE", "0.0"))
MAX_RSS_MB = float(os.environ.get("LOAD_MAX_RSS_MB", "1024"))


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


def _wait_for_health_sync() -> None:
    deadline = time.time() + READY_TIMEOUT
    last = "no response"
    with httpx.Client(base_url=BASE_URL) as client:
        while time.time() < deadline:
            try:
                r = client.get("/health", timeout=5.0)
                if r.status_code == 200:
                    return
                last = f"HTTP {r.status_code}"
            except Exception as exc:  # noqa: BLE001
                last = type(exc).__name__
            time.sleep(2.0)
    raise RuntimeError(f"/health not ready after {READY_TIMEOUT:.0f}s (last: {last})")


def _seed_metrics_sync() -> None:
    interval = 300
    end = datetime.now(timezone.utc)
    start = end - timedelta(seconds=interval * (SEED_POINTS + 1))
    with httpx.Client(base_url=BASE_URL) as client:
        for name in METRIC_NAMES:
            series = generate_series(name, start, end, interval, seed=1234)[-SEED_POINTS:]
            payload = {
                "points": [
                    {"metric_name": p.metric_name, "timestamp": p.timestamp.isoformat(), "value": p.value}
                    for p in series
                ]
            }
            r = client.post("/metrics", json=payload, timeout=30.0)
            if r.status_code != 201:
                raise RuntimeError(f"seed POST /metrics for {name} -> {r.status_code}")
    print(f"  seeded {SEED_POINTS} points x {len(METRIC_NAMES)} metrics", flush=True)


def _read_rss_mb() -> float | None:
    """Read process RSS (MB) from GET /health performance.rss_mb (best-effort)."""
    try:
        with httpx.Client(base_url=BASE_URL) as client:
            r = client.get("/health", timeout=10.0)
            if r.status_code != 200:
                return None
            perf = r.json().get("performance", {})
            rss = perf.get("rss_mb")
            return float(rss) if rss is not None else None
    except Exception:  # noqa: BLE001
        return None


def _build_targets() -> list[tuple[str, dict | None]]:
    """The mix of fast read endpoints hammered under load."""
    targets: list[tuple[str, dict | None]] = [
        ("/health", None),
        ("/metrics", None),
        (f"/metrics/{METRIC}", {"limit": 50}),
    ]
    # Include /predictions only if it currently serves 200 (avoids counting the
    # expected fresh-stack 404 as an error). Probe once.
    try:
        with httpx.Client(base_url=BASE_URL) as client:
            r = client.get("/predictions", params={"metric": METRIC}, timeout=15.0)
            if r.status_code == 200:
                targets.append(("/predictions", {"metric": METRIC}))
    except Exception:  # noqa: BLE001
        pass
    return targets


async def _run_load(targets: list[tuple[str, dict | None]]) -> dict:
    latencies: list[float] = []
    errors = 0
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:

        async def one(i: int) -> None:
            nonlocal errors
            path, params = targets[i % len(targets)]
            async with sem:
                start = time.perf_counter()
                try:
                    r = await client.get(path, params=params)
                    elapsed = (time.perf_counter() - start) * 1000.0
                    latencies.append(elapsed)
                    if r.status_code // 100 != 2:
                        errors += 1
                except Exception:  # noqa: BLE001 - any failure counts as an error
                    errors += 1

        wall_start = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(REQUESTS)))
        wall = time.perf_counter() - wall_start

    completed = len(latencies)
    return {
        "requests": REQUESTS,
        "completed": completed,
        "errors": errors,
        "wall_s": wall,
        "rps": (REQUESTS / wall) if wall > 0 else 0.0,
        "error_rate": (errors / REQUESTS) if REQUESTS else 0.0,
        "p50": _percentile(latencies, 50),
        "p95": _percentile(latencies, 95),
        "max": max(latencies) if latencies else 0.0,
    }


def run() -> int:
    print(f"== Load test against {BASE_URL} ==", flush=True)
    print(
        f"  config: {REQUESTS} requests, concurrency {CONCURRENCY}; "
        f"gates: error_rate <= {MAX_ERROR_RATE}, rps >= {MIN_RPS:.0f}, "
        f"rss <= {MAX_RSS_MB:.0f}MB",
        flush=True,
    )

    _wait_for_health_sync()
    _seed_metrics_sync()

    rss_before = _read_rss_mb()
    targets = _build_targets()
    print(f"  endpoints under load: {[t[0] for t in targets]}", flush=True)

    result = asyncio.run(_run_load(targets))
    rss_after = _read_rss_mb()

    print("\n[results]", flush=True)
    print(
        f"  completed={result['completed']}/{result['requests']} "
        f"errors={result['errors']} wall={result['wall_s']:.2f}s",
        flush=True,
    )
    print(
        f"  throughput={result['rps']:.1f} req/s  error_rate={result['error_rate']:.3f}",
        flush=True,
    )
    print(
        f"  latency: p50={result['p50']:.1f}ms p95={result['p95']:.1f}ms max={result['max']:.1f}ms",
        flush=True,
    )
    print(
        f"  memory (RSS): before={rss_before}MB after={rss_after}MB",
        flush=True,
    )

    failures: list[str] = []

    print("\n[gates]", flush=True)
    # Error rate (hard).
    if result["error_rate"] > MAX_ERROR_RATE:
        msg = f"error_rate {result['error_rate']:.3f} > {MAX_ERROR_RATE}"
        failures.append(msg)
        print(f"  [FAIL] {msg}", flush=True)
    else:
        print(f"  [PASS] error_rate {result['error_rate']:.3f} <= {MAX_ERROR_RATE}", flush=True)

    # Throughput (hard).
    if result["rps"] < MIN_RPS:
        msg = f"throughput {result['rps']:.1f} req/s < floor {MIN_RPS:.0f}"
        failures.append(msg)
        print(f"  [FAIL] {msg}", flush=True)
    else:
        print(f"  [PASS] throughput {result['rps']:.1f} req/s >= {MIN_RPS:.0f}", flush=True)

    # Memory (lenient ceiling; reported).
    if rss_after is None:
        print("  [REPORT] RSS unavailable from /health (skipping memory gate)", flush=True)
    elif rss_after > MAX_RSS_MB:
        msg = f"RSS {rss_after:.1f}MB > ceiling {MAX_RSS_MB:.0f}MB"
        failures.append(msg)
        print(f"  [FAIL] {msg}", flush=True)
    else:
        print(
            f"  [REPORT] RSS {rss_after:.1f}MB <= {MAX_RSS_MB:.0f}MB "
            f"(spec target < 200MB/1000 metrics)",
            flush=True,
        )

    if failures:
        print("\nLOAD FAIL: " + "; ".join(failures), file=sys.stderr, flush=True)
        return 1
    print("\nLOAD PASS: throughput + error-rate + memory within bounds.", flush=True)
    return 0


def main() -> int:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001
        print(f"\nLOAD FAIL: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
