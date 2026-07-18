"""Performance + load gates for the NLP Log Processing Engine (C10).

Runs **inside Docker** (the profile-gated ``loadtest`` compose service) against the LIVE
backend over HTTP. Where :mod:`scripts.verify_e2e` answers "is the analysis CORRECT?", this
script answers "does it stay FAST and lean under a realistic message load?" — one concurrent
ingest phase, then four hard gates (fail-fast: the first breached gate exits non-zero so
``make load`` propagates it).

The phase: generate ``LOAD_MESSAGES`` deterministic log lines from
:func:`~src.generators.sample_messages` and fire them all at ``POST /api/analyze`` through an
``httpx.AsyncClient`` bounded by an ``asyncio.Semaphore(LOAD_CONCURRENCY)``, recording each
request's wall latency and status. Then it computes and gates on:

* **throughput** = successful msgs / wall seconds  ->  gate ``>= MIN_MSGS_PER_SEC``
* **latency**    p50 / p95 / p99 of the per-request latencies (measured UNDER the concurrent
  load, so they include queuing)  ->  gate ``p95 <= MAX_P95_MS``
* **errors**     every request must return HTTP 200  ->  gate ``error rate == 0``
* **memory**     backend RSS via ``GET /api/debug/memory`` after the load  ->  gate
  ``<= MAX_BACKEND_MEM_MB``

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``          backend base URL (default ``http://backend:8000``)
* ``LOAD_READY_TIMEOUT``  seconds to wait for /api/health (default 90)
* ``LOAD_MESSAGES``       total analyze POSTs fired (default 2000)
* ``LOAD_CONCURRENCY``    max in-flight POSTs (default 20)
* ``MIN_MSGS_PER_SEC``    throughput gate (default 100)
* ``MAX_P95_MS``          under-load p95 latency gate, ms (default 500)
* ``MAX_BACKEND_MEM_MB``  backend RSS gate, MB (default 500)

The defaults are calibrated to the measured reality — single-request analyze latency is
~10 ms and the backend RSS is ~250 MB — so throughput clears 100 msgs/s and p95 clears 500 ms
with margin while RSS sits well under the 500 MB ceiling. Every gate is host-overridable, so a
deliberately impossible bar bites and proves the gate is real, e.g.
``MIN_MSGS_PER_SEC=100000 make load`` (throughput) or ``MAX_BACKEND_MEM_MB=1 make load``
(memory) MUST fail. The run ends with machine-readable ``RESULT key=value`` lines and
``LOAD PASSED``, or ``FAIL: <reason>`` + ``LOAD FAILED (<gate>)`` on stderr with a non-zero exit.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
import time

import httpx

from src.generators import sample_messages

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://backend:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "90"))
LOAD_MESSAGES = int(os.environ.get("LOAD_MESSAGES", "2000"))
CONCURRENCY = max(1, int(os.environ.get("LOAD_CONCURRENCY", "20")))
MIN_MSGS_PER_SEC = float(os.environ.get("MIN_MSGS_PER_SEC", "100"))
MAX_P95_MS = float(os.environ.get("MAX_P95_MS", "500"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "500"))

#: Seed for the deterministic message corpus (same lines every run).
_LOAD_SEED = 0
#: Discarded warm-up requests (connection pool, first-hit lazy paths) before timing.
_WARMUP_HEALTH = 5
_REQUEST_TIMEOUT = 60.0


class CheckError(AssertionError):
    """Raised to fail a load gate with a clear, single-line message."""


def check(cond: bool, msg: str) -> None:
    """Assert ``cond``; raise :class:`CheckError` with ``msg`` when it is falsy."""
    if not cond:
        raise CheckError(msg)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live)."""
    print(f"[load] {msg}", flush=True)


def result(key: str, value: object) -> None:
    """Print one machine-readable summary line (scraped for the README)."""
    print(f"RESULT {key}={value}", flush=True)


def percentile(values: list[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100); 0.0 for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)]


# --------------------------------------------------------------------------- #
# Setup: readiness + warm-up
# --------------------------------------------------------------------------- #
def wait_ready(client: httpx.Client, timeout: float = READY_TIMEOUT) -> None:
    """Poll GET /api/health until it answers 200, within the timeout."""
    info(f"waiting for {BASE_URL}/api/health (up to {timeout:.0f}s)...")
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            resp = client.get("/api/health", timeout=5.0)
            if resp.status_code == 200:
                info("backend is ready")
                return
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 — the service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    raise CheckError(f"/api/health not ready after {timeout:.0f}s (last: {last})")


def warm_up(client: httpx.Client) -> None:
    """Fire discarded /api/health GETs + one analyze before timing anything."""
    for i in range(_WARMUP_HEALTH):
        resp = client.get("/api/health", timeout=_REQUEST_TIMEOUT)
        check(resp.status_code == 200, f"warm-up GET /api/health #{i + 1} -> HTTP {resp.status_code}")
    resp = client.post(
        "/api/analyze",
        json={"message": "warm-up: gateway health check on web-01"},
        timeout=_REQUEST_TIMEOUT,
    )
    check(resp.status_code == 200, f"warm-up analyze -> HTTP {resp.status_code}")
    info(f"warm-up done ({_WARMUP_HEALTH} discarded /api/health GETs + 1 analyze)")


# --------------------------------------------------------------------------- #
# The concurrent load phase
# --------------------------------------------------------------------------- #
def _load_messages() -> list[str]:
    """The deterministic corpus fired at the analyzer (``LOAD_MESSAGES`` seeded lines)."""
    return [sample.message for sample in sample_messages(LOAD_MESSAGES, seed=_LOAD_SEED)]


async def _run_load(messages: list[str]) -> tuple[float, list[float], list[int]]:
    """POST every message concurrently through the semaphore; return (wall, latencies, statuses).

    Each task records its own wall latency (ms) and HTTP status; a request that raises is
    recorded as status ``0`` (a hard error) so the zero-error gate sees it. Wall time spans the
    first dispatch to the last completion, so ``throughput = successes / wall`` is the real
    end-to-end ingest rate at concurrency ``CONCURRENCY``.
    """
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies_ms: list[float] = []
    statuses: list[int] = []

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=_REQUEST_TIMEOUT) as client:

        async def one(message: str) -> None:
            async with sem:
                t0 = time.perf_counter()
                try:
                    resp = await client.post("/api/analyze", json={"message": message})
                    latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                    statuses.append(resp.status_code)
                except Exception:  # noqa: BLE001 — any failure is a hard error (status 0)
                    latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                    statuses.append(0)

        wall0 = time.perf_counter()
        await asyncio.gather(*(one(message) for message in messages))
        wall = time.perf_counter() - wall0

    return wall, latencies_ms, statuses


def _backend_memory_mb(client: httpx.Client) -> float:
    """Read the BACKEND's own reported RSS (never this client's) via /api/debug/memory."""
    resp = client.get("/api/debug/memory", timeout=_REQUEST_TIMEOUT)
    check(resp.status_code == 200, f"GET /api/debug/memory -> HTTP {resp.status_code}")
    memory_mb = resp.json().get("memory_mb")
    check(
        isinstance(memory_mb, (int, float)) and not isinstance(memory_mb, bool),
        f"/api/debug/memory memory_mb is {memory_mb!r} (want a number; /proc unavailable?)",
    )
    return float(memory_mb)


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== load test against {BASE_URL} ==")
    info(
        f"gates: throughput >= {MIN_MSGS_PER_SEC:.0f} msgs/s over {LOAD_MESSAGES} messages at "
        f"concurrency {CONCURRENCY}; p95 <= {MAX_P95_MS:.0f}ms; error rate == 0; "
        f"memory <= {MAX_BACKEND_MEM_MB:.0f} MB"
    )
    with httpx.Client(base_url=BASE_URL, timeout=_REQUEST_TIMEOUT) as client:
        wait_ready(client)
        warm_up(client)
        info(f"firing {LOAD_MESSAGES} analyze POSTs at concurrency {CONCURRENCY}...")
        wall, latencies_ms, statuses = asyncio.run(_run_load(_load_messages()))
        memory_mb = _backend_memory_mb(client)

    ok = sum(1 for status in statuses if status == 200)
    errors = len(statuses) - ok
    throughput = (ok / wall) if wall > 0 else 0.0
    p50 = percentile(latencies_ms, 50)
    p95 = percentile(latencies_ms, 95)
    p99 = percentile(latencies_ms, 99)
    worst = max(latencies_ms) if latencies_ms else 0.0

    # Human-scannable report of every measured number.
    info(f"processed {ok}/{LOAD_MESSAGES} messages in {wall:.2f}s (errors={errors})")
    info(f"throughput: {throughput:.1f} msgs/second")
    info(f"latency: p50 {p50:.1f}ms / p95 {p95:.1f}ms / p99 {p99:.1f}ms / max {worst:.1f}ms")
    info(f"backend memory: {memory_mb:.1f} MB")

    # Machine-readable summary lines (scraped for the README's measured-numbers table).
    result("messages", LOAD_MESSAGES)
    result("concurrency", CONCURRENCY)
    result("wall_s", f"{wall:.2f}")
    result("throughput_msgs_per_sec", f"{throughput:.1f}")
    result("errors", errors)
    result("p50_ms", f"{p50:.1f}")
    result("p95_ms", f"{p95:.1f}")
    result("p99_ms", f"{p99:.1f}")
    result("memory_mb", f"{memory_mb:.1f}")

    # Gates (fail-fast; the message names the breached gate).
    check(errors == 0, f"error rate: {errors}/{len(statuses)} requests did not return HTTP 200")
    check(
        throughput >= MIN_MSGS_PER_SEC,
        f"throughput {throughput:.1f} msgs/s below gate {MIN_MSGS_PER_SEC:.0f} "
        f"({ok} messages in {wall:.2f}s)",
    )
    check(p95 <= MAX_P95_MS, f"p95 latency {p95:.1f}ms above gate {MAX_P95_MS:.0f}ms")
    check(
        memory_mb <= MAX_BACKEND_MEM_MB,
        f"backend memory {memory_mb:.1f} MB above gate {MAX_BACKEND_MEM_MB:.0f} MB",
    )

    print("", flush=True)
    info(
        f"RESULT (headline) throughput_msgs_per_sec={throughput:.1f} p95_ms={p95:.1f} "
        f"errors={errors} memory_mb={memory_mb:.1f}"
    )
    print("LOAD PASSED", flush=True)


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print("", flush=True)
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
        print(f"LOAD FAILED ({exc})", file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 — any unexpected error is a hard failure
        print("", flush=True)
        print(f"FAIL: unexpected {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        print("LOAD FAILED (unexpected error)", file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
