"""Performance + load gates for the RCA Analysis Engine (C10).

Runs **inside Docker** (the profile-gated ``loadtest`` compose service) against the LIVE
backend over HTTP. Where :mod:`scripts.verify_e2e` answers "is the analysis CORRECT?",
this script answers "does it stay FAST under a realistic ingest load?" in three
hard-gated phases (fail-fast: the first breached gate exits non-zero so ``make load``
propagates it):

* **Phase A — throughput**: fire ``LOAD_EVENTS`` (default 1000) synthetic events at the
  analyzer and report ``events/second``. The events are split into
  ``LOAD_EVENTS / LOAD_EVENTS_PER_INCIDENT`` incidents (default 50 incidents x 20 events)
  POSTed concurrently through an ``asyncio.Semaphore(LOAD_CONCURRENCY)``; throughput =
  ``total_events / wall_seconds``. Spreading the events across many small incidents keeps
  each causal graph sparse and realistic (a single 1000-event dense in-window graph is
  neither). Gate: ``events_per_sec >= MIN_EVENTS_PER_SEC`` (default 1000).
* **Phase B — analyze latency**: ``LOAD_LATENCY_SAMPLES`` sequential single-incident
  analyze POSTs; report p50 / p95 / p99. Gate: ``p95_ms <= MAX_P95_MS`` (default 2000).
* **Phase C — memory**: ``GET /api/debug/memory`` after the load; gate ``memory_mb <=
  MAX_BACKEND_MEM_MB``. The spec's hard ceiling is 500 MB, so that is the DEFAULT gate (a
  heavy scipy/sklearn RSS must not false-fail); 200 MB is the aspirational target — always
  REPORTED, and reachable by overriding the gate to 200.

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``               backend base URL (default ``http://backend:8000``)
* ``LOAD_READY_TIMEOUT``       seconds to wait for /api/health (default 90)
* ``LOAD_EVENTS``              total events fired in Phase A (default 1000)
* ``LOAD_EVENTS_PER_INCIDENT`` events per incident in Phase A (default 20)
* ``LOAD_CONCURRENCY``         max in-flight analyze POSTs in Phase A (default 20)
* ``MIN_EVENTS_PER_SEC``       Phase A throughput gate (default 1000)
* ``LOAD_LATENCY_SAMPLES``     Phase B sequential-analyze sample size (default 50)
* ``MAX_P95_MS``               Phase B p95 analyze-latency gate, ms (default 2000)
* ``MAX_BACKEND_MEM_MB``       Phase C memory gate, MB (default 500; aspirational 200)

Output ends with machine-readable ``RESULT key=value`` lines (for the README's
measured-numbers table) and ``LOAD PASSED``, or ``FAIL: <reason>`` on stderr with a
non-zero exit.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
import time
from typing import Any

import httpx

from src.generators import generate_events, generate_incident
from src.models import LogEvent

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://backend:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "90"))
LOAD_EVENTS = int(os.environ.get("LOAD_EVENTS", "1000"))
EVENTS_PER_INCIDENT = max(1, int(os.environ.get("LOAD_EVENTS_PER_INCIDENT", "20")))
CONCURRENCY = max(1, int(os.environ.get("LOAD_CONCURRENCY", "20")))
MIN_EVENTS_PER_SEC = float(os.environ.get("MIN_EVENTS_PER_SEC", "1000"))
LATENCY_SAMPLES = int(os.environ.get("LOAD_LATENCY_SAMPLES", "50"))
MAX_P95_MS = float(os.environ.get("MAX_P95_MS", "2000"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "500"))

#: The aspirational (soft) memory target — reported, never the default gate.
ASPIRATIONAL_MEM_MB = 200.0

#: Discarded warm-up requests (connection pools, first-hit lazy imports) so the measured
#: phases see a warm service.
WARMUP_HEALTH = 5
REQUEST_TIMEOUT = 60.0


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


def _events_json(events: list[LogEvent]) -> list[dict]:
    """Serialize LogEvents to the JSON array POSTed to ``/api/analyze-incident``."""
    return [event.model_dump(mode="json") for event in events]


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
    """Fire discarded /api/health GETs + one small analyze before timing anything."""
    for i in range(WARMUP_HEALTH):
        resp = client.get("/api/health", timeout=REQUEST_TIMEOUT)
        check(resp.status_code == 200, f"warm-up GET /api/health #{i + 1} -> HTTP {resp.status_code}")
    resp = client.post(
        "/api/analyze-incident",
        json=_events_json(generate_incident(seed=0).events),
        timeout=REQUEST_TIMEOUT,
    )
    check(resp.status_code == 200, f"warm-up analyze -> HTTP {resp.status_code}")
    info(f"warm-up done ({WARMUP_HEALTH} discarded /api/health GETs + 1 analyze)")


# --------------------------------------------------------------------------- #
# Phase A — throughput (concurrent ingestion of LOAD_EVENTS across many incidents)
# --------------------------------------------------------------------------- #
def _incident_batches() -> list[list[dict]]:
    """Split LOAD_EVENTS synthetic events into per-incident JSON batches.

    One deterministic ``generate_events(LOAD_EVENTS)`` stream is sliced into contiguous
    chunks of ``EVENTS_PER_INCIDENT`` — each chunk is a small, in-window, sparse incident
    (the article's model), and together they cover exactly ``LOAD_EVENTS`` events.
    """
    events = generate_events(LOAD_EVENTS, seed=0)
    return [
        _events_json(events[i : i + EVENTS_PER_INCIDENT])
        for i in range(0, len(events), EVENTS_PER_INCIDENT)
    ]


async def _throughput() -> dict[str, float]:
    """POST every incident batch concurrently through the semaphore; measure it."""
    batches = _incident_batches()
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies_ms: list[float] = []
    processed_events = 0
    root_causes = 0
    errors = 0

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as client:

        async def one(batch: list[dict]) -> None:
            nonlocal processed_events, root_causes, errors
            async with sem:
                t0 = time.perf_counter()
                try:
                    resp = await client.post("/api/analyze-incident", json=batch)
                    latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                    if resp.status_code == 200:
                        processed_events += len(batch)
                        root_causes += len(resp.json().get("root_causes") or [])
                    else:
                        errors += 1
                except Exception:  # noqa: BLE001 — any failure counts as an error
                    errors += 1

        wall0 = time.perf_counter()
        await asyncio.gather(*(one(batch) for batch in batches))
        wall = time.perf_counter() - wall0

    return {
        "wall_s": wall,
        "incidents": float(len(batches)),
        "processed_events": float(processed_events),
        "root_causes": float(root_causes),
        "errors": float(errors),
        "events_per_sec": (processed_events / wall) if wall > 0 else 0.0,
        "p95_ms": percentile(latencies_ms, 95),
    }


def phase_throughput() -> dict[str, float]:
    """Gate: sustained ingest throughput over the full LOAD_EVENTS batch."""
    info(
        f"phase A: firing {LOAD_EVENTS} events as "
        f"{math.ceil(LOAD_EVENTS / EVENTS_PER_INCIDENT)} incidents "
        f"({EVENTS_PER_INCIDENT} events each) at concurrency {CONCURRENCY}..."
    )
    stats = asyncio.run(_throughput())
    eps = stats["events_per_sec"]
    wall = stats["wall_s"]
    info(
        f"phase A: {stats['processed_events']:.0f}/{LOAD_EVENTS} events across "
        f"{stats['incidents']:.0f} incidents in {wall:.2f}s "
        f"(errors={stats['errors']:.0f}); {stats['root_causes']:.0f} root causes identified"
    )
    # Article-style summary lines.
    info(f"✅ Processed {stats['processed_events']:.0f} events in {wall:.2f}s")
    info(f"✅ Throughput: {eps:.1f} events/second")
    result("events_per_sec", f"{eps:.1f}")
    result("throughput_wall_s", f"{wall:.2f}")
    result("root_causes", f"{stats['root_causes']:.0f}")
    check(
        stats["errors"] == 0,
        f"{stats['errors']:.0f} analyze POSTs failed during the throughput phase",
    )
    check(
        eps >= MIN_EVENTS_PER_SEC,
        f"throughput {eps:.1f} events/s below gate {MIN_EVENTS_PER_SEC:.0f} "
        f"({stats['processed_events']:.0f} events in {wall:.2f}s)",
    )
    return stats


# --------------------------------------------------------------------------- #
# Phase B — sequential analyze latency
# --------------------------------------------------------------------------- #
def phase_latency(client: httpx.Client) -> dict[str, float]:
    """Gate: single-incident analyze p95 stays under the ceiling."""
    info(f"phase B: {LATENCY_SAMPLES} sequential single-incident analyze POSTs...")
    samples_ms: list[float] = []
    for i in range(LATENCY_SAMPLES):
        payload = _events_json(generate_incident(seed=5000 + i).events)
        t0 = time.perf_counter()
        resp = client.post("/api/analyze-incident", json=payload, timeout=REQUEST_TIMEOUT)
        samples_ms.append((time.perf_counter() - t0) * 1000.0)
        check(resp.status_code == 200, f"latency analyze #{i + 1} -> HTTP {resp.status_code}")
    p50 = percentile(samples_ms, 50)
    p95 = percentile(samples_ms, 95)
    p99 = percentile(samples_ms, 99)
    worst = max(samples_ms)
    info(f"phase B: p50 {p50:.1f}ms / p95 {p95:.1f}ms / p99 {p99:.1f}ms / max {worst:.1f}ms")
    result("p50_ms", f"{p50:.1f}")
    result("p95_ms", f"{p95:.1f}")
    result("p99_ms", f"{p99:.1f}")
    check(
        p95 <= MAX_P95_MS,
        f"analyze p95 {p95:.1f}ms > gate {MAX_P95_MS:.0f}ms over n={LATENCY_SAMPLES}",
    )
    return {"p50": p50, "p95": p95, "p99": p99, "max": worst}


# --------------------------------------------------------------------------- #
# Phase C — server-side memory
# --------------------------------------------------------------------------- #
def phase_memory(client: httpx.Client) -> float:
    """Gate: the BACKEND's own reported RSS (never this client's) stays under the ceiling."""
    resp = client.get("/api/debug/memory", timeout=REQUEST_TIMEOUT)
    check(resp.status_code == 200, f"GET /api/debug/memory -> HTTP {resp.status_code}")
    memory_mb = resp.json().get("memory_mb")
    check(
        isinstance(memory_mb, (int, float)) and not isinstance(memory_mb, bool),
        f"/api/debug/memory memory_mb is {memory_mb!r} (want a number; /proc unavailable?)",
    )
    aspirational = "MET" if memory_mb <= ASPIRATIONAL_MEM_MB else "NOT MET"
    info(
        f"phase C: backend memory {memory_mb:.1f} MB "
        f"(gate {MAX_BACKEND_MEM_MB:.0f} MB; aspirational {ASPIRATIONAL_MEM_MB:.0f} MB {aspirational})"
    )
    result("memory_mb", f"{memory_mb:.1f}")
    result("memory_aspirational_200_met", "yes" if memory_mb <= ASPIRATIONAL_MEM_MB else "no")
    check(
        memory_mb <= MAX_BACKEND_MEM_MB,
        f"backend memory {memory_mb:.1f} MB > gate {MAX_BACKEND_MEM_MB:.0f} MB",
    )
    return float(memory_mb)


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== load test against {BASE_URL} ==")
    info(
        f"gates: throughput >= {MIN_EVENTS_PER_SEC:.0f} events/s over {LOAD_EVENTS} events; "
        f"analyze p95 <= {MAX_P95_MS:.0f}ms; memory <= {MAX_BACKEND_MEM_MB:.0f} MB "
        f"(aspirational {ASPIRATIONAL_MEM_MB:.0f} MB)"
    )
    with httpx.Client(base_url=BASE_URL) as client:
        wait_ready(client)
        warm_up(client)
        throughput = phase_throughput()
        latency = phase_latency(client)
        memory_mb = phase_memory(client)

    print("", flush=True)
    # Consolidated, human-scannable echo of the four headline gates (each is also emitted
    # above as its own machine-readable RESULT key=value line).
    info(
        f"RESULT (headline) events_per_sec={throughput['events_per_sec']:.1f} "
        f"p95_ms={latency['p95']:.1f} memory_mb={memory_mb:.1f} "
        f"root_causes={throughput['root_causes']:.0f}"
    )
    print("LOAD PASSED", flush=True)


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
