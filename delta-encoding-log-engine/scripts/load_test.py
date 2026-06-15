"""Containerized load test for the delta-encoding log engine (C13).

Run by the compose ``loadtest`` profile service (``Dockerfile.test``), this script
drives the LIVE ``app`` container by **service name** (``http://app:8080`` via
``APP_URL``, never ``localhost``) and proves the project's throughput / latency /
reliability criteria in three phases, exiting 1 if any gate fails. It is a pure
black-box HTTP client (``httpx`` only — no ``app.*`` import).

Phase 1 — compression throughput
    Generate a ``LOAD_BATCH`` batch, then time a single ``POST /api/compress`` over it.
    ``compress_eps = batch / elapsed``. **Gate: compress_eps >= 1000** entries/sec.

Phase 2 — concurrent HTTP load + error rate
    For ``LOAD_DURATION`` seconds, ``LOAD_CONCURRENCY`` async workers hammer a read-heavy
    mix (≈70% ``GET /api/logs/{random}`` + ≈30% ``GET /api/stats``) against the compressed
    log. Total requests, failures (non-2xx or transport exception), rps, and the error
    rate are recorded. **Gate: error_rate <= 0.01** (≤1%); rps + p50/p99 are reported.

Phase 3 — processing throughput
    Time a full end-to-end generate → compress → reconstruct(whole-batch) cycle and
    report ``processing_eps = batch / cycle_seconds``. **Gate: processing_eps >= 100**
    entries/sec end-to-end through the live stack.

After the load, ``GET /api/stats`` is read once more: **gate ``performance
.reconstruct_p99_ms < 100``** (reported) and **gate ``system.errors == 0``** — the
concurrent load above must not have produced a single internal 500.

Environment (all overridable; the compose ``loadtest`` service sets them):
    APP_URL           base URL of the API service     (default http://app:8080)
    LOAD_DURATION     phase-2 measured wall clock secs (default 8)
    LOAD_CONCURRENCY  phase-2 async workers           (default 16)
    LOAD_BATCH        phase-1/3 batch size            (default 10000)

Exit code 0 and ``LOAD: all gates passed`` only when every gate holds.
"""
from __future__ import annotations

import asyncio
import math
import os
import random
import sys
import time
from dataclasses import dataclass, field
from uuid import uuid4

import httpx

APP_URL = os.environ.get("APP_URL", "http://app:8080").rstrip("/")
DURATION_SECONDS = float(os.environ.get("LOAD_DURATION", "8"))
CONCURRENCY = int(os.environ.get("LOAD_CONCURRENCY", "16"))
LOAD_BATCH = int(os.environ.get("LOAD_BATCH", "10000"))

#: Per-run nonce: labels this run and seeds the generated batch so it is reproducible
#: and distinct from any data a warm ``make up`` already holds.
NONCE = uuid4().hex[:8]
SEED = int(NONCE, 16) % 2_000_000_000

# Gates (project success criteria).
MIN_COMPRESS_EPS = 1_000.0  # ≥1000 entries/sec compression
MIN_PROCESSING_EPS = 100.0  # >100 entries/sec end-to-end processing
MAX_ERROR_RATE = 0.01  # ≤1% HTTP error rate under load
MAX_RECONSTRUCT_P99_MS = 100.0  # <100ms reconstruction-latency p99

#: Phase-3 cycle batch — a smaller batch than phase 1 keeps the generate+compress+
#: reconstruct round trip quick while still measuring real end-to-end entries/sec.
PROCESSING_BATCH = min(LOAD_BATCH, 2_000)

_TIMEOUT = 30.0


def pct(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile of an already-sorted list (``p`` in [0, 1])."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    rank = max(1, min(math.ceil(p * n), n))
    return sorted_values[rank - 1]


def _wait_for_health(client: httpx.Client) -> bool:
    """Poll ``GET /health`` up to 30×1s (compose's depends_on makes attempt 1 win)."""
    for _ in range(30):
        try:
            resp = client.get(f"{APP_URL}/health", timeout=5.0)
            if resp.status_code == 200 and resp.json().get("status") == "healthy":
                return True
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return False


# --------------------------------------------------------------------- #
# phase 1 — compression throughput                                      #
# --------------------------------------------------------------------- #


def run_phase1(client: httpx.Client, failures: list[str]) -> dict[str, float]:
    """Generate ``LOAD_BATCH`` entries, time one compress, gate entries/sec.

    The batch is left compressed on the server so phase 2's ``GET /api/logs/{i}`` and
    ``GET /api/stats`` have a real log to read.
    """
    print(f"PHASE 1: compression throughput ({LOAD_BATCH} entries)")
    gen = client.post(
        f"{APP_URL}/api/generate", json={"count": LOAD_BATCH, "seed": SEED}
    )
    if gen.status_code != 200:
        failures.append(f"phase1 /api/generate HTTP {gen.status_code}")
        return {"compress_eps": 0.0}

    start = time.perf_counter()
    comp = client.post(f"{APP_URL}/api/compress", json={"use_generated": True})
    elapsed = time.perf_counter() - start
    if comp.status_code != 200:
        failures.append(f"phase1 /api/compress HTTP {comp.status_code}")
        return {"compress_eps": 0.0}

    compress_eps = LOAD_BATCH / elapsed if elapsed > 0 else float("inf")
    print(f"  compressed {LOAD_BATCH} entries in {elapsed * 1000:.1f}ms")
    print(f"  compress entries/sec:      {compress_eps:,.0f}   (gate >= {MIN_COMPRESS_EPS:,.0f})")
    if compress_eps < MIN_COMPRESS_EPS:
        failures.append(f"compress {compress_eps:,.0f} eps < {MIN_COMPRESS_EPS:,.0f} eps")
    return {"compress_eps": compress_eps}


# --------------------------------------------------------------------- #
# phase 2 — concurrent HTTP load + error rate                           #
# --------------------------------------------------------------------- #


@dataclass
class HttpResults:
    """Per-request outcomes accumulated by every phase-2 worker."""

    latencies_ms: list[float] = field(default_factory=list)
    ok: int = 0
    errors: int = 0

    @property
    def total(self) -> int:
        return self.ok + self.errors

    def record(self, *, success: bool, latency_ms: float) -> None:
        if success:
            self.ok += 1
            self.latencies_ms.append(latency_ms)
        else:
            self.errors += 1


async def _worker(
    client: httpx.AsyncClient,
    deadline: float,
    rng: random.Random,
    results: HttpResults,
) -> None:
    """One worker: ≈70% ``GET /api/logs/{random}`` + ≈30% ``GET /api/stats``.

    Reconstruct indices are drawn from the ``LOAD_BATCH`` compressed log left behind by
    phase 1, so every probe hits a valid entry (a 404 would count as an error).
    """
    while time.perf_counter() < deadline:
        if rng.random() < 0.70:
            path = f"/api/logs/{rng.randrange(LOAD_BATCH)}"
        else:
            path = "/api/stats"
        t0 = time.perf_counter()
        try:
            resp = await client.get(path)
            success = 200 <= resp.status_code < 300
        except Exception:  # noqa: BLE001 — a transport failure is an error
            success = False
        results.record(success=success, latency_ms=(time.perf_counter() - t0) * 1000.0)


async def _run_phase2_async(failures: list[str]) -> dict[str, float]:
    """Drive the measured concurrent-HTTP phase and evaluate the error-rate gate."""
    print(
        f"PHASE 2: concurrent HTTP load on {APP_URL} "
        f"({CONCURRENCY} workers x {DURATION_SECONDS:.0f}s, 70% logs/{{i}} / 30% stats)"
    )
    results = HttpResults()
    master = random.Random(SEED)
    limits = httpx.Limits(
        max_connections=CONCURRENCY * 2, max_keepalive_connections=CONCURRENCY * 2
    )
    async with httpx.AsyncClient(base_url=APP_URL, timeout=_TIMEOUT, limits=limits) as client:
        start = time.perf_counter()
        deadline = start + DURATION_SECONDS
        await asyncio.gather(
            *(
                _worker(client, deadline, random.Random(master.random()), results)
                for _ in range(CONCURRENCY)
            )
        )
        elapsed = time.perf_counter() - start

    rps = results.ok / elapsed if elapsed > 0 else 0.0
    error_rate = results.errors / results.total if results.total else 1.0
    latencies = sorted(results.latencies_ms)
    p50, p99 = pct(latencies, 0.50), pct(latencies, 0.99)

    print(f"  requests ok / errors:      {results.ok} / {results.errors}")
    print(f"  rps:                       {rps:,.1f}")
    print(f"  error rate:                {error_rate:.2%}   (gate <= {MAX_ERROR_RATE:.0%})")
    print(f"  latency p50/p99 ms:        {p50:.2f} / {p99:.2f}")

    if results.total == 0:
        failures.append("phase2 recorded no HTTP requests")
    if error_rate > MAX_ERROR_RATE:
        failures.append(f"HTTP error rate {error_rate:.2%} > {MAX_ERROR_RATE:.0%}")
    return {"rps": rps, "error_rate": error_rate, "p50": p50, "p99": p99}


# --------------------------------------------------------------------- #
# phase 3 — end-to-end processing throughput                            #
# --------------------------------------------------------------------- #


def run_phase3(client: httpx.Client, failures: list[str]) -> dict[str, float]:
    """Time a full generate → compress → reconstruct(all) cycle; gate entries/sec."""
    print(f"PHASE 3: end-to-end processing throughput ({PROCESSING_BATCH} entries/cycle)")
    start = time.perf_counter()

    gen = client.post(
        f"{APP_URL}/api/generate", json={"count": PROCESSING_BATCH, "seed": SEED + 1}
    )
    if gen.status_code != 200:
        failures.append(f"phase3 /api/generate HTTP {gen.status_code}")
        return {"processing_eps": 0.0}
    comp = client.post(f"{APP_URL}/api/compress", json={"use_generated": True})
    if comp.status_code != 200:
        failures.append(f"phase3 /api/compress HTTP {comp.status_code}")
        return {"processing_eps": 0.0}
    rec = client.post(f"{APP_URL}/api/reconstruct", json={})
    if rec.status_code != 200:
        failures.append(f"phase3 /api/reconstruct HTTP {rec.status_code}")
        return {"processing_eps": 0.0}

    elapsed = time.perf_counter() - start
    processing_eps = PROCESSING_BATCH / elapsed if elapsed > 0 else float("inf")
    print(f"  generate+compress+reconstruct of {PROCESSING_BATCH} entries in {elapsed * 1000:.1f}ms")
    print(f"  processing entries/sec:    {processing_eps:,.0f}   (gate >= {MIN_PROCESSING_EPS:,.0f})")
    if processing_eps < MIN_PROCESSING_EPS:
        failures.append(f"processing {processing_eps:,.0f} eps < {MIN_PROCESSING_EPS:,.0f} eps")
    return {"processing_eps": processing_eps}


# --------------------------------------------------------------------- #
# post-load stats gates                                                 #
# --------------------------------------------------------------------- #


def run_poststats(client: httpx.Client, failures: list[str]) -> None:
    """After the load, assert the p99 latency and the zero-errors reliability gates."""
    print("POST-LOAD: /api/stats latency + error gates")
    try:
        data = client.get(f"{APP_URL}/api/stats").json()
    except Exception as exc:  # noqa: BLE001 — without /api/stats the gates fail
        failures.append(f"could not read /api/stats post-load: {exc}")
        return

    perf = data.get("performance", {})
    p99 = perf.get("reconstruct_p99_ms")
    print(f"  reconstruct p99 ms:        {p99}   (gate < {MAX_RECONSTRUCT_P99_MS})")
    if not isinstance(p99, (int, float)):
        failures.append(f"performance.reconstruct_p99_ms missing/not numeric: {p99!r}")
    elif p99 >= MAX_RECONSTRUCT_P99_MS:
        failures.append(f"reconstruct_p99_ms {p99} not < {MAX_RECONSTRUCT_P99_MS}")

    errors = data.get("system", {}).get("errors")
    print(f"  system.errors:             {errors}   (gate == 0)")
    if errors != 0:
        failures.append(f"system.errors == {errors!r} after load, want 0")


# --------------------------------------------------------------------- #
# entrypoint                                                            #
# --------------------------------------------------------------------- #


def main() -> int:
    """Run all phases; print the gate verdicts; return the exit code."""
    print(
        f"LOAD test target: {APP_URL} "
        f"(duration={DURATION_SECONDS:.0f}s, concurrency={CONCURRENCY}, "
        f"batch={LOAD_BATCH}, nonce={NONCE})"
    )
    failures: list[str] = []
    with httpx.Client(timeout=_TIMEOUT) as client:
        if not _wait_for_health(client):
            print(f"LOAD: FAILED — {APP_URL}/health never became ready in 30 attempts")
            return 1
        run_phase1(client, failures)
        asyncio.run(_run_phase2_async(failures))
        run_phase3(client, failures)
        run_poststats(client, failures)

    bar = "=" * 60
    print(bar)
    if failures:
        for failure in failures:
            print(f"GATE FAILED: {failure}")
        print("LOAD: FAILED")
        return 1
    print("LOAD: all gates passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
