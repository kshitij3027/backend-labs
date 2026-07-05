"""Concurrent throughput + error-rate gate for the Log Recommendation Engine (C20).

Runs **inside Docker** (the profile-gated ``loadtest`` service) against the *live* API
over HTTP — no in-process imports of ``src``, no direct DB/Redis access. Where the C19
``scripts/perf_test.py`` sizes a *single*-request latency tail (p95 of one call at a
time), this script drives **concurrent** load and answers the complementary question:
how many requests/second can the service sustain, and does it stay error-free under that
pressure?

    fire LOAD_REQUESTS  POST /recommend  concurrently (a semaphore of LOAD_CONCURRENCY
        in flight) with varied+unique query text (cache misses -> real embed + K-NN +
        blend + persist work per request)
      -> RPS = completed / wall-clock,  error_rate = (non-2xx | exception) / total
      -> latency p50 / p95 / max over the batch

and it also measures an **ingest throughput** pass (concurrent ``POST /incidents``,
reported/lenient) and a best-effort **memory** figure, then **hard-gates**:

* throughput  ``RPS >= LOAD_MIN_RPS``            (default 15 — conservative: a single
  uvicorn worker + CPU-bound MiniLM embedding bounds achievable RPS on a CI host), and
* error rate  ``error_rate <= LOAD_MAX_ERROR_RATE`` (default 0.0 — no request may fail).

Memory is **reported only, never a gate** (see below). A failure of *either* hard gate
exits non-zero with a loud ``LOAD FAILED`` so ``make load`` propagates it.

Why the queries are varied
--------------------------
An identical repeated ``/recommend`` body is served straight from the Redis
recommendation cache (``cached=True``) and skips the whole compute path — that would
measure Redis, not the recommender under load. So every fired request interpolates a
unique per-request marker into its description (a fresh cache key), and the observed
cache-hit rate is reported so a surprise (everything cached) is visible. The script
warms the model up first (a couple of discarded recommends; the very first loads the
~90 MB MiniLM model, which would otherwise dominate the concurrent batch).

Why the throughput floor is conservative
-----------------------------------------
The ``api`` runs a single uvicorn worker and each ``/recommend`` does CPU-bound
sentence-transformer inference, so true parallelism is limited by the GIL + one CPU.
``LOAD_MIN_RPS`` is therefore a *floor* (a regression / stall detector), not a target —
it is set low enough to be stable on a loaded CI host while still catching a service
that has fallen over or serialised to a crawl.

Memory (best-effort, reported — NEVER fails)
--------------------------------------------
This app registers no Prometheus ``ProcessCollector``, so ``GET /metrics`` does **not**
expose ``process_resident_memory_bytes`` by default. The script still *tries* to scrape
a resident-memory gauge from ``/metrics`` (so if a future commit adds one it is picked
up automatically); if none is present it falls back to reporting **this loadtest
client's own** RSS via :func:`resource.getrusage` — clearly labelled as the *client*
figure, not the API's. ``ru_maxrss`` is in **bytes on macOS but kilobytes on Linux**
(the Docker runtime), so the value is normalised to MB with the platform in mind.
Memory is only checked against a generous, lenient ceiling (``LOAD_MAX_RSS_MB``) and a
breach is *reported*, not gated — a memory number is never allowed to fail ``make load``.

Concurrency uses :mod:`asyncio` + ``httpx.AsyncClient`` (both in the tester image).

Configuration (all via env, with sensible defaults):

* ``LOAD_BASE_URL``        base URL of the live API (default ``http://api:8000``).
* ``LOAD_READY_TIMEOUT``   seconds to wait for ``/health`` (default 90).
* ``LOAD_SEED_INCIDENTS``  min corpus size before load; seed up to it (default 120).
* ``LOAD_REQUESTS``        concurrent ``POST /recommend`` calls to fire (default 200).
* ``LOAD_CONCURRENCY``     max in-flight requests (semaphore size; default 10).
* ``LOAD_INGEST_N``        concurrent ``POST /incidents`` in the ingest pass (default 60).
* ``LOAD_MIN_RPS``         **hard gate**: recommend RPS must be >= this (default 15).
* ``LOAD_MAX_ERROR_RATE``  **hard gate**: recommend error rate must be <= this (default 0.0).
* ``LOAD_MAX_RSS_MB``      reported/lenient RSS ceiling, MB (default 1024).

Exit code: ``0`` with ``LOAD PASSED ✅`` only when BOTH hard gates hold; non-zero with a
loud ``FAIL:`` line otherwise — so ``make load`` propagates the failure.
"""

from __future__ import annotations

import asyncio
import os
import resource
import sys
import time
from typing import Any

import httpx

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("LOAD_BASE_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("LOAD_READY_TIMEOUT", "90"))
SEED_INCIDENTS = int(os.environ.get("LOAD_SEED_INCIDENTS", "120"))
REQUESTS = int(os.environ.get("LOAD_REQUESTS", "200"))
CONCURRENCY = max(1, int(os.environ.get("LOAD_CONCURRENCY", "10")))
INGEST_N = int(os.environ.get("LOAD_INGEST_N", "60"))
MIN_RPS = float(os.environ.get("LOAD_MIN_RPS", "15"))          # hard gate
MAX_ERROR_RATE = float(os.environ.get("LOAD_MAX_ERROR_RATE", "0.0"))  # hard gate
MAX_RSS_MB = float(os.environ.get("LOAD_MAX_RSS_MB", "1024"))  # reported / lenient

#: Per-request client timeout under load. Generous: a concurrent burst against a
#: single CPU-bound worker can queue, and a *slow* request is still a completed
#: request — we only want genuine failures (5xx / connection drops) to count as errors.
REQUEST_TIMEOUT = 120.0


# --------------------------------------------------------------------------- #
# Assertion + logging helpers (mirrors scripts/perf_test.py / verify_e2e.py)
# --------------------------------------------------------------------------- #
class CheckError(AssertionError):
    """Raised to fail the load gate with a clear, single-line message."""


def check(cond: bool, msg: str) -> None:
    """Assert ``cond``; raise :class:`CheckError` with ``msg`` when it is falsy."""
    if not cond:
        raise CheckError(msg)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live)."""
    print(f"[load] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Percentile maths (nearest-rank; no numpy dependency)
# --------------------------------------------------------------------------- #
def _percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile of ``values`` (0 <= pct <= 100)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


# --------------------------------------------------------------------------- #
# Synthetic corpus this script seeds when the live corpus is too small.
#
# Several coherent incident families (each a distinct failure mode with varied
# phrasings + distinct resolutions) expanded with a numeric suffix so the corpus reaches
# LOAD_SEED_INCIDENTS while staying realistic (retrieval has clusters to discriminate,
# not N copies of one text). Deterministic (no RNG) so runs are comparable.
# --------------------------------------------------------------------------- #
_SERVICES = ["orders-api", "checkout", "payments", "search", "inventory", "gateway"]
_SEVERITIES = ["critical", "high", "medium", "low"]

_FAMILIES: list[dict[str, Any]] = [
    {
        "title": "Database connection pool exhausted under load",
        "description": (
            "Requests began failing with connection-timeout errors; the pool was fully "
            "checked out and new queries queued until they timed out."
        ),
        "tags": ["db", "connection-pool", "timeout"],
        "resolution": (
            "Raised the max pool size and added a statement timeout so slow queries "
            "release connections instead of pinning the pool."
        ),
    },
    {
        "title": "Kafka consumer lag growing unbounded",
        "description": (
            "Consumer lag climbed steadily as the producer rate outpaced the consumers, "
            "delaying downstream processing by minutes."
        ),
        "tags": ["kafka", "consumer-lag", "backpressure"],
        "resolution": (
            "Scaled out the consumer group and increased partitions so throughput "
            "matched the producer rate; lag drained."
        ),
    },
    {
        "title": "Service OOM-killed due to a memory leak",
        "description": (
            "Resident memory climbed steadily over several hours until the kernel "
            "OOM-killer terminated the process, then the cycle repeated."
        ),
        "tags": ["memory", "oom", "gc"],
        "resolution": (
            "Patched the leak (an unbounded cache) by adding LRU eviction and a TTL; "
            "steady-state memory flattened."
        ),
    },
    {
        "title": "Elevated 5xx after a bad deploy",
        "description": (
            "Error rate spiked immediately after a rollout as a new code path threw on "
            "an unhandled null, returning 500s to a fraction of traffic."
        ),
        "tags": ["deploy", "5xx", "regression"],
        "resolution": (
            "Rolled back to the previous release and added a null guard plus a "
            "regression test before re-deploying."
        ),
    },
    {
        "title": "Redis cache latency spike saturating clients",
        "description": (
            "Cache GET latency jumped and client connection pools filled up waiting on "
            "slow Redis responses, cascading timeouts upstream."
        ),
        "tags": ["redis", "cache", "latency"],
        "resolution": (
            "Enabled client-side timeouts, added a local fallback, and moved a hot key "
            "off the single shard that was hot-spotting."
        ),
    },
    {
        "title": "TLS certificate expired on the edge proxy",
        "description": (
            "Clients began rejecting connections with certificate-expired errors the "
            "instant the leaf certificate passed its notAfter date."
        ),
        "tags": ["tls", "cert", "expiry"],
        "resolution": (
            "Renewed and rotated the certificate, then fixed the cron that was supposed "
            "to auto-renew it."
        ),
    },
    {
        "title": "Disk full on the log collector",
        "description": (
            "The data volume filled up and writes started failing with 'no space left "
            "on device', taking the service down."
        ),
        "tags": ["disk", "storage", "full"],
        "resolution": (
            "Expanded the volume and enabled log rotation with a size cap so it cannot "
            "fill the disk again."
        ),
    },
    {
        "title": "Upstream dependency timeouts causing request pile-up",
        "description": (
            "A slow downstream API caused requests to queue and threads to block, "
            "exhausting the worker pool and stalling unrelated endpoints."
        ),
        "tags": ["timeout", "dependency", "thread-pool"],
        "resolution": (
            "Added a per-call timeout and a circuit breaker so a slow dependency sheds "
            "load instead of exhausting the workers."
        ),
    },
]


def _build_seed_incidents(target: int) -> list[dict[str, Any]]:
    """Return ``target`` distinct incident bodies by expanding the family templates.

    Each family is cloned across services / severities with a numeric suffix so the
    bodies stay distinct (distinct text -> distinct embeddings) while remaining a
    realistic clustered corpus. Deterministic (no RNG) so runs are comparable.
    """
    out: list[dict[str, Any]] = []
    i = 0
    while len(out) < target:
        fam = _FAMILIES[i % len(_FAMILIES)]
        variant = i // len(_FAMILIES)
        service = _SERVICES[i % len(_SERVICES)]
        severity = _SEVERITIES[i % len(_SEVERITIES)]
        out.append(
            {
                "title": f"{fam['title']} (incident #{i + 1})",
                "description": (
                    f"{fam['description']} Observed on {service} "
                    f"(occurrence {variant + 1})."
                ),
                "service": service,
                "severity": severity,
                "tags": fam["tags"],
                "resolution": fam["resolution"],
            }
        )
        i += 1
    return out


# --------------------------------------------------------------------------- #
# Varied load queries — a rotation of realistic descriptions, each made a distinct
# cache key by interpolating the request index so the concurrent batch exercises the
# full embed + K-NN + blend path (not a Redis cache hit).
# --------------------------------------------------------------------------- #
_QUERY_TEMPLATES: list[dict[str, Any]] = [
    {
        "title": "DB pool timeouts — clients cannot get a database connection",
        "description": (
            "Our service is intermittently failing because the database connection pool "
            "runs out; callers block waiting for a connection and then time out."
        ),
        "service": "orders-api",
        "severity": "high",
        "tags": ["db", "connection-pool", "timeout"],
    },
    {
        "title": "Consumer group falling behind, lag keeps rising",
        "description": (
            "The event consumers cannot keep up with the producer and the backlog grows "
            "steadily, so downstream jobs are delayed by minutes."
        ),
        "service": "search",
        "severity": "medium",
        "tags": ["kafka", "consumer-lag", "backpressure"],
    },
    {
        "title": "Process keeps getting OOM-killed overnight",
        "description": (
            "Memory usage grows without bound until the container is OOM-killed and "
            "restarts, repeating on a cycle every few hours."
        ),
        "service": "inventory",
        "severity": "high",
        "tags": ["memory", "oom", "leak"],
    },
    {
        "title": "Spike in 500s right after the latest release",
        "description": (
            "A fresh deploy started returning server errors on part of the traffic; the "
            "new code path throws on a missing field."
        ),
        "service": "checkout",
        "severity": "critical",
        "tags": ["deploy", "5xx", "regression"],
    },
    {
        "title": "Cache slowdown is stalling all requests",
        "description": (
            "Redis responses got slow and every request that reads the cache is backing "
            "up, filling the client connection pool and timing out."
        ),
        "service": "payments",
        "severity": "high",
        "tags": ["redis", "cache", "latency"],
    },
    {
        "title": "Edge proxy rejecting clients after cert rollover",
        "description": (
            "TLS handshakes are failing with an expired-certificate error and clients "
            "cannot connect through the edge proxy at all."
        ),
        "service": "gateway",
        "severity": "critical",
        "tags": ["tls", "cert", "expiry"],
    },
    {
        "title": "Writes failing, volume looks full",
        "description": (
            "The service is throwing no-space-left-on-device and cannot persist anything "
            "because the disk on the collector filled up."
        ),
        "service": "orders-api",
        "severity": "high",
        "tags": ["disk", "storage", "full"],
    },
    {
        "title": "Slow downstream is piling up our requests",
        "description": (
            "A dependency got slow and now our worker threads are all blocked waiting on "
            "it, so even unrelated endpoints are stalling."
        ),
        "service": "search",
        "severity": "medium",
        "tags": ["timeout", "dependency", "thread-pool"],
    },
]


def _load_query(i: int) -> dict[str, Any]:
    """Build the i-th load query — a rotation template made unique by ``i``.

    The description gets a per-request marker so no two fired requests share a cache key
    (an identical body would be served from the Redis recommendation cache and skip the
    compute path we are loading).
    """
    tpl = _QUERY_TEMPLATES[i % len(_QUERY_TEMPLATES)]
    q = dict(tpl)
    q["description"] = f"{tpl['description']} (load probe {i})"
    return q


def _ingest_incident(i: int) -> dict[str, Any]:
    """Build the i-th ingest body — a unique, embeddable resolved incident.

    Distinct text (per-request marker) so each ingest computes a real embedding rather
    than colliding, giving a meaningful incidents/s figure for the ingest pass.
    """
    fam = _FAMILIES[i % len(_FAMILIES)]
    service = _SERVICES[i % len(_SERVICES)]
    severity = _SEVERITIES[i % len(_SEVERITIES)]
    return {
        "title": f"{fam['title']} (load ingest {i})",
        "description": f"{fam['description']} (load ingest probe {i} on {service})",
        "service": service,
        "severity": severity,
        "tags": fam["tags"],
        "resolution": fam["resolution"],
    }


# --------------------------------------------------------------------------- #
# Memory (best-effort, reported — NEVER fails)
# --------------------------------------------------------------------------- #
#: Prometheus gauge names that would carry a *server-side* resident-memory figure if the
#: app registered a ProcessCollector. Scraped best-effort from /metrics; none present in
#: this app today (no ProcessCollector), so we fall back to the client's own RSS.
_MEM_METRIC_NAMES = (
    "process_resident_memory_bytes",
    "process_resident_memory_mb",
)


def _scrape_server_rss_mb(client: httpx.Client) -> float | None:
    """Best-effort: parse a resident-memory gauge (MB) from GET /metrics.

    Scans the Prometheus text exposition for one of :data:`_MEM_METRIC_NAMES` and, if
    found, returns it in MB (``*_bytes`` normalised /1MiB, ``*_mb`` used as-is). Returns
    ``None`` if /metrics is unreachable or exposes no such gauge (the case for this app
    today). NEVER raises.
    """
    try:
        resp = client.get("/metrics", timeout=10.0)
        if resp.status_code != 200:
            return None
        for raw in resp.text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            for name in _MEM_METRIC_NAMES:
                # A sample line is "<name> <value>" (this app's gauges are unlabelled).
                if line.startswith(name + " ") or line.startswith(name + "{"):
                    try:
                        value = float(line.rsplit(" ", 1)[1])
                    except (ValueError, IndexError):
                        continue
                    return value / (1024.0 * 1024.0) if name.endswith("_bytes") else value
    except Exception:  # noqa: BLE001 - memory is best-effort; never raise
        return None
    return None


def _client_rss_mb() -> float:
    """This loadtest process's own peak RSS in MB (fallback when no server gauge).

    ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` is in **bytes on macOS** but
    **kilobytes on Linux** — the Docker runtime this runs in is Linux, so kB is the
    common case; the value is normalised accordingly. Best-effort; returns 0.0 on any
    error (never raises).
    """
    try:
        ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Heuristic: Linux reports kB (values ~10^4–10^6 for a Python proc); macOS
        # reports bytes (values ~10^7–10^8). Treat by platform, not magnitude.
        if sys.platform == "darwin":
            return ru / (1024.0 * 1024.0)  # bytes -> MB
        return ru / 1024.0  # kB -> MB (Linux / Docker)
    except Exception:  # noqa: BLE001
        return 0.0


# --------------------------------------------------------------------------- #
# Synchronous setup steps (health / config / corpus / warm-up), reused from the
# perf-test style. These run once, before the concurrent phases.
# --------------------------------------------------------------------------- #
def wait_for_health(client: httpx.Client) -> None:
    """Poll ``GET /health`` until it returns HTTP 200, within the timeout."""
    deadline = time.time() + READY_TIMEOUT
    last = "no response"
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            resp = client.get("/health", timeout=5.0)
            if resp.status_code == 200:
                body = resp.json()
                info(
                    f"health ready after {attempt} attempt(s): "
                    f"status={body.get('status')} corpus_size={body.get('corpus_size')}"
                )
                return
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 - service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    raise CheckError(f"/health not ready after {READY_TIMEOUT:.0f}s (last: {last})")


def force_no_exploration(client: httpx.Client) -> None:
    """PUT /config {"epsilon_explore": 0} so load reflects the deterministic path.

    Best-effort: a non-200 is logged but does not fail the load gate. It removes the
    stochastic exploration branch (so every request walks the same compute path) and
    bumps the config version so the cache starts clean.
    """
    try:
        resp = client.put("/config", json={"epsilon_explore": 0}, timeout=15.0)
        if resp.status_code == 200:
            ver = resp.json().get("version")
            info(f"determinism: epsilon_explore=0 applied (config version={ver})")
        else:
            info(f"note: PUT /config epsilon_explore=0 -> {resp.status_code} (continuing)")
    except Exception as exc:  # noqa: BLE001 - non-fatal for a load run
        info(f"note: PUT /config failed ({type(exc).__name__}); continuing")


def corpus_size(client: httpx.Client) -> int:
    """Return ``GET /stats.corpus_size``."""
    resp = client.get("/stats", timeout=15.0)
    check(resp.status_code == 200, f"GET /stats -> {resp.status_code}: {resp.text[:200]}")
    return int(resp.json().get("corpus_size", 0))


def embedded_count(client: httpx.Client) -> int:
    """Return ``GET /stats.embedded_count`` (searchable incidents)."""
    resp = client.get("/stats", timeout=15.0)
    check(resp.status_code == 200, f"GET /stats -> {resp.status_code}: {resp.text[:200]}")
    return int(resp.json().get("embedded_count", 0))


def seed_incident(client: httpx.Client, inc: dict[str, Any]) -> None:
    """POST one incident; assert 201 + has_embedding=true (else it is unsearchable)."""
    resp = client.post("/incidents", json=inc, timeout=30.0)
    check(
        resp.status_code == 201,
        f"POST /incidents {inc['title']!r} -> {resp.status_code}: {resp.text[:200]}",
    )
    check(
        bool(resp.json().get("has_embedding")),
        f"incident {inc['title']!r} persisted without an embedding — not retrievable",
    )


def ensure_corpus(client: httpx.Client) -> int:
    """Ensure the live corpus has >= SEED_INCIDENTS embedded incidents; seed the gap."""
    have = corpus_size(client)
    info(f"corpus_size before seeding = {have} (target >= {SEED_INCIDENTS})")
    if have >= SEED_INCIDENTS:
        emb = embedded_count(client)
        info(f"corpus already sufficient (embedded={emb}); skipping seed")
        return have

    to_add = SEED_INCIDENTS - have
    incidents = _build_seed_incidents(to_add)
    info(f"seeding {to_add} incidents to reach {SEED_INCIDENTS}...")
    for n, inc in enumerate(incidents, start=1):
        seed_incident(client, inc)
        if n % 25 == 0 or n == to_add:
            info(f"  seeded {n}/{to_add}")

    final = corpus_size(client)
    emb = embedded_count(client)
    check(
        final >= SEED_INCIDENTS,
        f"corpus_size {final} still < target {SEED_INCIDENTS} after seeding",
    )
    check(
        emb >= SEED_INCIDENTS,
        f"embedded_count {emb} < target {SEED_INCIDENTS} — some incidents unsearchable",
    )
    info(f"corpus ready: corpus_size={final} embedded_count={emb}")
    return final


def warm_up(client: httpx.Client) -> None:
    """Fire 2 throwaway recommends (discarded). The first loads the MiniLM model.

    Done synchronously *before* the concurrent batch so the ~90 MB model-load cost is not
    paid inside the timed load (it would otherwise inflate the wall-clock and crater RPS).
    """
    info("warming up (2 discarded recommends; the first loads the MiniLM model)...")
    for i in range(2):
        start = time.perf_counter()
        resp = client.post("/recommend", json=_load_query(10_000 + i), timeout=REQUEST_TIMEOUT)
        ms = (time.perf_counter() - start) * 1000.0
        check(
            resp.status_code == 200,
            f"warm-up POST /recommend -> {resp.status_code}: {resp.text[:200]}",
        )
        info(f"  warmup {i + 1}/2: {ms:.0f}ms (discarded)")


# --------------------------------------------------------------------------- #
# Concurrent phases (asyncio + httpx.AsyncClient, bounded by a semaphore)
# --------------------------------------------------------------------------- #
async def _ingest_load() -> dict[str, Any]:
    """Fire INGEST_N concurrent POST /incidents; measure incidents/s + errors (lenient).

    A nice-to-have throughput figure for the *write* path. Reported only — it does not
    gate. Returns dict with completed / errors / wall / ips (incidents per second).
    """
    if INGEST_N <= 0:
        return {"requests": 0, "completed": 0, "errors": 0, "wall_s": 0.0, "ips": 0.0}

    sem = asyncio.Semaphore(CONCURRENCY)
    completed = 0
    errors = 0

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as client:

        async def one(i: int) -> None:
            nonlocal completed, errors
            async with sem:
                try:
                    resp = await client.post("/incidents", json=_ingest_incident(i))
                    if resp.status_code == 201:
                        completed += 1
                    else:
                        errors += 1
                except Exception:  # noqa: BLE001 - any failure counts as an ingest error
                    errors += 1

        wall_start = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(INGEST_N)))
        wall = time.perf_counter() - wall_start

    return {
        "requests": INGEST_N,
        "completed": completed,
        "errors": errors,
        "wall_s": wall,
        "ips": (completed / wall) if wall > 0 else 0.0,
    }


async def _recommend_load() -> dict[str, Any]:
    """Fire REQUESTS concurrent POST /recommend; measure RPS, error rate, latency, cache.

    Each request records its own wall-clock latency and ok/err (a non-2xx status OR an
    exception is an error). The whole-batch wall-clock gives ``RPS = completed / elapsed``
    — the true achieved concurrent throughput.
    """
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies: list[float] = []
    errors = 0
    cache_hits = 0

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=REQUEST_TIMEOUT) as client:

        async def one(i: int) -> None:
            nonlocal errors, cache_hits
            async with sem:
                start = time.perf_counter()
                try:
                    resp = await client.post("/recommend", json=_load_query(i))
                    elapsed = (time.perf_counter() - start) * 1000.0
                    latencies.append(elapsed)
                    if resp.status_code // 100 != 2:
                        errors += 1
                        return
                    body = resp.json()
                    # A 2xx with no suggestions means retrieval found nothing — that is a
                    # functional failure under load (the corpus was seeded), so count it.
                    if int(body.get("count", 0)) <= 0 or not body.get("suggestions"):
                        errors += 1
                        return
                    if body.get("cached"):
                        cache_hits += 1
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
        "cache_hits": cache_hits,
        "wall_s": wall,
        "rps": (completed / wall) if wall > 0 else 0.0,
        "error_rate": (errors / REQUESTS) if REQUESTS else 0.0,
        "p50": _percentile(latencies, 50),
        "p95": _percentile(latencies, 95),
        "max": max(latencies) if latencies else 0.0,
    }


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== Concurrent load test against {BASE_URL} ==")
    info(
        f"config: requests={REQUESTS} concurrency={CONCURRENCY} ingest_n={INGEST_N} "
        f"seed_target={SEED_INCIDENTS}"
    )
    info(
        f"HARD GATES: rps >= {MIN_RPS:.0f}  AND  error_rate <= {MAX_ERROR_RATE}  "
        f"(memory reported only, lenient ceiling {MAX_RSS_MB:.0f}MB)"
    )

    # --- synchronous setup: health, determinism, corpus, warm-up, memory baseline ---
    with httpx.Client(base_url=BASE_URL) as client:
        wait_for_health(client)
        force_no_exploration(client)
        final_corpus = ensure_corpus(client)
        warm_up(client)
        server_rss_before = _scrape_server_rss_mb(client)

    # --- concurrent ingest throughput (reported / lenient) ---
    info(f"ingest pass: firing {INGEST_N} concurrent POST /incidents (conc {CONCURRENCY})...")
    ingest = asyncio.run(_ingest_load())
    info(
        f"ingest done: {ingest['completed']}/{ingest['requests']} in "
        f"{ingest['wall_s']:.2f}s -> {ingest['ips']:.1f} incidents/s "
        f"(errors={ingest['errors']})"
    )

    # --- concurrent recommend load (the hard-gated phase) ---
    info(f"recommend load: firing {REQUESTS} concurrent POST /recommend (conc {CONCURRENCY})...")
    rec = asyncio.run(_recommend_load())

    # --- memory (best-effort, reported): server gauge if present, else client RSS ---
    with httpx.Client(base_url=BASE_URL) as client:
        server_rss_after = _scrape_server_rss_mb(client)
    if server_rss_after is not None:
        mem_mb = server_rss_after
        mem_source = "server /metrics resident-memory gauge"
    else:
        mem_mb = _client_rss_mb()
        mem_source = "loadtest CLIENT RSS (getrusage; no server gauge exposed)"

    cache_hit_rate = (rec["cache_hits"] / rec["completed"] * 100.0) if rec["completed"] else 0.0

    # --------------------------------------------------------------------- #
    # Report
    # --------------------------------------------------------------------- #
    print("", flush=True)
    print("=" * 72, flush=True)
    print("Concurrent load (POST /recommend under a semaphore of "
          f"{CONCURRENCY})", flush=True)
    print("-" * 72, flush=True)
    print(f"  corpus_size        : {final_corpus}", flush=True)
    print(f"  ingest throughput  : {ingest['ips']:.1f} incidents/s "
          f"({ingest['completed']}/{ingest['requests']}, errors={ingest['errors']})  "
          f"[reported]", flush=True)
    print("-" * 72, flush=True)
    print(f"  recommend requests : {rec['completed']}/{rec['requests']}  "
          f"(errors={rec['errors']})", flush=True)
    print(f"  wall-clock         : {rec['wall_s']:.2f} s", flush=True)
    print(f"  THROUGHPUT (RPS)   : {rec['rps']:.1f} req/s", flush=True)
    print(f"  error_rate         : {rec['error_rate']:.3f}", flush=True)
    print(f"  cache hits         : {rec['cache_hits']}/{rec['completed']} "
          f"({cache_hit_rate:.0f}%  — lower = more of the full compute path loaded)", flush=True)
    print(f"  latency p50        : {rec['p50']:.1f} ms", flush=True)
    print(f"  latency p95        : {rec['p95']:.1f} ms", flush=True)
    print(f"  latency max        : {rec['max']:.1f} ms", flush=True)
    print(f"  memory (RSS)       : {mem_mb:.1f} MB  [{mem_source}]  [reported]", flush=True)
    if server_rss_before is not None:
        print(f"                       (server before load: {server_rss_before:.1f} MB)", flush=True)
    print("-" * 72, flush=True)

    # --------------------------------------------------------------------- #
    # Gates
    # --------------------------------------------------------------------- #
    rps_pass = rec["rps"] >= MIN_RPS
    err_pass = rec["error_rate"] <= MAX_ERROR_RATE

    print(
        f"  GATE  throughput {rec['rps']:.1f} rps {'>=' if rps_pass else '<'} "
        f"floor {MIN_RPS:.0f}  ->  {'PASS' if rps_pass else 'FAIL'}",
        flush=True,
    )
    print(
        f"  GATE  error_rate {rec['error_rate']:.3f} {'<=' if err_pass else '>'} "
        f"max {MAX_ERROR_RATE}  ->  {'PASS' if err_pass else 'FAIL'}",
        flush=True,
    )
    # Memory: reported only — a breach of the lenient ceiling is noted, NEVER gated.
    if mem_mb > MAX_RSS_MB:
        print(
            f"  NOTE  memory {mem_mb:.1f}MB exceeds lenient ceiling {MAX_RSS_MB:.0f}MB "
            f"(reported only; does NOT fail the load test)",
            flush=True,
        )
    else:
        print(
            f"  REPORT memory {mem_mb:.1f}MB within lenient ceiling {MAX_RSS_MB:.0f}MB "
            f"(not a gate)",
            flush=True,
        )
    print("=" * 72, flush=True)

    # Hard gates (memory intentionally excluded).
    check(
        rps_pass,
        f"throughput {rec['rps']:.1f} rps below floor {MIN_RPS:.0f} "
        f"(completed {rec['completed']}/{rec['requests']} in {rec['wall_s']:.2f}s, "
        f"concurrency {CONCURRENCY})",
    )
    check(
        err_pass,
        f"error_rate {rec['error_rate']:.3f} exceeds max {MAX_ERROR_RATE} "
        f"({rec['errors']} errors over {rec['requests']} requests)",
    )

    print("", flush=True)
    print("LOAD PASSED ✅", flush=True)
    print(
        f"  throughput {rec['rps']:.1f} rps (>= {MIN_RPS:.0f}), "
        f"error_rate {rec['error_rate']:.3f} (<= {MAX_ERROR_RATE}); "
        f"latency p50 {rec['p50']:.1f}ms / p95 {rec['p95']:.1f}ms / max {rec['max']:.1f}ms; "
        f"memory {mem_mb:.1f}MB [reported]",
        flush=True,
    )


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print("", flush=True)
        print("!" * 72, file=sys.stderr, flush=True)
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
        print("LOAD FAILED ❌", file=sys.stderr, flush=True)
        print("!" * 72, file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - any unexpected error is a hard failure
        print("", flush=True)
        print("!" * 72, file=sys.stderr, flush=True)
        print(f"FAIL: unexpected {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        print("LOAD FAILED ❌", file=sys.stderr, flush=True)
        print("!" * 72, file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
