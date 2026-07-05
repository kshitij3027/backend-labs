"""Recommend-path latency gate for the Log Recommendation Engine (C19).

Runs **inside Docker** (the profile-gated ``loadtest`` service) against the *live* API
over HTTP — no in-process imports of ``src``, no direct DB/Redis access. It measures the
end-to-end cost of the core promise of the system:

    POST /recommend  ==  embed the query (MiniLM)
                          -> pgvector K-NN retrieval
                          -> contextual scoring
                          -> weighted blend / rank
                          -> persist the served recommendation (+ cache write)

and turns that into p50 / p95 / max / mean, then **hard-gates the p95** under a
configurable ceiling so a latency regression fails ``make load`` loudly.

Why the warm-up matters
------------------------
The *first* ``/recommend`` after the api boots pays a one-off cost of several seconds to
load the ``all-MiniLM-L6-v2`` sentence-transformer into memory. Counting that in the
sample would wildly skew the tail, so the script fires ``PERF_WARMUP`` throwaway
recommends first and **discards** them; only the steady-state calls are measured.

Why the queries are varied
--------------------------
An identical repeated query is served from the Redis recommendation cache
(``cached=True``) and never touches the embed + K-NN + blend path we want to time. So the
measured loop rotates through a handful of realistic, *distinct* incident descriptions —
each a fresh cache key — so (almost) every sample exercises the full compute path. The
script reports the observed cache-hit rate so a surprise (everything cached) is visible.

The script is self-seeding: if the live corpus is small (``GET /stats.corpus_size`` <
``PERF_SEED_INCIDENTS``) it ingests a spread of incidents (several coherent families +
distractors) through ``POST /incidents`` until the corpus is large enough and fully
embedded, so retrieval has a realistic amount of data to search. On an already-seeded
stack (e.g. after ``make e2e`` or ``make seed``) it seeds nothing and just measures.

Configuration (all via env, with sensible defaults):

* ``PERF_BASE_URL``        base URL of the live API (default ``http://api:8000``).
* ``PERF_READY_TIMEOUT``   seconds to wait for ``/health`` to come up (default 90).
* ``PERF_SEED_INCIDENTS``  min corpus size before measuring; seed up to it (default 120).
* ``PERF_SAMPLES``         measured ``POST /recommend`` calls (default 40).
* ``PERF_WARMUP``          throwaway recommends before timing (default 3; #1 loads MiniLM).
* ``PERF_MAX_RECOMMEND_MS``**hard gate**: measured p95 must be <= this (default 2000).

Exit code: ``0`` with ``PERF PASSED ✅`` only when the p95 gate holds; non-zero with a
loud ``FAIL:`` line otherwise — so ``make load`` propagates the failure.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any

import httpx

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("PERF_BASE_URL", "http://api:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("PERF_READY_TIMEOUT", "90"))
SEED_INCIDENTS = int(os.environ.get("PERF_SEED_INCIDENTS", "120"))
SAMPLES = int(os.environ.get("PERF_SAMPLES", "40"))
WARMUP = int(os.environ.get("PERF_WARMUP", "3"))
MAX_RECOMMEND_MS = float(os.environ.get("PERF_MAX_RECOMMEND_MS", "2000"))


# --------------------------------------------------------------------------- #
# Assertion + logging helpers (mirrors scripts/verify_e2e.py)
# --------------------------------------------------------------------------- #
class CheckError(AssertionError):
    """Raised to fail the perf gate with a clear, single-line message."""


def check(cond: bool, msg: str) -> None:
    """Assert ``cond``; raise :class:`CheckError` with ``msg`` when it is falsy."""
    if not cond:
        raise CheckError(msg)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live)."""
    print(f"[perf] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Percentile / summary maths (nearest-rank; no numpy dependency)
# --------------------------------------------------------------------------- #
def _percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile of ``values`` (0 <= pct <= 100)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


def _summary(values: list[float]) -> dict[str, float]:
    """p50 / p95 / max / mean / min over ``values`` (ms)."""
    if not values:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "min": 0.0, "max": 0.0}
    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "p50": _percentile(values, 50),
        "p95": _percentile(values, 95),
        "min": min(values),
        "max": max(values),
    }


# --------------------------------------------------------------------------- #
# Synthetic corpus this script seeds when the live corpus is too small.
#
# Several coherent incident families (each a distinct failure mode with varied
# phrasings + distinct resolutions) plus one-off distractors. Families are expanded
# with a numeric suffix so the corpus reaches PERF_SEED_INCIDENTS while staying
# realistic (retrieval has clusters to discriminate, not N copies of one text).
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
# Varied measured queries — a rotation of realistic descriptions, each a distinct
# cache key so the measured loop exercises the full embed + K-NN + blend path
# (not a Redis cache hit). Interpolating the iteration index guarantees uniqueness
# even across a full rotation.
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


def _measured_query(i: int) -> dict[str, Any]:
    """Build the i-th measured query — a rotation template made unique by ``i``.

    The description gets a per-iteration marker so no two measured requests share a
    cache key (an identical body would be served from the Redis recommendation cache
    and skip the compute path we are timing).
    """
    tpl = _QUERY_TEMPLATES[i % len(_QUERY_TEMPLATES)]
    q = dict(tpl)
    q["description"] = f"{tpl['description']} (perf probe {i})"
    return q


# --------------------------------------------------------------------------- #
# Steps
# --------------------------------------------------------------------------- #
def wait_for_health(client: httpx.Client) -> None:
    """Poll ``GET /health`` until it returns HTTP 200, within the timeout.

    ``/health`` answers 200 while the process is alive; connection errors are tolerated
    while the container is still starting.
    """
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
    """PUT /config {"epsilon_explore": 0} so timing reflects the deterministic path.

    Not about ordering here (this is a perf run) — it just removes the stochastic
    exploration branch so every measured ``/recommend`` walks the same compute path,
    and it bumps the config version so the cache starts clean. Best-effort: a
    non-200 is logged but does not fail the perf gate.
    """
    try:
        resp = client.put("/config", json={"epsilon_explore": 0}, timeout=15.0)
        if resp.status_code == 200:
            ver = resp.json().get("version")
            info(f"determinism: epsilon_explore=0 applied (config version={ver})")
        else:
            info(f"note: PUT /config epsilon_explore=0 -> {resp.status_code} (continuing)")
    except Exception as exc:  # noqa: BLE001 - non-fatal for a perf run
        info(f"note: PUT /config failed ({type(exc).__name__}); continuing")


def corpus_size(client: httpx.Client) -> int:
    """Return ``GET /stats.corpus_size`` (embedded count is asserted separately)."""
    resp = client.get("/stats", timeout=15.0)
    check(resp.status_code == 200, f"GET /stats -> {resp.status_code}: {resp.text[:200]}")
    body = resp.json()
    return int(body.get("corpus_size", 0))


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
    body = resp.json()
    check(
        bool(body.get("has_embedding")),
        f"incident {inc['title']!r} persisted without an embedding "
        f"(has_embedding={body.get('has_embedding')}) — it would not be retrievable",
    )


def ensure_corpus(client: httpx.Client) -> int:
    """Ensure the live corpus has >= SEED_INCIDENTS embedded incidents; seed the gap.

    Returns the final corpus size. If the stack is already seeded (>= target) nothing
    is inserted. Otherwise the shortfall is filled from :func:`_build_seed_incidents`
    and we re-check the embedded count so we never measure against a half-embedded corpus.
    """
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


def _recommend_once(client: httpx.Client, query: dict[str, Any]) -> tuple[float, bool, int]:
    """POST /recommend one query; return (elapsed_ms, cached, count). Assert 200 + count>0.

    Wall-clock is measured with ``time.perf_counter`` around the single HTTP call, so it
    captures the whole server-side pipeline (embed + K-NN + contextual + blend + persist)
    plus network — the real user-perceived latency of a recommendation.
    """
    start = time.perf_counter()
    resp = client.post("/recommend", json=query, timeout=60.0)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    check(
        resp.status_code == 200,
        f"POST /recommend -> {resp.status_code}: {resp.text[:200]}",
    )
    body = resp.json()
    count = int(body.get("count", 0))
    check(
        count > 0 and bool(body.get("suggestions")),
        f"POST /recommend returned no suggestions (count={count}) — corpus not searchable?",
    )
    return elapsed_ms, bool(body.get("cached")), count


def warm_up(client: httpx.Client) -> None:
    """Fire WARMUP throwaway recommends (discarded). The first loads the MiniLM model."""
    if WARMUP <= 0:
        return
    info(f"warming up ({WARMUP} discarded recommend(s); the first loads the MiniLM model)...")
    for i in range(WARMUP):
        ms, _cached, _count = _recommend_once(client, _measured_query(i))
        info(f"  warmup {i + 1}/{WARMUP}: {ms:.0f}ms (discarded)")


def measure(client: httpx.Client) -> tuple[list[float], int]:
    """Time SAMPLES varied recommends; return (latencies_ms, cache_hits)."""
    latencies: list[float] = []
    cache_hits = 0
    info(f"measuring {SAMPLES} recommend(s) with rotating (mostly cache-miss) queries...")
    for i in range(SAMPLES):
        # Offset by WARMUP so the measured rotation does not reuse warm-up cache keys.
        ms, cached, _count = _recommend_once(client, _measured_query(WARMUP + i))
        latencies.append(ms)
        if cached:
            cache_hits += 1
    return latencies, cache_hits


# --------------------------------------------------------------------------- #
# The full flow
# --------------------------------------------------------------------------- #
def run() -> None:
    info(f"== Recommend-path perf test against {BASE_URL} ==")
    info(
        f"config: samples={SAMPLES} warmup={WARMUP} seed_target={SEED_INCIDENTS} "
        f"HARD GATE p95 <= {MAX_RECOMMEND_MS:.0f}ms"
    )
    with httpx.Client(base_url=BASE_URL) as client:
        # 1. Wait for the live API.
        wait_for_health(client)

        # 2. Remove the stochastic exploration branch + bump config version (clean cache).
        force_no_exploration(client)

        # 3. Ensure a realistically-sized, fully-embedded corpus.
        final_corpus = ensure_corpus(client)

        # 4. Warm up (discard) — the FIRST recommend loads the MiniLM model (~seconds).
        warm_up(client)

        # 5. Measure the steady-state recommend path with varied queries.
        latencies, cache_hits = measure(client)

    s = _summary(latencies)
    cache_hit_rate = (cache_hits / len(latencies) * 100.0) if latencies else 0.0

    # --- report ---
    print("", flush=True)
    print("=" * 68, flush=True)
    print("Recommend-path latency (POST /recommend: embed + K-NN + blend + persist)", flush=True)
    print("-" * 68, flush=True)
    print(f"  corpus_size     : {final_corpus}", flush=True)
    print(f"  samples         : {int(s['count'])}  (warmup {WARMUP} discarded)", flush=True)
    print(f"  cache hits      : {cache_hits}/{int(s['count'])} "
          f"({cache_hit_rate:.0f}%  — lower = more of the full compute path exercised)", flush=True)
    print(f"  p50             : {s['p50']:.1f} ms", flush=True)
    print(f"  p95             : {s['p95']:.1f} ms", flush=True)
    print(f"  max             : {s['max']:.1f} ms", flush=True)
    print(f"  mean            : {s['mean']:.1f} ms", flush=True)
    print(f"  min             : {s['min']:.1f} ms", flush=True)
    print("-" * 68, flush=True)

    # --- hard gate: p95 vs ceiling ---
    passed = s["p95"] <= MAX_RECOMMEND_MS
    print(
        f"  GATE  p95 {s['p95']:.1f}ms {'<=' if passed else '>'} "
        f"ceiling {MAX_RECOMMEND_MS:.0f}ms  ->  {'PASS' if passed else 'FAIL'}",
        flush=True,
    )
    print("=" * 68, flush=True)

    check(
        passed,
        f"recommend p95 {s['p95']:.1f}ms exceeds hard ceiling {MAX_RECOMMEND_MS:.0f}ms "
        f"(p50={s['p50']:.1f}ms max={s['max']:.1f}ms over {int(s['count'])} samples)",
    )

    print("", flush=True)
    print("PERF PASSED ✅", flush=True)
    print(
        f"  recommend p95 {s['p95']:.1f}ms within ceiling {MAX_RECOMMEND_MS:.0f}ms "
        f"(p50 {s['p50']:.1f}ms, max {s['max']:.1f}ms, n={int(s['count'])})",
        flush=True,
    )


def main() -> int:
    try:
        run()
    except CheckError as exc:
        print("", flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        print(f"FAIL: {exc}", file=sys.stderr, flush=True)
        print("PERF FAILED ❌", file=sys.stderr, flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - any unexpected error is a hard failure
        print("", flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        print(f"FAIL: unexpected {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        print("PERF FAILED ❌", file=sys.stderr, flush=True)
        print("!" * 68, file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
