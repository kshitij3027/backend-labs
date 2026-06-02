"""Containerized load test proving cache speedup and high throughput (C20).

Run by the compose ``loadtest`` profile service (``Dockerfile.test``), this
script drives the live ``app`` container over HTTP — reaching it by **service
name** (``http://app:8000`` via ``APP_URL``), never ``localhost`` — with a
realistic, *Zipfian* query mix and asserts the §5 performance criteria:

* **Throughput** ``> 100 req/s`` (``--min-throughput``).
* **Cached p90** ``< 100 ms`` (``--max-cached-p90-ms``) — i.e. L1/L2 hits are fast.
* **Error rate** ``<= 1%`` (``--max-error-rate``).

It also reports the **speedup** = ``uncached_p50 / cached_p50`` (the §4 stretch
goal is ~10×; with ``BACKEND_DELAY_MS=150`` the uncached path runs a real
``GROUP BY`` over the seeded corpus while L1 hits return in well under a
millisecond, so a large multiple is expected). For the speedup to be *real* the
measured phase must contain both populations — a hot set of cache hits **and** a
sustained stream of genuinely uncached (backend) requests.

Design
------
* **Workset.** A fixed set of ``--workset`` *distinct* queries is built from the
  cartesian product of the supported query types × seed sources × a handful of
  distinct 300-second-spaced time buckets. Distinct buckets (and sources/types)
  produce distinct cache keys, so the workset is the population of "hot" keys.
* **Zipf selection.** Each (warm) request picks a workset entry with weight
  ``1/(rank+1)`` so a small head of the distribution absorbs the bulk of the
  traffic — a realistic hot set that drives a high cache-hit rate. ``random`` is
  seeded for reproducibility.
* **Warm phase.** Every workset query is fired once (bounded concurrency) so the
  hot set populates L1/L2; these requests are excluded from the measured metrics.
* **Measured phase — hits + a cold stream.** ``--concurrency`` async workers
  hammer the app for ``--duration`` seconds. In each iteration a worker rolls a
  die: with probability ``--cold-fraction`` it issues a **guaranteed-cold**
  request (a unique ``[start, end)`` window drawn from a process-wide monotonic
  nonce, so its 300-s-bucketed cache key has never been seen and the request
  *must* hit the slow backend); otherwise it replays a warmed Zipf hot entry
  (a cache hit). This yields a steady, realistic ~``1 - cold_fraction`` hit rate
  with a continuous supply of uncached samples. Each request's wall latency
  (``time.perf_counter``) and serving tier (``meta.tier``) are recorded and
  classified as a cache *hit* (tier ∈ ``l1``/``l2``/``l3``) or *miss*
  (``backend``). Non-200 responses count as errors.

The script prints a clear summary block and exits ``0`` with ``LOAD TEST: PASS``
when every gate is met (throughput, cached p90, error rate, hit rate, and the
presence of *both* cached and uncached samples), otherwise ``sys.exit(1)``.
"""
from __future__ import annotations

import argparse
import asyncio
import itertools
import os
import random
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import httpx

# --- domain constants (mirror src/backend.SUPPORTED_QUERIES + db/seed.SOURCES) -
# Kept as literals (rather than importing src.*) so the load generator stands
# alone and needs no app object-graph imports, but they intentionally match the
# server's accepted values so every request resolves a real aggregation.
SUPPORTED_QUERIES: list[str] = [
    "requests_over_time",
    "error_rate",
    "avg_latency",
    "top_sources",
]
SOURCES: list[str] = ["api", "web", "db", "auth", "worker"]

# Cache-hit tiers vs the slow source. ``meta.tier`` is one of these.
HIT_TIERS: frozenset[str] = frozenset({"l1", "l2", "l3"})

# The seed window: db-init seeds ``raw_logs`` over roughly the last 7 days
# (``end_ts = time.time()``). We bracket *recent* time so the windows actually
# scan rows (a genuine GROUP BY cost), making the cached-vs-uncached speedup
# real rather than empty-result fast.
_SEED_SPAN_SECONDS = 7 * 24 * 3600

# Server-side timestamp bucket (mirrors settings.time_bucket_seconds). The cache
# canonicalizer floors ``start``/``end`` to this many seconds, so two windows
# collide onto one key iff they share a (start-bucket, end-bucket) pair. We use
# this to give every cold request its own pair and therefore its own never-seen
# key.
_BUCKET_SECONDS = 300

# Param key carrying a per-request nonce. It is **not** a timestamp key and is
# **not** read by any backend handler (which only touch source/start/end/bucket/
# limit), so it is inert server-side — yet the cache canonicalizer serializes it
# verbatim, so a distinct nonce yields a distinct canonical string and therefore
# a brand-new cache key. This is what makes every cold request a guaranteed miss.
_COLD_NONCE_KEY = "_lt_nonce"

# Cold windows are confined to ``[oldest, now - _COLD_TOP_MARGIN_SECONDS]`` so
# they never reach the *recent* zone the hot workset occupies (which spans only
# the last ~15 min). One day of head-room leaves ~6 days of seeded buckets to
# rotate cold windows across (purely so backend scans aren't byte-identical;
# uniqueness itself comes from the nonce param, not the window).
_COLD_TOP_MARGIN_SECONDS = 24 * 3600
# How many distinct in-span buckets the cold windows rotate over.
_COLD_WINDOW_SLOTS = 512

# Reproducible request stream.
_RANDOM_SEED = 1337


def pct(sorted_values: list[float], p: float) -> float:
    """Return the ``p``-th percentile of an already-sorted list (nearest-rank).

    ``p`` is a fraction in ``[0, 1]`` (e.g. ``0.9`` for p90). Returns ``0.0`` for
    an empty list. Uses the nearest-rank method: ``ceil(p * N)`` clamped into
    ``[1, N]`` as a 1-based index.
    """
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if p <= 0:
        return sorted_values[0]
    if p >= 1:
        return sorted_values[-1]
    import math

    rank = math.ceil(p * n)
    rank = max(1, min(rank, n))
    return sorted_values[rank - 1]


def build_workset(size: int, *, bucket_seconds: int = 300, now: float | None = None) -> list[dict[str, Any]]:
    """Build ``size`` distinct query bodies spanning types × sources × buckets.

    Each entry is a ready-to-POST body ``{"query": qt, "params": {...}}`` with a
    half-open ``[start, end)`` window aligned to a distinct ``bucket_seconds``
    bucket (so windows never collide into the same cache key). We iterate buckets
    outermost and walk (query_type × source) within each bucket, which spreads
    the workset across all four query shapes and all five sources while keeping
    every entry's key distinct.

    The windows are placed at increasing offsets *back* from ``now`` so they fall
    inside the freshly-seeded ~7-day corpus and therefore scan real rows.
    """
    if now is None:
        now = time.time()

    combos = [(qt, src) for qt in SUPPORTED_QUERIES for src in SOURCES]
    # Each window is one bucket wide; space distinct windows by 2 buckets so the
    # canonicalizer (which floors timestamps to a bucket) keeps them distinct.
    window_width = bucket_seconds
    spacing = bucket_seconds * 2

    workset: list[dict[str, Any]] = []
    bucket_index = 0
    while len(workset) < size:
        # Offset this bucket's window back from "now" but stay within the corpus.
        offset = (bucket_index * spacing) % _SEED_SPAN_SECONDS
        end = now - offset
        start = end - window_width
        for qt, src in combos:
            if len(workset) >= size:
                break
            params: dict[str, Any] = {
                "source": src,
                "start": int(start),
                "end": int(end),
            }
            # top_sources ignores `source` server-side, but including it still
            # yields a distinct canonical key, so it remains a valid hot entry.
            workset.append({"query": qt, "params": params})
        bucket_index += 1

    return workset[:size]


def zipf_weights(n: int) -> list[float]:
    """Return ``1/(rank+1)`` weights for ``n`` ranks (rank 0 is the hottest).

    A small head absorbs most traffic: rank 0 gets weight 1.0, rank 1 gets 0.5,
    and so on. The list is returned unnormalized — :func:`random.choices` accepts
    relative weights directly.
    """
    return [1.0 / (rank + 1) for rank in range(n)]


def cold_query(
    nonce: int,
    *,
    bucket_seconds: int = _BUCKET_SECONDS,
    now: float | None = None,
) -> dict[str, Any]:
    """Build a query whose canonical cache key is **guaranteed never to repeat**.

    Uniqueness is carried by a per-request ``nonce`` param (:data:`_COLD_NONCE_KEY`):
    the cache canonicalizer serializes every non-timestamp param verbatim, so two
    requests with different nonces *cannot* share a key — and no backend handler
    reads that param, so it is inert server-side. Distinct nonce -> distinct key,
    for the entire (unbounded) nonce stream, with no wrap-around bookkeeping.

    The ``[start, end)`` window is rotated across :data:`_COLD_WINDOW_SLOTS`
    distinct buckets inside the cold region ``[oldest, now - _COLD_TOP_MARGIN_SECONDS]``
    — fully seeded, yet a day clear of the recent zone the hot workset occupies.
    Rotating the window (rather than reusing one) just keeps the backend's
    ``GROUP BY`` from scanning byte-identical rows every time; the window is *not*
    what makes the key unique (the nonce is), so it can never collide with the hot
    set. Every window lies inside the ~7-day corpus, so each cold request runs a
    genuine aggregation over real rows — a true backend cost, not an empty-result
    fast path. The query type and source also rotate deterministically with the
    nonce so all four aggregations are exercised.
    """
    if now is None:
        now = time.time()

    # Bucket-aligned bounds of the cold region; rotate the window across a fixed
    # set of in-span buckets (purely for scan variety — uniqueness is the nonce).
    base_bucket = (int(now - _SEED_SPAN_SECONDS) // bucket_seconds) * bucket_seconds
    slot = nonce % _COLD_WINDOW_SLOTS
    start = base_bucket + slot * bucket_seconds
    end = start + bucket_seconds

    qt = SUPPORTED_QUERIES[nonce % len(SUPPORTED_QUERIES)]
    src = SOURCES[nonce % len(SOURCES)]
    return {
        "query": qt,
        "params": {
            "source": src,
            "start": int(start),
            "end": int(end),
            _COLD_NONCE_KEY: nonce,  # inert at backend; makes the cache key unique
        },
    }


@dataclass
class Results:
    """Accumulates per-request outcomes during the measured phase."""

    cached_latencies: list[float] = field(default_factory=list)
    uncached_latencies: list[float] = field(default_factory=list)
    errors: int = 0

    @property
    def total(self) -> int:
        return len(self.cached_latencies) + len(self.uncached_latencies) + self.errors

    @property
    def hits(self) -> int:
        return len(self.cached_latencies)

    def record(self, *, ok: bool, cached: bool, latency_ms: float) -> None:
        """Record one completed request."""
        if not ok:
            self.errors += 1
            return
        if cached:
            self.cached_latencies.append(latency_ms)
        else:
            self.uncached_latencies.append(latency_ms)


async def _post_query(client: httpx.AsyncClient, body: dict[str, Any]) -> tuple[bool, bool, float]:
    """POST a single ``/query``; return ``(ok, cached, latency_ms)``.

    ``ok`` is True only on HTTP 200. ``cached`` is True when ``meta.tier`` is a
    cache tier (l1/l2/l3). Latency is wall time around the request in ms. Any
    transport error (timeout, connection reset) is reported as ``ok=False``.
    """
    t0 = time.perf_counter()
    try:
        resp = await client.post("/query", json=body)
    except Exception:
        return (False, False, (time.perf_counter() - t0) * 1000.0)
    latency_ms = (time.perf_counter() - t0) * 1000.0
    if resp.status_code != 200:
        return (False, False, latency_ms)
    try:
        tier = resp.json().get("meta", {}).get("tier", "")
    except Exception:
        tier = ""
    return (True, tier in HIT_TIERS, latency_ms)


async def _wait_for_health(url: str, *, attempts: int = 60, delay: float = 1.0) -> bool:
    """Poll ``GET {url}/health`` until 200 (or attempts exhausted)."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        for _ in range(attempts):
            try:
                resp = await client.get(f"{url}/health")
                if resp.status_code == 200:
                    return True
            except Exception:
                pass
            await asyncio.sleep(delay)
    return False


async def _warm(client: httpx.AsyncClient, workset: list[dict[str, Any]], *, concurrency: int) -> None:
    """Fire every workset query once (bounded concurrency) to populate caches.

    Warm-phase outcomes are intentionally discarded — they only exist to move the
    hot set into L1/L2 so the measured phase reflects steady-state behaviour.
    """
    sem = asyncio.Semaphore(max(1, concurrency))

    async def _one(body: dict[str, Any]) -> None:
        async with sem:
            await _post_query(client, body)

    await asyncio.gather(*(_one(b) for b in workset))


async def _worker(
    client: httpx.AsyncClient,
    workset: list[dict[str, Any]],
    weights: list[float],
    deadline: float,
    rng: random.Random,
    results: Results,
    *,
    cold_fraction: float,
    cold_nonces: Iterator[int],
    now: float,
) -> None:
    """One measured-phase worker: roll cold-vs-hot, POST, record, repeat.

    Loops until ``time.perf_counter() >= deadline``. Each iteration draws a
    uniform ``[0, 1)`` sample from the per-worker ``rng`` (seeded off the master
    RNG, so the stream is reproducible): below ``cold_fraction`` it issues a
    guaranteed-cold request built from the *shared* monotonic ``cold_nonces``
    counter (every nonce -> a unique, never-cached key); otherwise it replays a
    warmed Zipf hot entry, which serves from cache.

    ``next(cold_nonces)`` is atomic under CPython's GIL and there is no ``await``
    between the draw and its use, so concurrent workers never collide on a nonce —
    each cold request keeps its own distinct cache key.
    """
    while time.perf_counter() < deadline:
        if rng.random() < cold_fraction:
            body = cold_query(next(cold_nonces), now=now)
        else:
            body = rng.choices(workset, weights=weights, k=1)[0]
        ok, cached, latency_ms = await _post_query(client, body)
        results.record(ok=ok, cached=cached, latency_ms=latency_ms)


async def run_load_test(args: argparse.Namespace) -> int:
    """Execute warm + measured phases, print the report, return an exit code."""
    print(f"Load test target: {args.url}")
    print(
        f"  duration={args.duration}s concurrency={args.concurrency} "
        f"workset={args.workset}"
    )

    if not await _wait_for_health(args.url):
        print(f"LOAD TEST FAILED: {args.url}/health never became ready")
        return 1

    random.seed(_RANDOM_SEED)
    # One shared epoch anchor: the hot workset is placed at *recent* time
    # (offsets back from ``now``) while cold windows live in an older corpus
    # region; combined with the per-request nonce param, the two key populations
    # are provably disjoint.
    now = time.time()
    workset = build_workset(args.workset, now=now)
    weights = zipf_weights(len(workset))
    print(f"  built {len(workset)} distinct hot queries (Zipfian 1/(rank+1))")
    print(
        f"  measured phase mixes ~{args.cold_fraction:.0%} guaranteed-cold "
        f"requests (unique time-bucketed keys) with warmed hot hits"
    )

    # A generous per-request timeout: the cold path runs a real GROUP BY over the
    # 200k-row corpus plus the artificial BACKEND_DELAY_MS.
    limits = httpx.Limits(
        max_connections=args.concurrency * 2,
        max_keepalive_connections=args.concurrency * 2,
    )
    async with httpx.AsyncClient(base_url=args.url, timeout=30.0, limits=limits) as client:
        # --- warm phase ------------------------------------------------------
        print("  warming caches (one request per workset entry)...")
        await _warm(client, workset, concurrency=args.concurrency)

        # --- measured phase --------------------------------------------------
        print(f"  running measured phase for {args.duration}s...")
        results = Results()
        master = random.Random(_RANDOM_SEED + 1)
        # Shared monotonic nonce source: every cold request consumes the next
        # integer, which the cache key embeds, guaranteeing a never-cached key.
        cold_nonces: Iterator[int] = itertools.count()
        start = time.perf_counter()
        deadline = start + args.duration
        workers = [
            _worker(
                client,
                workset,
                weights,
                deadline,
                random.Random(master.random()),
                results,
                cold_fraction=args.cold_fraction,
                cold_nonces=cold_nonces,
                now=now,
            )
            for _ in range(args.concurrency)
        ]
        await asyncio.gather(*workers)
        elapsed = time.perf_counter() - start

    # --- metrics -------------------------------------------------------------
    total = results.total
    throughput = total / elapsed if elapsed > 0 else 0.0
    hit_rate = results.hits / total if total else 0.0
    error_rate = results.errors / total if total else 0.0

    cached = sorted(results.cached_latencies)
    uncached = sorted(results.uncached_latencies)
    overall = sorted(results.cached_latencies + results.uncached_latencies)

    cached_p50 = pct(cached, 0.50)
    cached_p90 = pct(cached, 0.90)
    cached_p99 = pct(cached, 0.99)
    uncached_p50 = pct(uncached, 0.50)
    uncached_p90 = pct(uncached, 0.90)
    overall_p50 = pct(overall, 0.50)
    overall_p90 = pct(overall, 0.90)
    speedup = (uncached_p50 / cached_p50) if cached_p50 > 0 else 0.0

    # --- report --------------------------------------------------------------
    bar = "=" * 60
    print(f"\n{bar}\nLOAD TEST RESULTS\n{bar}")
    print(f"Duration:        {elapsed:.2f}s")
    print(f"Total requests:  {total}")
    print(f"Throughput:      {throughput:.1f} req/s   (gate > {args.min_throughput})")
    print(f"Hit rate:        {hit_rate:.1%}   ({results.hits}/{total})"
          f"   (gate >= {args.min_hit_rate:.0%})")
    print(f"Error rate:      {error_rate:.2%}   (gate <= {args.max_error_rate:.1%})")
    print(f"Cached   ({len(cached)}) p50/p90/p99: "
          f"{cached_p50:.2f} / {cached_p90:.2f} / {cached_p99:.2f} ms"
          f"   (p90 gate < {args.max_cached_p90_ms})")
    print(f"Uncached ({len(uncached)}) p50/p90:     "
          f"{uncached_p50:.2f} / {uncached_p90:.2f} ms")
    print(f"Overall  p50/p90:     {overall_p50:.2f} / {overall_p90:.2f} ms")
    print(f"Speedup (uncached_p50 / cached_p50): {speedup:.1f}x")
    print(bar)

    # --- gates ---------------------------------------------------------------
    failures: list[str] = []
    # Require BOTH populations so the reported speedup is real, not an artefact of
    # an empty cached/uncached list.
    if len(cached) == 0:
        failures.append("no cached requests recorded (warm phase ineffective)")
    if len(uncached) == 0:
        failures.append(
            "no uncached requests recorded (cold stream produced no backend hits)"
        )
    if throughput < args.min_throughput:
        failures.append(
            f"throughput {throughput:.1f} req/s < {args.min_throughput} req/s"
        )
    if cached_p90 >= args.max_cached_p90_ms:
        failures.append(
            f"cached p90 {cached_p90:.2f} ms >= {args.max_cached_p90_ms} ms"
        )
    if error_rate > args.max_error_rate:
        failures.append(
            f"error rate {error_rate:.2%} > {args.max_error_rate:.2%}"
        )
    if hit_rate < args.min_hit_rate:
        failures.append(
            f"hit rate {hit_rate:.1%} < {args.min_hit_rate:.1%}"
        )

    if failures:
        for f in failures:
            print(f"GATE FAILED: {f}")
        print("LOAD TEST: FAIL")
        return 1

    print("LOAD TEST: PASS")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Define CLI args, defaulting from env where sensible."""
    parser = argparse.ArgumentParser(description="Load test the multi-tier caching layer")
    parser.add_argument(
        "--url",
        default=os.environ.get("APP_URL", "http://app:8000"),
        help="Base URL of the app (default: env APP_URL or http://app:8000)",
    )
    parser.add_argument("--duration", type=int, default=12, help="Measured-phase seconds")
    parser.add_argument("--concurrency", type=int, default=20, help="Concurrent async workers")
    parser.add_argument("--workset", type=int, default=40, help="Number of distinct hot queries")
    parser.add_argument(
        "--cold-fraction",
        type=float,
        default=0.15,
        help="Fraction of measured requests that are guaranteed-cold (unique key)",
    )
    parser.add_argument(
        "--min-throughput", type=float, default=100.0, help="Min req/s gate"
    )
    parser.add_argument(
        "--max-cached-p90-ms", type=float, default=100.0, help="Max cached p90 (ms) gate"
    )
    parser.add_argument(
        "--max-error-rate", type=float, default=0.01, help="Max error fraction gate"
    )
    parser.add_argument(
        "--min-hit-rate", type=float, default=0.75, help="Min cache hit-rate gate"
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    sys.exit(asyncio.run(run_load_test(args)))


if __name__ == "__main__":
    main()
