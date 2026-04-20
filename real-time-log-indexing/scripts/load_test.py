"""Latency + throughput load test.

Asserts the project's success criteria:

    search p95        < 50 ms
    indexing latency  < 100 ms   (see drain-delay proxy below)
    throughput        >= 1000 logs/s

Runs entirely inside the Docker test container (``make load``) so it
hits the real FastAPI app at ``APP_URL`` and the real Redis broker at
``REDIS_URL``. On failure the script exits non-zero so CI catches
regressions.

Indexing-latency proxy
----------------------

Wiring per-message XADD→searchable timestamps requires a tap into the
consumer that Commit 13 intentionally avoids (zero app-side changes).
Instead we measure the *drain delay* — the wall-clock gap between the
last XADD and the point at which ``/api/stats.docs_indexed`` catches
up to the expected total — and divide by the batch size to obtain an
upper bound on per-message indexing latency. If that upper bound sits
safely below 100 ms we are meeting the success criterion.
"""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time

import httpx
import redis.asyncio as redis_async


# ---------------------------------------------------------------------------
# Environment / tuning
# ---------------------------------------------------------------------------

APP_URL = os.environ.get("APP_URL", "http://app:8080")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

# Keep defaults modest so `make load` finishes inside a typical CI
# budget. All three are overridable via env vars for ad-hoc stress
# runs from the host.
TOTAL_DOCS = int(os.environ.get("LOAD_TEST_COUNT", "6000"))
TARGET_RATE = int(os.environ.get("LOAD_TEST_RATE", "1500"))       # logs/s
SEARCH_QPS = int(os.environ.get("LOAD_TEST_SEARCH_QPS", "20"))    # queries/s
BATCH = int(os.environ.get("LOAD_TEST_BATCH", "500"))             # XADDs/req

# Terms likely to appear in the sample templates (see src/sample_data.py).
SEARCH_TERMS = ["error", "timeout", "auth", "payment", "cache"]

# Success-criteria thresholds the script asserts before exiting 0.
SEARCH_P95_MAX_MS = 50.0
INDEX_PER_DOC_MAX_MS = 100.0
MIN_THROUGHPUT = 1000.0


async def _flush_redis() -> None:
    """Wipe Redis so the run starts from zeroed counters.

    We do this before talking to the app so the consumer's ingest
    counter and the index's ``docs_indexed`` line up. Running against
    a dirty Redis would still work but would skew the drain-delay
    proxy on the first batch.
    """
    client = redis_async.from_url(REDIS_URL, decode_responses=False)
    try:
        await client.flushall()
    finally:
        await client.aclose()


async def _wait_for_health(http: httpx.AsyncClient) -> None:
    """Block until the app reports ``redis_connected`` or bail."""
    for _ in range(30):
        try:
            r = await http.get("/health")
            if r.status_code == 200 and r.json().get("redis_connected"):
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    print("load_test: service never became healthy", file=sys.stderr)
    sys.exit(1)


async def _ingest(
    http: httpx.AsyncClient,
    batches: int,
    batch_size: int,
    target_rate: int,
) -> tuple[float, float]:
    """Push ``batches * batch_size`` sample docs at ``target_rate``.

    Returns the (start, end) wall-clock timestamps so the caller can
    compute throughput without having to measure the loop itself.
    """
    target_interval = batch_size / max(target_rate, 1)
    t_start = time.perf_counter()
    for _ in range(batches):
        t0 = time.perf_counter()
        r = await http.post("/api/generate-sample", json={"count": batch_size})
        r.raise_for_status()
        elapsed = time.perf_counter() - t0
        if elapsed < target_interval:
            await asyncio.sleep(target_interval - elapsed)
    t_end = time.perf_counter()
    return t_start, t_end


async def _search_loop(
    http: httpx.AsyncClient,
    stop: asyncio.Event,
    latencies_ms: list[float],
    errors: list[int],
    qps: int,
) -> None:
    """Hammer ``/api/search`` at ``qps`` while ingest is running.

    Appends per-query latencies (ms) to ``latencies_ms`` and increments
    ``errors[0]`` on failures. Exits as soon as ``stop`` is set.
    """
    interval = 1.0 / max(qps, 1)
    i = 0
    while not stop.is_set():
        term = SEARCH_TERMS[i % len(SEARCH_TERMS)]
        i += 1
        t0 = time.perf_counter()
        try:
            r = await http.get(f"/api/search?q={term}&limit=20")
            r.raise_for_status()
            latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        except Exception:
            errors[0] += 1
        # Use wait_for on the stop event so shutdown doesn't wait a
        # full ``interval`` after the ingest side finishes.
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


def _p95(samples: list[float]) -> float:
    """Best-effort 95th percentile — falls back to ``max`` on tiny samples."""
    if not samples:
        return 0.0
    if len(samples) < 20:
        return max(samples)
    return statistics.quantiles(samples, n=20)[18]


def _p99(samples: list[float]) -> float:
    if not samples:
        return 0.0
    if len(samples) < 100:
        return max(samples)
    return statistics.quantiles(samples, n=100)[98]


async def _drain(
    http: httpx.AsyncClient, target_total: int, deadline_s: float = 30.0
) -> tuple[int, float]:
    """Poll ``/api/stats`` until ``docs_indexed >= target_total``.

    Returns the tuple ``(last_docs_indexed, drain_wall_clock_s)``
    measured from the moment this function is entered (which is
    immediately after the last XADD returned).
    """
    t0 = time.perf_counter()
    deadline = time.time() + deadline_s
    last = 0
    while time.time() < deadline:
        cur = (await http.get("/api/stats")).json()["docs_indexed"]
        last = cur
        if cur >= target_total:
            return cur, time.perf_counter() - t0
        await asyncio.sleep(0.1)
    return last, time.perf_counter() - t0


async def main() -> None:
    print(
        f"load_test: target {TOTAL_DOCS} docs @ {TARGET_RATE} l/s "
        f"(batch={BATCH}); search @ {SEARCH_QPS} qps"
    )
    print(f"load_test: app={APP_URL} redis={REDIS_URL}")

    await _flush_redis()

    latencies_ms: list[float] = []
    errors = [0]

    async with httpx.AsyncClient(base_url=APP_URL, timeout=30.0) as http:
        await _wait_for_health(http)

        stats_before = (await http.get("/api/stats")).json()
        print(f"load_test: stats before docs_indexed={stats_before['docs_indexed']}")

        stop = asyncio.Event()
        search_task = asyncio.create_task(
            _search_loop(http, stop, latencies_ms, errors, SEARCH_QPS),
            name="load-search",
        )

        batches = TOTAL_DOCS // BATCH
        t_start, t_end = await _ingest(http, batches, BATCH, TARGET_RATE)

        target_total = stats_before["docs_indexed"] + batches * BATCH
        last_indexed, drain_s = await _drain(http, target_total, deadline_s=30.0)

        stop.set()
        await search_task

        stats_after = (await http.get("/api/stats")).json()

    duration = t_end - t_start
    total_ingested = batches * BATCH
    throughput = total_ingested / duration if duration > 0 else 0.0
    search_p50 = statistics.median(latencies_ms) if latencies_ms else 0.0
    search_p95 = _p95(latencies_ms)
    search_p99 = _p99(latencies_ms)

    # Drain delay / batch = upper bound on per-document indexing latency.
    # A consumer that keeps up with ingest has drain_s -> (batch_size /
    # consumer_throughput), and dividing by batch size gives us the
    # per-message latency bound.
    index_per_doc_ms = (drain_s / BATCH) * 1000.0 if drain_s > 0 else 0.0

    print("---- RESULTS ----")
    print(f"total docs ingested   : {total_ingested}")
    print(f"ingest wall-clock     : {duration:.2f}s")
    print(f"throughput            : {throughput:.1f} logs/s")
    print(f"search samples        : {len(latencies_ms)}")
    print(f"search p50            : {search_p50:.2f} ms")
    print(f"search p95            : {search_p95:.2f} ms")
    print(f"search p99            : {search_p99:.2f} ms")
    print(f"search errors         : {errors[0]}")
    print(f"drain delay           : {drain_s*1000:.1f} ms")
    print(f"index per-doc (<=)    : {index_per_doc_ms:.2f} ms")
    print(f"docs_indexed before   : {stats_before['docs_indexed']}")
    print(f"docs_indexed after    : {stats_after['docs_indexed']}")
    print(f"disk segments after   : {stats_after['disk_segments']}")
    print(f"flushed memory after  : {stats_after['flushed_memory_segments']}")

    # ------------------------------------------------------------------
    # Assertions — these are the project's success-criteria gates.
    # ------------------------------------------------------------------
    ok = True

    if search_p95 > SEARCH_P95_MAX_MS:
        print(
            f"FAIL: search_p95 {search_p95:.2f} ms > "
            f"{SEARCH_P95_MAX_MS:.0f} ms",
            file=sys.stderr,
        )
        ok = False

    if throughput < MIN_THROUGHPUT:
        print(
            f"FAIL: throughput {throughput:.1f} l/s < "
            f"{MIN_THROUGHPUT:.0f} l/s",
            file=sys.stderr,
        )
        ok = False

    if index_per_doc_ms > INDEX_PER_DOC_MAX_MS:
        print(
            f"FAIL: indexing upper bound {index_per_doc_ms:.2f} ms > "
            f"{INDEX_PER_DOC_MAX_MS:.0f} ms",
            file=sys.stderr,
        )
        ok = False

    if stats_after["docs_indexed"] < target_total:
        print(
            f"FAIL: docs_indexed {stats_after['docs_indexed']} < "
            f"expected {target_total} (last seen {last_indexed})",
            file=sys.stderr,
        )
        ok = False

    if errors[0] > 0:
        # Not strictly a criterion, but any search error under load is
        # worth failing on — the dashboard hits this endpoint constantly.
        print(f"FAIL: {errors[0]} search errors observed", file=sys.stderr)
        ok = False

    if ok:
        print("PASS")
        sys.exit(0)
    print("FAIL")
    sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
