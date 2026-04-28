"""Async load test for the Log Search API.

Two phases:

  1. Uncached: 500 search requests with distinct queries (each is a fresh
     cache miss).
  2. Cached: 500 search requests with the *same* query (the first warms the
     cache, the rest are hits).

For each phase we print p50/p95/p99 latency and assert SLOs:

    p95_uncached < 500 ms
    p95_cached   < 100 ms

Exits non-zero if any SLO is breached or any request fails.

Usage (inside the api container):

    python scripts/load_test.py

Environment overrides:

    API_URL              default http://api:8000
    SEED_USERNAME        default demo
    SEED_PASSWORD        default demo
    LOAD_TOTAL           default 500
    LOAD_CONCURRENCY     default 50
    LOAD_P95_UNCACHED_MS default 500
    LOAD_P95_CACHED_MS   default 100
"""

from __future__ import annotations

import asyncio
import math
import os
import statistics
import sys
import time
from typing import Any

import httpx


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    k = (len(s) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


async def _fetch_token(client: httpx.AsyncClient, username: str, password: str) -> str:
    response = await client.post(
        "/api/v1/auth/token",
        data={"username": username, "password": password},
        timeout=30.0,
    )
    if response.status_code != 200:
        print(
            f"FATAL: token request failed status={response.status_code} body={response.text[:300]}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return str(response.json()["access_token"])


async def _one_request(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    headers: dict[str, str],
    payload: dict[str, Any],
    results: list[float],
    failures: list[str],
) -> None:
    async with sem:
        start = time.perf_counter()
        try:
            response = await client.post(
                "/api/v1/logs/search",
                json=payload,
                headers=headers,
                timeout=30.0,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if response.status_code != 200:
                failures.append(f"status={response.status_code} body={response.text[:200]}")
                return
            results.append(elapsed_ms)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"exception={type(exc).__name__}: {exc}")


async def _run_phase(
    label: str,
    client: httpx.AsyncClient,
    headers: dict[str, str],
    total: int,
    concurrency: int,
    payload_for: callable,  # type: ignore[valid-type]
) -> tuple[list[float], list[str]]:
    sem = asyncio.Semaphore(concurrency)
    results: list[float] = []
    failures: list[str] = []
    started = time.perf_counter()
    tasks = [
        _one_request(client, sem, headers, payload_for(i), results, failures)
        for i in range(total)
    ]
    await asyncio.gather(*tasks)
    duration = time.perf_counter() - started
    print(
        f"[{label}] {total} reqs in {duration:.2f}s  "
        f"throughput={total / duration:.1f} rps  failures={len(failures)}"
    )
    return results, failures


def _summarize(label: str, latencies: list[float]) -> dict[str, float]:
    if not latencies:
        print(f"[{label}] no successful requests — cannot summarise")
        return {"p50": float("nan"), "p95": float("nan"), "p99": float("nan"), "mean": float("nan")}
    p50 = _percentile(latencies, 0.50)
    p95 = _percentile(latencies, 0.95)
    p99 = _percentile(latencies, 0.99)
    mean = statistics.mean(latencies)
    print(
        f"[{label}] count={len(latencies)} mean={mean:.1f}ms "
        f"p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms "
        f"min={min(latencies):.1f}ms max={max(latencies):.1f}ms"
    )
    return {"p50": p50, "p95": p95, "p99": p99, "mean": mean}


async def _main_async() -> int:
    api_url = os.getenv("API_URL", "http://api:8000").rstrip("/")
    username = os.getenv("SEED_USERNAME", "demo")
    password = os.getenv("SEED_PASSWORD", "demo")
    total = int(os.getenv("LOAD_TOTAL", "500"))
    concurrency = int(os.getenv("LOAD_CONCURRENCY", "50"))
    p95_uncached_slo = float(os.getenv("LOAD_P95_UNCACHED_MS", "500"))
    p95_cached_slo = float(os.getenv("LOAD_P95_CACHED_MS", "100"))

    print(
        f"load test target={api_url} total={total} concurrency={concurrency} "
        f"p95_slo uncached<{p95_uncached_slo}ms cached<{p95_cached_slo}ms"
    )

    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency * 2)
    async with httpx.AsyncClient(base_url=api_url, limits=limits, timeout=30.0) as client:
        token = await _fetch_token(client, username, password)
        headers = {"Authorization": f"Bearer {token}"}

        # Phase 1: distinct queries → cache miss every time.
        def uncached_payload(i: int) -> dict[str, Any]:
            return {"q": f"loadtest-uncached-{i}", "limit": 10}

        uncached_latencies, uncached_failures = await _run_phase(
            "uncached", client, headers, total, concurrency, uncached_payload
        )

        # Phase 2: identical query → first call warms the cache, rest are hits.
        cached_query = {"q": "loadtest-cached-fixed", "limit": 10}

        # Warm the cache deterministically.
        warmup = await client.post(
            "/api/v1/logs/search", json=cached_query, headers=headers, timeout=30.0
        )
        if warmup.status_code != 200:
            print(
                f"FATAL: cache warmup failed status={warmup.status_code} body={warmup.text[:300]}",
                file=sys.stderr,
            )
            return 1

        def cached_payload(_: int) -> dict[str, Any]:
            return cached_query

        cached_latencies, cached_failures = await _run_phase(
            "cached", client, headers, total, concurrency, cached_payload
        )

    print("------------------------------------------------------------")
    uncached_stats = _summarize("uncached", uncached_latencies)
    cached_stats = _summarize("cached", cached_latencies)
    print("------------------------------------------------------------")

    failed_count = len(uncached_failures) + len(cached_failures)
    if failed_count > 0:
        print(f"FAIL: {failed_count} request failures detected", file=sys.stderr)
        for f in (uncached_failures + cached_failures)[:10]:
            print(f"  - {f}", file=sys.stderr)
        return 1

    breaches: list[str] = []
    if uncached_stats["p95"] >= p95_uncached_slo:
        breaches.append(
            f"uncached p95={uncached_stats['p95']:.1f}ms >= SLO {p95_uncached_slo}ms"
        )
    if cached_stats["p95"] >= p95_cached_slo:
        breaches.append(
            f"cached p95={cached_stats['p95']:.1f}ms >= SLO {p95_cached_slo}ms"
        )

    if breaches:
        print("FAIL: SLO breach", file=sys.stderr)
        for b in breaches:
            print(f"  - {b}", file=sys.stderr)
        return 1

    print("PASS: all SLOs met")
    return 0


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == "__main__":
    raise SystemExit(main())
