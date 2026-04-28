"""Smoke load test: a small uncached + cached burst that asserts SLOs.

The full 500-request load test lives in ``scripts/load_test.py`` and is run
by ``make load-test``. This test is the lighter-weight CI gate (50 reqs /
concurrency 10) so we catch obvious regressions without paying the full
load-test cost on every test invocation. It is opt-in via ``LOAD_TEST_ENABLED``
to keep CI runs fast.
"""

from __future__ import annotations

import asyncio
import math
import os
import statistics
import time
from typing import Any

import httpx
import pytest


pytestmark = pytest.mark.skipif(
    not (os.getenv("API_URL") and os.getenv("LOAD_TEST_ENABLED")),
    reason="set both API_URL and LOAD_TEST_ENABLED=1 to run the load smoke test",
)


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
    assert response.status_code == 200, response.text
    return str(response.json()["access_token"])


async def _one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    headers: dict[str, str],
    payload: dict[str, Any],
    out: list[float],
) -> None:
    async with sem:
        start = time.perf_counter()
        response = await client.post(
            "/api/v1/logs/search",
            json=payload,
            headers=headers,
            timeout=30.0,
        )
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        assert response.status_code == 200, response.text
        out.append(elapsed_ms)


async def _phase(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    total: int,
    concurrency: int,
    payload_for: Any,
) -> list[float]:
    sem = asyncio.Semaphore(concurrency)
    out: list[float] = []
    await asyncio.gather(*[_one(client, sem, headers, payload_for(i), out) for i in range(total)])
    return out


@pytest.mark.asyncio
async def test_load_smoke_uncached_and_cached_meet_slos() -> None:
    api_url = os.environ["API_URL"].rstrip("/")
    username = os.getenv("SEED_USERNAME", os.getenv("TEST_USERNAME", "demo"))
    password = os.getenv("SEED_PASSWORD", os.getenv("TEST_PASSWORD", "demo"))
    total = int(os.getenv("LOAD_SMOKE_TOTAL", "50"))
    concurrency = int(os.getenv("LOAD_SMOKE_CONCURRENCY", "10"))
    p50_uncached_slo = float(os.getenv("LOAD_P50_UNCACHED_MS", "500"))
    p50_cached_slo = float(os.getenv("LOAD_P50_CACHED_MS", "100"))

    limits = httpx.Limits(
        max_connections=concurrency * 2,
        max_keepalive_connections=concurrency * 2,
    )

    async with httpx.AsyncClient(base_url=api_url, limits=limits, timeout=30.0) as client:
        token = await _fetch_token(client, username, password)
        headers = {"Authorization": f"Bearer {token}"}

        def uncached_payload(i: int) -> dict[str, Any]:
            return {"q": f"loadsmoke-uncached-{i}", "limit": 5}

        uncached = await _phase(client, headers, total, concurrency, uncached_payload)

        cached_query: dict[str, Any] = {"q": "loadsmoke-cached-fixed", "limit": 5}
        warmup = await client.post(
            "/api/v1/logs/search", json=cached_query, headers=headers, timeout=30.0
        )
        assert warmup.status_code == 200, warmup.text

        def cached_payload(_: int) -> dict[str, Any]:
            return cached_query

        cached = await _phase(client, headers, total, concurrency, cached_payload)

    p50_uncached = _percentile(uncached, 0.50)
    p50_cached = _percentile(cached, 0.50)

    summary = (
        f"smoke uncached(n={len(uncached)}): mean={statistics.mean(uncached):.1f}ms p50={p50_uncached:.1f}ms; "
        f"cached(n={len(cached)}): mean={statistics.mean(cached):.1f}ms p50={p50_cached:.1f}ms"
    )
    assert p50_uncached < p50_uncached_slo, f"uncached SLO breach (median): {summary}"
    assert p50_cached < p50_cached_slo, f"cached SLO breach (median): {summary}"
