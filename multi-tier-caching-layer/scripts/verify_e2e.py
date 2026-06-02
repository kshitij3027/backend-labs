"""Cross-container end-to-end verifier for the multi-tier caching layer.

Run by the compose ``e2e`` profile service (``Dockerfile.test``), this script
talks to the live ``app`` container over HTTP + WebSocket — reaching it by
**service name** (``http://app:8000`` via ``APP_URL``), never ``localhost`` — and
asserts the §5 success criteria that can be exercised without tearing down a
dependency:

* ``check_health``        — the app comes up and reports ``{"status":"healthy"}``.
* ``miss_then_hit``       — a fresh query misses to the slow source, an identical
  repeat is served from a cache tier and is **faster** (§5 "repeated query is a
  hit on the 2nd call").
* ``semantic_equivalence``— a cosmetically-different timestamp in the *same*
  300-second bucket normalizes to the same key and therefore hits (§2 semantic
  keys).
* ``stats_shape``         — ``/cache/stats`` exposes a coherent ``performance``
  block (``overall_hit_rate`` + ``total_requests``) and per-tier detail.
* ``ws_receives``         — the ``/ws/metrics`` WebSocket delivers a ``tick``
  payload carrying live ``stats`` (§3 Feature C transport).

Each check prints progress; any failed assertion prints a clear message and
``sys.exit(1)``. On success the script prints ``E2E: all checks passed`` and
``sys.exit(0)``.

NOTE on the stop-Redis fallback: this container does **not** stop the ``redis``
container. The graceful-fallback choreography (``docker compose stop redis`` →
re-query still 200 → ``start redis``) is driven by the host / main thread
separately (see plan.md "Stop-Redis fallback"), and the in-process equivalent
lives in ``tests/e2e/test_cache_flow.py``.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import httpx
import websockets

# Talk to the app by service name inside the compose network.
APP_URL = os.environ.get("APP_URL", "http://app:8000")

# A fresh, unique cache-buster computed once per process run. ``_nonce`` is an
# INERT param: the backend handlers only read source/start/end/bucket/limit
# (see src/backend.py), so it is ignored server-side, but src/keys.py folds
# every non-timestamp param into the canonical SHA-256 cache key. Threading a
# unique nonce through the queries guarantees the first /query is a genuine
# COLD miss regardless of any prior cache state (e.g. a warm Redis/Postgres left
# over from ``make test`` on the same volume), making this verifier
# order-independent. ``time.time()`` is fine here — this script runs as a normal
# OS process, not in a frozen/replayed environment.
_NONCE = f"{time.time():.6f}-{os.getpid()}"

# A query window that brackets the seeded corpus (db-init seeds end_ts ~ 1.78e9).
_BASE_PARAMS = {
    "source": "api",
    "start": 1_779_000_000,
    "end": 1_781_000_000,
    "_nonce": _NONCE,
}
_QUERY_BODY = {"query": "error_rate", "params": dict(_BASE_PARAMS)}

# Per-request timeout for HTTP calls (the cold path runs a real GROUP BY scan).
_HTTP_TIMEOUT = 15.0


def _fail(message: str) -> None:
    """Print a failure banner and exit non-zero."""
    print(f"E2E FAILED: {message}")
    sys.exit(1)


async def check_health(client: httpx.AsyncClient) -> None:
    """Poll ``GET /health`` up to 30×1s until ``{"status":"healthy"}``."""
    url = f"{APP_URL}/health"
    print(f"E2E: polling {url} (max 30 attempts) ...")
    for attempt in range(1, 31):
        try:
            resp = await client.get(url, timeout=5.0)
            if resp.status_code == 200 and resp.json().get("status") == "healthy":
                print(f"  health OK (attempt {attempt})")
                return
            print(f"  attempt {attempt}: HTTP {resp.status_code}")
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            print(f"  attempt {attempt}: {exc}")
        await asyncio.sleep(1)
    _fail("health check never returned healthy")


async def miss_then_hit(client: httpx.AsyncClient) -> None:
    """First query misses to backend/l3; an identical repeat hits and is faster."""
    print("E2E: miss-then-hit (repeat query is a hit on the 2nd call) ...")

    first = await client.post(f"{APP_URL}/query", json=_QUERY_BODY, timeout=_HTTP_TIMEOUT)
    if first.status_code != 200:
        _fail(f"first /query returned HTTP {first.status_code}: {first.text}")
    meta1 = first.json().get("meta", {})
    tier1 = meta1.get("tier")
    t1 = meta1.get("elapsed_ms")
    if tier1 not in {"backend", "l3"}:
        _fail(f"first /query expected a cold tier (backend/l3), got {tier1!r}")
    print(f"  1st call: tier={tier1} elapsed_ms={t1}")

    second = await client.post(f"{APP_URL}/query", json=_QUERY_BODY, timeout=_HTTP_TIMEOUT)
    if second.status_code != 200:
        _fail(f"second /query returned HTTP {second.status_code}: {second.text}")
    meta2 = second.json().get("meta", {})
    tier2 = meta2.get("tier")
    t2 = meta2.get("elapsed_ms")
    if tier2 not in {"l1", "l2"}:
        _fail(f"second /query expected a cache hit (l1/l2), got {tier2!r}")
    if not (isinstance(t1, (int, float)) and isinstance(t2, (int, float))):
        _fail(f"missing elapsed_ms on meta (t1={t1!r}, t2={t2!r})")
    if not t2 < t1:
        _fail(f"cache hit was not faster: t2={t2}ms !< t1={t1}ms")
    print(f"  2nd call: tier={tier2} elapsed_ms={t2}  (faster than {t1}) OK")


async def semantic_equivalence(client: httpx.AsyncClient) -> None:
    """A same-bucket, cosmetically-different timestamp normalizes to a hit."""
    print("E2E: semantic equivalence (same 300s bucket -> same key -> hit) ...")
    # 1_779_000_123 floors to the same 300s bucket as 1_779_000_000. Spreading
    # _BASE_PARAMS first carries the SAME _nonce as the miss-then-hit query, so
    # this cosmetically-different timestamp normalizes to that exact key and hits.
    body = {
        "query": "error_rate",
        "params": {**_BASE_PARAMS, "start": 1_779_000_123},
    }
    resp = await client.post(f"{APP_URL}/query", json=body, timeout=_HTTP_TIMEOUT)
    if resp.status_code != 200:
        _fail(f"semantic /query returned HTTP {resp.status_code}: {resp.text}")
    tier = resp.json().get("meta", {}).get("tier")
    if tier not in {"l1", "l2"}:
        _fail(
            "semantically-equivalent query (same bucket) expected a cache hit "
            f"(l1/l2), got {tier!r}"
        )
    print(f"  cosmetically-different timestamp -> tier={tier} OK")


async def stats_shape(client: httpx.AsyncClient) -> None:
    """`/cache/stats` exposes a coherent performance + tiers surface."""
    print("E2E: /cache/stats shape ...")
    resp = await client.get(f"{APP_URL}/cache/stats", timeout=_HTTP_TIMEOUT)
    if resp.status_code != 200:
        _fail(f"/cache/stats returned HTTP {resp.status_code}: {resp.text}")
    data = resp.json()

    perf = data.get("performance")
    if not isinstance(perf, dict):
        _fail(f"/cache/stats missing 'performance' object: {data}")
    hit_rate = perf.get("overall_hit_rate")
    if not isinstance(hit_rate, (int, float)):
        _fail(f"performance.overall_hit_rate is not a number: {hit_rate!r}")
    total = perf.get("total_requests")
    if not isinstance(total, int) or total < 3:
        _fail(f"performance.total_requests expected >= 3, got {total!r}")

    tiers = data.get("tiers")
    if not isinstance(tiers, dict) or "l1" not in tiers:
        _fail(f"/cache/stats missing a 'tiers' block with l1: {tiers}")

    print(
        f"  stats OK (overall_hit_rate={hit_rate}, total_requests={total}, "
        f"tiers={sorted(tiers)})"
    )


async def ws_receives() -> None:
    """Connect to ``/ws/metrics`` and assert one ``tick`` payload with stats."""
    ws_url = APP_URL.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_url}/ws/metrics"
    print(f"E2E: WebSocket {ws_url} ...")
    try:
        async with websockets.connect(ws_url, open_timeout=10) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
    except Exception as exc:  # noqa: BLE001 — any failure here is a hard fail
        _fail(f"WebSocket connect/recv failed: {exc}")

    try:
        data = json.loads(raw)
    except (ValueError, TypeError) as exc:
        _fail(f"WebSocket payload was not JSON: {exc}")

    if data.get("type") != "tick":
        _fail(f"WebSocket message expected type='tick', got {data.get('type')!r}")
    if "stats" not in data:
        _fail(f"WebSocket message missing 'stats': {sorted(data)}")
    print("  WebSocket tick received with stats OK")


async def main() -> None:
    """Run every check in order; exit 0 only if all pass."""
    async with httpx.AsyncClient() as client:
        await check_health(client)
        await miss_then_hit(client)
        await semantic_equivalence(client)
        await stats_shape(client)
    await ws_receives()
    print("E2E: all checks passed")
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
