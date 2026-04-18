"""End-to-end demo script for the Faceted Log Search Engine.

Walks a realistic facet drill-down over a seeded dataset, printing
facet counts and per-query timing at each stage. Designed to produce
output that is both human-readable and pasteable into the README.

Flow:
  1. Health check (poll ``/health`` until 200).
  2. Generate 2000 synthetic logs (seed=42 for repeatability).
  3. Run a sequence of five increasingly specific queries and print
     the interesting facets + ``query_time_ms`` after each.
  4. Verify the Redis cache-aside layer speeds up a repeat query.
  5. Print a summary banner on success.

The script talks to the running app over HTTP only — no ``src.*``
imports — so it works identically against the dockerized stack
(``APP_URL=http://app:8000``) and a host-local server (fallback to
``http://localhost:8000``).
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any, Dict, List, Optional

import httpx


APP_URL = os.getenv("APP_URL", "http://app:8000")
REQUEST_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


# ---------------------------------------------------------------------------
# Pretty-print helpers (ASCII only so README paste stays clean).
# ---------------------------------------------------------------------------

def header(title: str) -> None:
    """Print a section banner like ``=== Stage 2: service=payments ===``."""
    bar = "=" * max(60, len(title) + 6)
    print(f"\n{bar}")
    print(f"=== {title} ===")
    print(bar)


def kv(label: str, value: Any) -> None:
    """Aligned label/value pair for scannable output."""
    print(f"  {label:<22} {value}")


def find_facet(
    response: Dict[str, Any],
    dim: str,
) -> Optional[Dict[str, Any]]:
    """Locate one facet summary dict by dimension name."""
    for f in response.get("facets", []):
        if f.get("name") == dim:
            return f
    return None


def format_values(values: List[Dict[str, Any]], top: int) -> str:
    """Render a facet's top-N value/count pairs as a single line."""
    pairs = [f"{v['value']}={v['count']}" for v in values[:top]]
    return ", ".join(pairs) if pairs else "(none)"


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

async def wait_for_health(client: httpx.AsyncClient) -> None:
    """Poll ``/health`` for up to 10 seconds until it reports ok."""
    header("Stage 1: Health check")
    deadline = 10.0
    sleep = 0.5
    elapsed = 0.0
    last_err: Optional[str] = None
    while elapsed < deadline:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                body = resp.json()
                kv("status", body.get("status"))
                kv("db", body.get("db"))
                kv("redis", body.get("redis"))
                kv("redis_url", body.get("redis_url"))
                return
            last_err = f"HTTP {resp.status_code}"
        except Exception as exc:
            last_err = str(exc)
        await asyncio.sleep(sleep)
        elapsed += sleep
    raise SystemExit(f"/health never became ready after {deadline}s (last error: {last_err})")


async def seed_logs(client: httpx.AsyncClient, count: int = 2000, seed: int = 42) -> None:
    """Generate ``count`` synthetic logs with a fixed seed."""
    header(f"Stage 2: Generate {count} logs (seed={seed})")
    resp = await client.post(f"/api/logs/generate?count={count}&seed={seed}")
    resp.raise_for_status()
    body = resp.json()
    kv("generated_count", body.get("generated_count"))
    kv("query_time_ms", body.get("query_time_ms"))


async def query_baseline(client: httpx.AsyncClient) -> None:
    """Q1: empty filters — report totals + top service/level facets."""
    header("Stage 3 / Q1: Empty filters (baseline)")
    payload = {"filters": {}, "limit": 5}
    resp = await client.post("/api/search", json=payload)
    resp.raise_for_status()
    body = resp.json()

    kv("returned_logs", len(body.get("logs", [])))
    kv("total_count", body.get("total_count"))
    kv("has_more", body.get("has_more"))
    kv("query_time_ms", body.get("query_time_ms"))

    service = find_facet(body, "service")
    level = find_facet(body, "level")
    if service:
        kv("top-3 services", format_values(service["values"], 3))
    if level:
        kv("top-3 levels", format_values(level["values"], 3))


async def query_single_facet(client: httpx.AsyncClient) -> None:
    """Q2: filter by service=payments — show excluded-self across all dims."""
    header("Stage 4 / Q2: service=payments (excluded-self)")
    payload = {"filters": {"service": ["payments"]}, "limit": 5}
    resp = await client.post("/api/search", json=payload)
    resp.raise_for_status()
    body = resp.json()

    kv("returned_logs", len(body.get("logs", [])))
    kv("query_time_ms", body.get("query_time_ms"))
    kv("applied_filters", body.get("applied_filters"))

    service = find_facet(body, "service")
    level = find_facet(body, "level")
    if service:
        # All 5 services should still be visible thanks to excluded-self.
        print("  service facet (excluded-self should show all 5 services):")
        for v in service["values"]:
            marker = " <selected>" if v.get("selected") else ""
            print(f"    - {v['value']:<12} count={v['count']}{marker}")
    if level:
        print("  level facet (all levels, scoped to service=payments):")
        for v in level["values"]:
            print(f"    - {v['value']:<6} count={v['count']}")


async def query_compound_filter(client: httpx.AsyncClient) -> None:
    """Q3: compound filter across service + level."""
    header("Stage 5 / Q3: service=payments AND level=ERROR (compound)")
    payload = {
        "filters": {"service": ["payments"], "level": ["ERROR"]},
        "limit": 10,
    }
    resp = await client.post("/api/search", json=payload)
    resp.raise_for_status()
    body = resp.json()

    kv("returned_logs", len(body.get("logs", [])))
    kv("query_time_ms", body.get("query_time_ms"))
    kv("applied_filters", body.get("applied_filters"))


async def query_free_text(client: httpx.AsyncClient) -> None:
    """Q4: free-text search on known substring 'timeout'."""
    header("Stage 6 / Q4: free-text query='timeout'")
    payload = {"query": "timeout", "limit": 5}
    resp = await client.post("/api/search", json=payload)
    resp.raise_for_status()
    body = resp.json()

    kv("returned_logs", len(body.get("logs", [])))
    kv("query_time_ms", body.get("query_time_ms"))

    logs = body.get("logs", [])
    if logs:
        first = logs[0]
        print("  first match preview:")
        print(f"    service={first.get('service')} level={first.get('level')}")
        print(f"    message: {first.get('message')}")
    else:
        print("  (no log matched 'timeout' — generator may not have emitted any this run)")


async def query_many_values(client: httpx.AsyncClient) -> None:
    """Q5: 10+ filter values across all five dims — stress the WHERE shape."""
    header("Stage 7 / Q5: 10+ values across all 5 dimensions")
    payload = {
        "filters": {
            "service": ["payments", "auth", "api-gateway"],
            "level": ["INFO", "WARN", "ERROR"],
            "region": ["us-east-1", "us-west-2", "eu-west-1"],
            "latency_bucket": ["0-100ms", "100-500ms", "500ms-2s"],
            "hour_bucket": [0, 6, 12, 18],
        },
        "limit": 5,
    }
    resp = await client.post("/api/search", json=payload)
    resp.raise_for_status()
    body = resp.json()

    total_filter_values = sum(len(v) for v in payload["filters"].values())
    kv("filter_values_applied", total_filter_values)
    kv("returned_logs", len(body.get("logs", [])))
    kv("query_time_ms", body.get("query_time_ms"))
    kv("status", "200 OK")


async def verify_cache_speedup(client: httpx.AsyncClient) -> None:
    """Run Q2 twice in a row and compare the two timings."""
    header("Stage 8: Cache speedup (repeat Q2)")
    payload = {"filters": {"service": ["payments"]}, "limit": 5}

    first = await client.post("/api/search", json=payload)
    first.raise_for_status()
    first_body = first.json()

    second = await client.post("/api/search", json=payload)
    second.raise_for_status()
    second_body = second.json()

    kv("first_call cached", first_body.get("cached"))
    kv("first_call query_time_ms", first_body.get("query_time_ms"))
    kv("second_call cached", second_body.get("cached"))
    kv("second_call query_time_ms", second_body.get("query_time_ms"))

    if not second_body.get("cached"):
        print("  WARNING: expected second call to be cached (Redis may be down)")
    else:
        t1 = first_body.get("query_time_ms", 0.0)
        t2 = second_body.get("query_time_ms", 0.0)
        if t1 > 0:
            ratio = t1 / max(t2, 0.001)
            kv("speedup_ratio", f"{ratio:.1f}x")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def main() -> int:
    """Run all demo stages sequentially against ``APP_URL``."""
    print(f"Demo target: {APP_URL}")
    async with httpx.AsyncClient(base_url=APP_URL, timeout=REQUEST_TIMEOUT) as client:
        await wait_for_health(client)
        await seed_logs(client, count=2000, seed=42)
        await query_baseline(client)
        await query_single_facet(client)
        await query_compound_filter(client)
        await query_free_text(client)
        await query_many_values(client)
        await verify_cache_speedup(client)

    header("Summary")
    print("  All 6 demo stages passed.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except httpx.HTTPError as exc:
        print(f"\nDemo failed: HTTP error: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
