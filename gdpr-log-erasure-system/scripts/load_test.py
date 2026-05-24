"""Load test: 100 concurrent erasure requests with concurrency cap 10.

For each iteration, pre-seed one user with 3 mappings, then submit one
erasure request and poll until terminal. Measures end-to-end latency
(track + request + poll) and success rate.

Assertions (per project_requirements.md §5 Performance):
- success_rate ≥ 0.99
- mean per-request POST latency (just the POST, not poll loop) < 500 ms

Reports p50, p95, p99 of full end-to-end latency.
"""
from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time
import uuid

import httpx


BASE_URL = os.environ.get("BASE_URL", "http://app:8000")
TOTAL_REQUESTS = int(os.environ.get("LOAD_TOTAL", "100"))
CONCURRENCY = int(os.environ.get("LOAD_CONCURRENCY", "10"))
TIMEOUT_S = float(os.environ.get("LOAD_TIMEOUT", "30"))


async def _one_request(client: httpx.AsyncClient, idx: int) -> dict:
    user = f"load-{uuid.uuid4().hex[:8]}-{idx}"
    t0 = time.perf_counter()

    # 3 mappings (mixed anonymisable + PII)
    for dtype in ("system_logs", "analytics_events", "personal_profile"):
        r = await client.post(
            "/api/user-data-tracking",
            json={
                "user_id": user, "data_type": dtype,
                "storage_location": f"loc-{idx}-{dtype}",
                "metadata": {"user_id": user, "ip": "10.0.0.1"},
            },
        )
        r.raise_for_status()

    # Submit erasure
    t_post0 = time.perf_counter()
    r = await client.post(
        "/api/erasure-requests",
        json={"user_id": user, "request_type": "ANONYMIZE"},
    )
    r.raise_for_status()
    post_latency = time.perf_counter() - t_post0
    rid = r.json()["id"]

    # Poll until terminal
    deadline = time.perf_counter() + TIMEOUT_S
    state = "PENDING"
    while time.perf_counter() < deadline:
        gr = await client.get(f"/api/erasure-requests/{rid}")
        gr.raise_for_status()
        state = gr.json()["state"]
        if state in ("COMPLETED", "FAILED"):
            break
        await asyncio.sleep(0.05)

    e2e = time.perf_counter() - t0
    return {
        "idx": idx, "state": state, "e2e_s": e2e,
        "post_s": post_latency, "request_id": rid,
        "success": state == "COMPLETED",
    }


async def main() -> int:
    print(f"BASE_URL = {BASE_URL}")
    print(f"TOTAL    = {TOTAL_REQUESTS}")
    print(f"CONCUR   = {CONCURRENCY}")
    print(f"TIMEOUT  = {TIMEOUT_S}s per request")
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=TIMEOUT_S + 5) as client:
        # Health check
        r = await client.get("/health")
        r.raise_for_status()

        async def _bounded(i: int) -> dict:
            async with sem:
                return await _one_request(client, i)

        t0 = time.perf_counter()
        results = await asyncio.gather(
            *[_bounded(i) for i in range(TOTAL_REQUESTS)],
            return_exceptions=True,
        )
        total_wall = time.perf_counter() - t0

    successes = [r for r in results if isinstance(r, dict) and r["success"]]
    failures = [r for r in results if not isinstance(r, dict) or not r.get("success")]

    e2e_latencies = sorted([r["e2e_s"] for r in successes])
    post_latencies = sorted([r["post_s"] for r in successes])

    def _pct(xs: list[float], p: float) -> float:
        if not xs:
            return 0.0
        idx = max(0, min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1)))))
        return xs[idx]

    print()
    print("── LOAD TEST RESULTS ──")
    print(f"total                = {TOTAL_REQUESTS}")
    print(f"successes            = {len(successes)}")
    print(f"failures             = {len(failures)}")
    print(f"success_rate         = {len(successes) / TOTAL_REQUESTS:.4f}")
    print(f"wall_time_s          = {total_wall:.2f}")
    if e2e_latencies:
        print(f"e2e_mean_s           = {statistics.mean(e2e_latencies):.3f}")
        print(f"e2e_p50_s            = {_pct(e2e_latencies, 50):.3f}")
        print(f"e2e_p95_s            = {_pct(e2e_latencies, 95):.3f}")
        print(f"e2e_p99_s            = {_pct(e2e_latencies, 99):.3f}")
    if post_latencies:
        print(f"post_mean_ms         = {statistics.mean(post_latencies) * 1000:.1f}")
        print(f"post_p95_ms          = {_pct(post_latencies, 95) * 1000:.1f}")

    # Assertions
    success_rate = len(successes) / TOTAL_REQUESTS if TOTAL_REQUESTS else 0
    post_mean_ms = (statistics.mean(post_latencies) * 1000) if post_latencies else 0

    failed = False
    if success_rate < 0.99:
        print(f"FAIL: success_rate {success_rate:.4f} < 0.99")
        failed = True
    if post_mean_ms >= 500:
        print(f"FAIL: post_mean_ms {post_mean_ms:.1f} >= 500")
        failed = True

    if failed:
        # Print first 5 failures
        print()
        print("first failures:")
        for r in failures[:5]:
            print(f"  {r!r}")
        return 1

    print("LOAD TEST PASS")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
