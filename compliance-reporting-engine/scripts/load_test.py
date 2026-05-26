"""Minimal 5-concurrent sanity load test.

Verifies the "<2 min for a 30-day window" target from
project_requirements.md §5 by issuing 5 concurrent generations and
asserting they all reach COMPLETED inside 120s. Not a stretch load
test — it's a smoke check that the coordinator's semaphore + the
async generation pipeline can sustain the documented concurrency.

Run inside the ``tester`` container via::

    docker compose --profile test run --rm tester python scripts/load_test.py
"""
from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
CONCURRENCY = 5
TIMEOUT_SECONDS = 120
FRAMEWORKS = ["SOX", "HIPAA", "PCI_DSS", "GDPR", "FINHEALTH"]
FORMATS = ["JSON", "CSV", "PDF", "XML", "JSON"]  # one per framework


async def generate_and_wait(
    client: httpx.AsyncClient, framework: str, fmt: str
) -> tuple[str, float]:
    """Kick off one generate request and poll status until terminal.

    Returns ``(state, elapsed_seconds)`` so the caller can compute the
    success-rate + latency percentiles. ``state`` is ``"TIMEOUT"`` when
    the budget runs out before COMPLETED/FAILED.
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=30)
    started = time.monotonic()
    r = await client.post(
        "/reports/generate",
        json={
            "framework": framework,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "export_format": fmt,
        },
    )
    r.raise_for_status()
    report_id = r.json()["report_id"]
    while True:
        if time.monotonic() - started > TIMEOUT_SECONDS:
            return "TIMEOUT", time.monotonic() - started
        s = await client.get(f"/reports/{report_id}")
        state = s.json()["state"]
        if state in ("COMPLETED", "FAILED"):
            return state, time.monotonic() - started
        await asyncio.sleep(1)


async def main() -> int:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        tasks = [
            generate_and_wait(client, f, fmt) for f, fmt in zip(FRAMEWORKS, FORMATS)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
    states = [r[0] for r in results]
    latencies = [r[1] for r in results]
    success = sum(1 for s in states if s == "COMPLETED")
    success_rate = success / len(results)
    print(f"Concurrency: {CONCURRENCY}")
    print(f"States:      {states}")
    print(f"Latencies:   {[f'{l:.1f}s' for l in latencies]}")
    print(f"p50:         {statistics.median(latencies):.1f}s")
    print(f"p95:         {sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)]:.1f}s")
    print(f"Success:     {success}/{len(results)} ({success_rate:.0%})")
    if success_rate < 1.0:
        print("FAIL: not all reports reached COMPLETED")
        return 1
    if max(latencies) > TIMEOUT_SECONDS:
        print(f"FAIL: max latency {max(latencies):.1f}s > {TIMEOUT_SECONDS}s budget")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
