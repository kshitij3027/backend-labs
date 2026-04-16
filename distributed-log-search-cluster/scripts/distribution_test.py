"""Distribution-quality E2E driver.

Indexes a large corpus of documents containing distinct terms via the
coordinator, then queries ``/cluster/stats`` to gather per-node term
counts. Asserts that ``stddev(term_counts) / mean(term_counts) < 0.20``
— i.e. consistent-hash-ring distribution is reasonably even across the
4 physical nodes.
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys

import httpx

COORDINATOR = os.getenv("COORDINATOR_URL", "http://coordinator:8000")
N_DOCS = int(os.getenv("N_DOCS", "1200"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "50"))
THRESHOLD = float(os.getenv("STDDEV_RATIO_MAX", "0.20"))


async def wait_healthy(client: httpx.AsyncClient) -> None:
    for _ in range(60):
        try:
            r = await client.get(f"{COORDINATOR}/health")
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise SystemExit("coordinator not healthy")


async def index_one(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, doc: dict
) -> None:
    async with sem:
        r = await client.post(f"{COORDINATOR}/index", json=doc, timeout=10)
        r.raise_for_status()


async def main() -> None:
    async with httpx.AsyncClient(timeout=15) as client:
        await wait_healthy(client)
        sem = asyncio.Semaphore(CONCURRENCY)
        docs = [
            {
                "doc_id": f"d{i:05d}",
                "content": (
                    f"sysevent record token_{i:05d} service region shard payload"
                ),
            }
            for i in range(N_DOCS)
        ]
        await asyncio.gather(*(index_one(client, sem, d) for d in docs))
        print(f"indexed {len(docs)} docs", flush=True)

        r = await client.get(f"{COORDINATOR}/cluster/stats")
        r.raise_for_status()
        stats = r.json()
        term_counts = [v["term_count"] for v in stats.values() if v]
        if not term_counts:
            print("FAIL: no node stats", file=sys.stderr)
            sys.exit(1)
        mean = statistics.mean(term_counts)
        stddev = statistics.pstdev(term_counts)
        ratio = (stddev / mean) if mean else float("inf")
        summary = {
            "term_counts": term_counts,
            "mean": mean,
            "stddev": round(stddev, 3),
            "ratio": round(ratio, 3),
            "threshold": THRESHOLD,
        }
        print(json.dumps(summary), flush=True)
        if ratio >= THRESHOLD:
            print(
                f"FAIL: stddev/mean ratio {ratio:.3f} >= {THRESHOLD}",
                file=sys.stderr,
            )
            sys.exit(1)
        print("PASS")


if __name__ == "__main__":
    asyncio.run(main())
