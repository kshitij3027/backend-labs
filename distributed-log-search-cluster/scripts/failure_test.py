"""Failure-mode E2E driver.

Continuously issues `/search` requests against the coordinator for
``DURATION_SEC`` seconds while the test orchestrator externally stops a
node mid-run (``docker compose stop <TARGET_NODE>``) and restarts it.

At the end asserts:
  * no hard HTTP failures occurred,
  * at least one query was served with ``failed_nodes`` populated (partial
    success), proving graceful degradation actually exercised.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import httpx

COORDINATOR = os.getenv("COORDINATOR_URL", "http://coordinator:8000")
DURATION = float(os.getenv("DURATION_SEC", "15"))
TARGET_NODE = os.getenv("TARGET_NODE", "node-3")

_WORDS = [
    "error", "timeout", "login", "database", "request", "user", "service",
    "cache", "queue", "retry", "connection", "socket", "disk", "memory",
    "network", "tls", "auth", "token", "session", "payload", "replica",
    "shard", "primary", "secondary", "coordinator", "worker", "scheduler",
    "partition", "offset", "commit", "rollback", "deadlock", "latency",
    "throughput", "backoff", "heartbeat", "gossip", "consensus", "quorum",
]

DOCS = [
    {
        "doc_id": f"d{i}",
        "content": " ".join(_WORDS[(i * 3 + j) % len(_WORDS)] for j in range(8))
        + f" host-{i} region-{i % 4}",
    }
    for i in range(40)
]


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


async def index_all(client: httpx.AsyncClient) -> None:
    for d in DOCS:
        r = await client.post(f"{COORDINATOR}/index", json=d)
        r.raise_for_status()


async def main() -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        await wait_healthy(client)
        await index_all(client)
        print(f"indexed {len(DOCS)} docs, target={TARGET_NODE}", flush=True)

        t_end = time.monotonic() + DURATION
        ok = fail = partial = 0
        iteration = 0
        # Vary the query each iteration so coordinator result cache never
        # serves a stale entry — we need every request to actually hit the
        # scatter-gather path so partial-failure behavior is observable.
        # Pairs of words spanning the full vocabulary — guarantees queries
        # route to every node in the ring (including the target node that
        # is taken down mid-run), making partial failure observable.
        query_pool = [
            f"{_WORDS[i]} {_WORDS[(i + 7) % len(_WORDS)]}"
            for i in range(len(_WORDS))
        ]
        while time.monotonic() < t_end:
            q = query_pool[iteration % len(query_pool)]
            iteration += 1
            try:
                r = await client.post(
                    f"{COORDINATOR}/search",
                    json={
                        "query": q,
                        "op": "OR",
                        "limit": 20,
                    },
                )
                if r.status_code != 200:
                    fail += 1
                else:
                    body = r.json()
                    if body.get("failed_nodes"):
                        partial += 1
                    else:
                        ok += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.2)

        summary = {"ok": ok, "partial": partial, "fail": fail}
        print(json.dumps(summary), flush=True)
        if fail > 0:
            print("FAIL: had hard failures", file=sys.stderr)
            sys.exit(1)
        if partial == 0:
            print(
                "FAIL: no partial-failure queries observed — node outage did "
                "not surface in coordinator responses",
                file=sys.stderr,
            )
            sys.exit(2)
        print("PASS")


if __name__ == "__main__":
    asyncio.run(main())
