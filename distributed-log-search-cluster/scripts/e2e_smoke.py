"""End-to-end correctness smoke test for the distributed log search cluster.

Indexes a fixed 6-document corpus through the coordinator, runs a fixed set
of AND/OR queries, and asserts exact expected ``doc_id`` set equality plus
the presence of required observability fields.

Run inside Docker:
    docker compose run --rm test python scripts/e2e_smoke.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import httpx

COORDINATOR = os.getenv("COORDINATOR_URL", "http://coordinator:8000")

DOCS = [
    {"doc_id": "d1", "content": "error timeout login failed"},
    {"doc_id": "d2", "content": "user login success"},
    {"doc_id": "d3", "content": "timeout error database connection"},
    {"doc_id": "d4", "content": "database user success"},
    {"doc_id": "d5", "content": "network partition retry"},
    {"doc_id": "d6", "content": "tls handshake error"},
]

QUERIES: list[tuple[dict, set[str]]] = [
    ({"query": "error", "op": "AND"}, {"d1", "d3", "d6"}),
    ({"query": "login", "op": "AND"}, {"d1", "d2"}),
    ({"query": "error timeout", "op": "AND"}, {"d1", "d3"}),
    ({"query": "user database", "op": "AND"}, {"d4"}),
    ({"query": "error timeout", "op": "OR"}, {"d1", "d3", "d6"}),
    ({"query": "network tls", "op": "OR"}, {"d5", "d6"}),
    ({"query": "nonexistenttoken", "op": "AND"}, set()),
]

REQUIRED_FIELDS = (
    "search_time_ms",
    "nodes_queried",
    "routing_ms",
    "scatter_ms",
    "merge_ms",
    "failed_nodes",
)


async def wait_healthy(client: httpx.AsyncClient) -> None:
    for _ in range(60):
        try:
            r = await client.get(f"{COORDINATOR}/health")
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise SystemExit("coordinator not healthy after 60s")


async def index_docs(client: httpx.AsyncClient) -> None:
    for d in DOCS:
        r = await client.post(f"{COORDINATOR}/index", json=d)
        r.raise_for_status()


async def run_queries(client: httpx.AsyncClient) -> list[dict]:
    failures: list[dict] = []
    for req, expected in QUERIES:
        r = await client.post(f"{COORDINATOR}/search", json={**req, "limit": 50})
        diag: dict = {"query": req, "expected": sorted(expected)}
        if r.status_code != 200:
            diag["error"] = f"HTTP {r.status_code}: {r.text[:200]}"
            failures.append(diag)
            print(json.dumps({"FAIL": diag}), flush=True)
            continue
        body = r.json()
        got = {doc["doc_id"] for doc in body.get("documents", [])}
        diag["got"] = sorted(got)
        missing = [f for f in REQUIRED_FIELDS if f not in body]
        if missing:
            diag["missing_fields"] = missing
            failures.append(diag)
            print(json.dumps({"FAIL": diag}), flush=True)
            continue
        if body.get("failed_nodes"):
            diag["failed_nodes"] = body["failed_nodes"]
            failures.append(diag)
            print(json.dumps({"FAIL": diag}), flush=True)
            continue
        if got != expected:
            failures.append(diag)
            print(json.dumps({"FAIL": diag}), flush=True)
            continue
        print(json.dumps({"PASS": diag}), flush=True)
    return failures


async def check_cluster_stats(client: httpx.AsyncClient) -> list[str]:
    problems: list[str] = []
    r = await client.get(f"{COORDINATOR}/cluster/stats")
    if r.status_code != 200:
        return [f"/cluster/stats HTTP {r.status_code}"]
    body = r.json()
    if len(body) != 4:
        problems.append(f"expected 4 nodes in /cluster/stats, got {len(body)}")
    for nid, stats in body.items():
        if not stats:
            problems.append(f"node {nid} returned no stats")
            continue
        if stats.get("term_count", 0) <= 0:
            problems.append(f"node {nid} term_count={stats.get('term_count')}")
    return problems


async def check_health(client: httpx.AsyncClient) -> list[str]:
    r = await client.get(f"{COORDINATOR}/health")
    if r.status_code != 200:
        return [f"/health HTTP {r.status_code}"]
    body = r.json()
    if body.get("healthy_nodes") != 4 or body.get("total_nodes") != 4:
        return [f"/health expected 4/4 healthy, got {body.get('healthy_nodes')}/{body.get('total_nodes')}"]
    return []


async def main() -> None:
    async with httpx.AsyncClient(timeout=15) as client:
        await wait_healthy(client)
        await index_docs(client)
        print(f"indexed {len(DOCS)} docs", flush=True)

        query_failures = await run_queries(client)
        stats_problems = await check_cluster_stats(client)
        health_problems = await check_health(client)

        summary = {
            "query_failures": len(query_failures),
            "stats_problems": stats_problems,
            "health_problems": health_problems,
        }
        print(json.dumps({"summary": summary}), flush=True)

        if query_failures or stats_problems or health_problems:
            print("E2E SMOKE FAIL", file=sys.stderr)
            sys.exit(1)
        print("E2E SMOKE PASS")


if __name__ == "__main__":
    asyncio.run(main())
