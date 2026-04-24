"""Scripted walkthrough of the full API surface.

Expects the FastAPI app to be reachable at ``APP_URL`` (default
``http://localhost:8000``). Ingests 500 synthetic log entries, runs a
handful of search queries (normal + incident mode), shows the top
suggestions, and prints the stats response. Used as the demo target
for this mini-project.
"""

from __future__ import annotations

import json
import os
import sys
import time

import httpx

from src.sample_data import generate_log_entries

APP_URL = os.environ.get("APP_URL", "http://localhost:8000")


def _pretty(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=False, default=str)


def main() -> int:
    client = httpx.Client(base_url=APP_URL, timeout=30.0)

    print(f"# Demo against {APP_URL}")
    health = client.get("/health").json()
    print(f"health -> {health}")

    print("\n# Ingest 500 synthetic entries")
    entries = [e.model_dump() for e in generate_log_entries(500, seed=0)]
    t0 = time.perf_counter()
    r = client.post("/api/logs/bulk", json={"entries": entries})
    r.raise_for_status()
    dt = (time.perf_counter() - t0) * 1000
    print(f"POST /api/logs/bulk -> {r.status_code} in {dt:.1f}ms; body: {_pretty(r.json())}")

    print("\n# Stats after ingest")
    print(_pretty(client.get("/api/search/stats").json()))

    queries = [
        ("authentication error", None),
        ("payment declined", None),
        ("slow response p99", None),
        ("error", {"mode": "incident"}),
    ]
    for q, ctx in queries:
        label = f'"{q}"' + (f" (context={ctx})" if ctx else "")
        print(f"\n# Search {label}")
        body = {"query": q, "limit": 3}
        if ctx:
            body["context"] = ctx
        r = client.post("/api/search", json=body)
        r.raise_for_status()
        data = r.json()
        print(f"intent={data['intent']!r} expanded={data['expanded_terms']} "
              f"total_hits={data['total_hits']} ranked_hits={data['ranked_hits']} "
              f"execution_time_ms={data['execution_time_ms']}")
        for i, hit in enumerate(data["results"], 1):
            print(f"  [{i}] score={hit['score']:.4f} level={hit['level']} ts={hit['timestamp']:.0f}")
            print(f"      msg: {hit['log_entry']}")
            print(f"      reasons: {hit['ranking_explanation']['reasons']}")

    print("\n# Suggestions for 'auth'")
    r = client.get("/api/search/suggestions", params={"q": "auth", "limit": 5})
    print(_pretty(r.json()))

    print("\n# Final stats")
    print(_pretty(client.get("/api/search/stats").json()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
