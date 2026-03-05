#!/usr/bin/env python3
"""Demo script for the distributed log query system."""

import json
import sys
import time
import httpx

BASE_URL = "http://localhost:8080"


def print_response(name: str, data: dict):
    print(f"\n{'='*60}")
    print(f" {name}")
    print(f"{'='*60}")
    print(f"  Query ID:         {data['query_id']}")
    print(f"  Total Results:    {data['total_results']}")
    print(f"  Partitions:       {data['partitions_successful']}/{data['partitions_queried']}")
    print(f"  Execution Time:   {data['total_execution_time_ms']}ms")
    print(f"  Cached:           {data['cached']}")
    if data['results']:
        print(f"\n  First 3 results:")
        for entry in data['results'][:3]:
            print(f"    [{entry['level']:5}] {entry['timestamp'][:19]} | {entry['service']:20} | {entry['message'][:50]}")


def main():
    coordinator = sys.argv[1] if len(sys.argv) > 1 else BASE_URL
    client = httpx.Client(base_url=coordinator, timeout=10.0)

    # Health check
    print("\n--- Health Check ---")
    resp = client.get("/health")
    print(json.dumps(resp.json(), indent=2))

    # Basic query
    resp = client.post("/query", json={"limit": 5})
    print_response("Basic Query (limit=5)", resp.json())

    # Filtered query - ERROR only
    resp = client.post("/query", json={
        "filters": [{"field": "level", "operator": "eq", "value": "ERROR"}],
        "limit": 5,
    })
    print_response("Filtered Query (ERROR only)", resp.json())

    # Service filter
    resp = client.post("/query", json={
        "filters": [{"field": "service", "operator": "eq", "value": "auth-service"}],
        "limit": 5,
    })
    print_response("Service Filter (auth-service)", resp.json())

    # Contains filter
    resp = client.post("/query", json={
        "filters": [{"field": "message", "operator": "contains", "value": "timeout"}],
        "limit": 5,
    })
    print_response("Contains Filter (timeout in message)", resp.json())

    # Cache test - repeat first query
    resp = client.post("/query", json={"limit": 5})
    print_response("Cache Test (repeat basic query)", resp.json())

    # Stats
    print("\n--- System Stats ---")
    resp = client.get("/stats")
    print(json.dumps(resp.json(), indent=2))

    print(f"\n{'='*60}")
    print(" Demo complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
