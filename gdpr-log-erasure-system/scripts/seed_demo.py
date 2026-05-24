"""Seed ~50 users with 3-5 mappings each across various data_types.

Posts directly to the live API at BASE_URL (default http://app:8000).
Idempotent — re-running won't fail (POST is idempotent on the unique tuple).
"""
from __future__ import annotations

import asyncio
import os
import random
import sys

import httpx


DATA_TYPES: tuple[str, ...] = (
    "system_logs",
    "analytics_events",
    "performance_metrics",
    "aggregated_data",  # anonymizable
    "personal_profile",
    "billing_records",
    "session_data",     # not anonymizable
)
ANONYMIZABLE = {"system_logs", "analytics_events", "performance_metrics", "aggregated_data"}
STORAGE_SYSTEMS: tuple[str, ...] = ("logs-cluster", "kafka-topic", "elastic-index", "s3-bucket", "snowflake-table")
USER_COUNT: int = 50


def _make_payload(user_id: str, data_type: str, storage: str, idx: int) -> dict:
    return {
        "user_id": user_id,
        "data_type": data_type,
        "storage_location": f"{storage}-{idx}",
        "data_path": f"/data/{data_type}/{user_id}",
        "metadata": {
            "user_id": user_id,
            "email": f"{user_id}@example.test",
            "ip": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "level": random.choice(["INFO", "DEBUG", "WARN"]),
        },
    }


async def _seed(base_url: str) -> dict[str, int]:
    rng = random.Random(20260524)  # deterministic
    posts = 0
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        # Health check first
        try:
            r = await client.get("/health")
            r.raise_for_status()
        except Exception as e:
            print(f"FATAL: cannot reach {base_url}/health — {e!r}", file=sys.stderr)
            sys.exit(2)

        for i in range(USER_COUNT):
            user_id = f"user-{i:03d}"
            mapping_count = rng.randint(3, 5)
            chosen_types = rng.sample(DATA_TYPES, mapping_count)
            for dtype in chosen_types:
                storage = rng.choice(STORAGE_SYSTEMS)
                payload = _make_payload(user_id, dtype, storage, i)
                r = await client.post("/api/user-data-tracking", json=payload)
                r.raise_for_status()
                posts += 1

        stats_resp = await client.get("/api/statistics")
        stats_resp.raise_for_status()
        stats = stats_resp.json()

    summary = {
        "users_seeded": USER_COUNT,
        "posts_sent": posts,
        "stats_total_mappings": stats["total_mappings"],
        "stats_unique_users": stats["unique_users"],
        "data_types": len(stats["data_type_counts"]),
    }
    return summary


def main() -> None:
    base_url = os.environ.get("BASE_URL", "http://app:8000")
    print(f"seeding via {base_url}")
    summary = asyncio.run(_seed(base_url))
    print("SEED SUMMARY:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"DONE — posted {summary['posts_sent']} mappings for {summary['users_seeded']} users.")


if __name__ == "__main__":
    main()
