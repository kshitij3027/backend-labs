#!/usr/bin/env python3
"""Seed ~500 demo records into the running app via POST /v1/logs/ingest.

The records are backdated across a 400-day window so the lifecycle
scanner will plan a mix of promote / compress / archive / delete
transitions when /v1/evaluate fires next. Useful before a demo or
final-E2E walkthrough so the dashboard isn't blank.

Usage:
  BASE_URL=http://app:8000 python scripts/seed_demo.py
"""
import asyncio
import os
import random
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")
TOTAL_RECORDS = 500
BATCH_SIZE = 50

# Mirror config/retention_config.yaml categories so policies actually match.
CATEGORIES = ["user_activity", "payment", "health", "payment_card", "ops_audit"]
SOURCES = ["app", "db", "auth", "billing", "ingestion"]
LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]


def _generate_record(idx: int, now: datetime) -> dict:
    # Spread across the past 400 days so we get a mix of tier ages.
    days_back = random.randint(0, 400)
    ts = now - timedelta(days=days_back, hours=random.randint(0, 23))
    return {
        "ts": ts.isoformat(),
        "level": random.choice(LEVELS),
        "source": random.choice(SOURCES),
        "category": random.choice(CATEGORIES),
        "message": f"seeded record {idx}",
    }


async def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    accepted_total = 0
    async with httpx.AsyncClient(timeout=30.0) as client:
        for batch_start in range(0, TOTAL_RECORDS, BATCH_SIZE):
            batch = [
                _generate_record(idx, now)
                for idx in range(batch_start, min(batch_start + BATCH_SIZE, TOTAL_RECORDS))
            ]
            r = await client.post(
                f"{BASE_URL}/v1/logs/ingest",
                json={"records": batch},
            )
            r.raise_for_status()
            accepted_total += r.json().get("accepted", 0)
    print(f"seed_demo: ingested {accepted_total} records to {BASE_URL}")
    # Trigger one evaluation so the dashboard isn't all hot.
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(f"{BASE_URL}/v1/evaluate")
        r.raise_for_status()
        print(f"seed_demo: /v1/evaluate -> {r.json()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
