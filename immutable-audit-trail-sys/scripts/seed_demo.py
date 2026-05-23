"""Seed the audit trail with ~30 realistic-looking records so the
dashboard shows something on first load.

Run via:
    docker compose --profile test run --rm -e BASE_URL=http://app:8000 \
        tester python scripts/seed_demo.py

Assumes the app is up via `make up`.
"""
from __future__ import annotations

import asyncio
import os
import random
import string

import httpx

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000")

ACTORS = ["alice", "bob", "carol", "dave", "eve"]
ACTIONS = ["read", "search", "export", "redact"]
RESOURCE_TEMPLATES = [
    "LOG_/var/log/app.log",
    "LOG_/var/log/auth.log",
    "PATIENT_42",
    "PATIENT_43",
    "PATIENT_44",
    "CARDHOLDER_4111111111111111",
    "CARDHOLDER_4222222222222222",
    "QUERY_search?term=login",
]


def _digest() -> str:
    return "".join(random.choices(string.hexdigits.lower()[:16], k=64))


async def main(count: int = 30) -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        for i in range(count):
            actor = random.choice(ACTORS)
            body = {
                "actor": actor,
                "action": random.choice(ACTIONS),
                "resource": random.choice(RESOURCE_TEMPLATES),
                "success": random.random() > 0.1,  # 10% failures
                "args_digest": _digest(),
                "result_digest": _digest(),
                "processing_ms": round(random.uniform(0.2, 8.5), 2),
            }
            if not body["success"]:
                body["error_message"] = random.choice([
                    "permission denied", "resource not found", "rate limited"
                ])
            r = await client.post(f"{BASE_URL}/v1/audit/append", json=body)
            if r.status_code != 201:
                print(f"seed {i}: failed status={r.status_code} body={r.text[:120]}")
            else:
                seq = r.json().get("seq")
                print(f"seeded seq={seq} actor={actor}")
    print(f"\nDone. Open {BASE_URL}/ to view the dashboard.")


if __name__ == "__main__":
    asyncio.run(main())
