"""Generate ~5,000 synthetic log entries and bulk-ingest them via the API.

Usage (inside the api container):

    python scripts/seed_data.py

Environment overrides:

    API_URL          default http://api:8000
    SEED_USERNAME    default demo
    SEED_PASSWORD    default demo
    SEED_TOTAL       default 5000  (override for smaller/larger batches)
    SEED_BATCH_SIZE  default 200
"""

from __future__ import annotations

import os
import random
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx


SERVICES = [
    "payment-service",
    "auth-service",
    "order-service",
    "notification-service",
    "inventory-service",
]

# Weighted level distribution; weights sum to 1.0.
LEVELS: list[tuple[str, float]] = [
    ("DEBUG", 0.30),
    ("INFO", 0.40),
    ("WARN", 0.15),
    ("ERROR", 0.13),
    ("CRITICAL", 0.02),
]


PAYMENT_RESULTS = ["succeeded", "failed", "pending", "refunded", "authorized"]
PAYMENT_METHODS = ["card", "ach", "wallet", "wire", "crypto"]
AUTH_ACTIONS = ["logged in", "logged out", "failed login", "rotated token", "verified email"]
ORDER_STATES = ["created", "shipped", "cancelled", "delivered", "returned"]
NOTIFICATION_KINDS = ["email", "sms", "push", "webhook"]
NOTIFICATION_RESULTS = ["delivered", "deferred", "bounced", "queued"]
INVENTORY_ACTIONS = ["restocked", "sold out", "low stock", "reserved", "released"]


def _weighted_level(rng: random.Random) -> str:
    threshold = rng.random()
    cumulative = 0.0
    for level, weight in LEVELS:
        cumulative += weight
        if threshold <= cumulative:
            return level
    return LEVELS[-1][0]


def _random_timestamp(rng: random.Random, now: datetime) -> datetime:
    seconds = rng.randint(0, 7 * 24 * 60 * 60)
    return now - timedelta(seconds=seconds)


def _make_entry(rng: random.Random, now: datetime) -> dict[str, Any]:
    service = rng.choice(SERVICES)
    level = _weighted_level(rng)
    ts = _random_timestamp(rng, now)
    entry_id = f"seed-{uuid.uuid4().hex[:16]}"
    content: dict[str, Any] = {
        "request_id": uuid.uuid4().hex,
        "host": f"node-{rng.randint(1, 12):02d}",
    }

    if service == "payment-service":
        result = rng.choice(PAYMENT_RESULTS)
        order_id = f"ord-{rng.randint(1000, 99999)}"
        amount = round(rng.uniform(0.5, 9999.99), 2)
        message = f"Payment {result} for order {order_id}"
        content.update(
            {
                "amount": amount,
                "currency": rng.choice(["USD", "EUR", "GBP", "INR"]),
                "method": rng.choice(PAYMENT_METHODS),
                "order_id": order_id,
                "latency_ms": rng.randint(20, 1500),
            }
        )
    elif service == "auth-service":
        user_id = rng.randint(1, 5000)
        action = rng.choice(AUTH_ACTIONS)
        message = f"user {user_id} {action}"
        content.update(
            {
                "user_id": user_id,
                "ip": f"10.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(0,255)}",
                "latency_ms": rng.randint(5, 800),
            }
        )
    elif service == "order-service":
        order_id = f"ord-{rng.randint(1000, 99999)}"
        state = rng.choice(ORDER_STATES)
        items = rng.randint(1, 8)
        message = f"order {order_id} {state} ({items} items)"
        content.update(
            {
                "order_id": order_id,
                "items": items,
                "total": round(rng.uniform(5.0, 1500.0), 2),
                "latency_ms": rng.randint(15, 1200),
            }
        )
    elif service == "notification-service":
        kind = rng.choice(NOTIFICATION_KINDS)
        result = rng.choice(NOTIFICATION_RESULTS)
        recipient = f"recipient-{rng.randint(1, 3000)}"
        message = f"{kind} notification {result} for {recipient}"
        content.update(
            {
                "channel": kind,
                "result": result,
                "recipient": recipient,
                "latency_ms": rng.randint(10, 600),
            }
        )
    else:  # inventory-service
        sku = f"sku-{rng.randint(100, 9999)}"
        action = rng.choice(INVENTORY_ACTIONS)
        qty = rng.randint(0, 500)
        message = f"item {sku} {action} qty={qty}"
        content.update(
            {
                "sku": sku,
                "qty": qty,
                "warehouse": rng.choice(["wh-east", "wh-west", "wh-central"]),
                "latency_ms": rng.randint(5, 400),
            }
        )

    return {
        "id": entry_id,
        "timestamp": ts.isoformat(),
        "level": level,
        "service_name": service,
        "message": message,
        "content": content,
    }


def _fetch_token(client: httpx.Client, username: str, password: str) -> str:
    response = client.post(
        "/api/v1/auth/token",
        data={"username": username, "password": password},
        timeout=30.0,
    )
    if response.status_code != 200:
        print(
            f"FATAL: token request failed status={response.status_code} body={response.text}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    body = response.json()
    token = body.get("access_token")
    if not isinstance(token, str) or not token:
        print(f"FATAL: missing access_token in response: {body}", file=sys.stderr)
        raise SystemExit(2)
    return token


def main() -> int:
    api_url = os.getenv("API_URL", "http://api:8000").rstrip("/")
    username = os.getenv("SEED_USERNAME", "demo")
    password = os.getenv("SEED_PASSWORD", "demo")
    total = int(os.getenv("SEED_TOTAL", "5000"))
    batch_size = int(os.getenv("SEED_BATCH_SIZE", "200"))

    seed = int(os.getenv("SEED_RNG_SEED", "1337"))
    rng = random.Random(seed)
    now = datetime.now(UTC)

    print(
        f"seeding {total} synthetic entries to {api_url} as {username} "
        f"(batch_size={batch_size}, services={len(SERVICES)})",
        flush=True,
    )

    with httpx.Client(base_url=api_url, timeout=60.0) as client:
        token = _fetch_token(client, username, password)
        headers = {"Authorization": f"Bearer {token}"}

        sent = 0
        batch_no = 0
        while sent < total:
            count = min(batch_size, total - sent)
            entries = [_make_entry(rng, now) for _ in range(count)]
            batch_no += 1
            response = client.post(
                "/api/v1/logs/bulk",
                json={"entries": entries},
                headers=headers,
                timeout=120.0,
            )
            if response.status_code != 200:
                print(
                    f"FATAL: batch {batch_no} failed status={response.status_code} body={response.text[:400]}",
                    file=sys.stderr,
                )
                return 1
            body = response.json()
            sent += count
            print(
                f"batch {batch_no:02d}/{(total + batch_size - 1) // batch_size:02d} "
                f"sent={count} created={body.get('created', 0)} errors={body.get('errors', 0)} "
                f"total_so_far={sent}",
                flush=True,
            )

    print(f"done — {sent} entries seeded", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
