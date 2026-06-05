"""Seed a RUNNING optimizer with demo data so the dashboard shows live values.

Run this against a live server (``make up`` then ``python scripts/seed_demo.py``)
to populate it with a realistic spread of data: a few hundred rows across 2-3
tenants with a MIX of recent and old timestamps and varied fields, followed by a
spread of full-record and analytical queries. The result is a dashboard with
non-placeholder numbers — multiple tenants, a format distribution that drifts as
the background migration loop reformats the cold/scan-heavy partitions, and real
per-format query latency / pattern data.

Unlike the e2e + load scripts (which target the compose service name), this one
defaults to ``http://localhost:8000`` because it is meant for host use against a
published port. Override with ``APP_URL``. Runtime is a few seconds.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

import httpx

APP_URL = os.environ.get("APP_URL", "http://localhost:8000")

# Demo tenants — each gets a different recent/old/field mix so their per-tenant
# format decisions differ on the dashboard.
TENANTS: list[str] = ["acme", "globex", "initech"]

# Columns every entry carries; analytical projections cycle through these so the
# old partitions look scan-heavy + narrow and migrate to COLUMNAR.
_COLUMNS: list[str] = ["region", "status", "method", "path", "user", "latency_ms"]

# A small epoch so these rows land in a COLD time-bucket (far in the past).
_OLD_TS = 200_000.0
# Rows in the single OLD partition per tenant — above select_min_rows (256).
_OLD_ROWS = 320
# A smaller batch of recent rows so each tenant also has a HOT partition.
_RECENT_ROWS = 60

_HTTP_TIMEOUT = 30.0


def _entry(ts: float, i: int) -> dict:
    """One demo entry carrying all demo columns, at timestamp ``ts``."""
    return {
        "ts": ts,
        "fields": {
            "region": f"r{i % 4}",
            "status": (200, 404, 500)[i % 3],
            "method": ("GET", "POST", "PUT")[i % 3],
            "path": f"/api/v{i % 5}/resource",
            "user": f"u{i % 25}",
            "latency_ms": (i % 50) + 1,
        },
    }


async def _ingest(client: httpx.AsyncClient, tenant: str, ts: float, n: int) -> int:
    """Ingest ``n`` entries at ``ts`` for ``tenant``; return rows landed."""
    entries = [_entry(ts, i) for i in range(n)]
    resp = await client.post(
        f"{APP_URL}/api/ingest",
        json={"tenant": tenant, "entries": entries},
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return int(resp.json().get("ingested", 0))


async def _query_spread(client: httpx.AsyncClient, tenant: str) -> int:
    """Issue a spread of full-record + analytical queries; return query count.

    The analytical projections cycle through distinct single columns so the OLD
    partition's access pattern reads as scan-heavy + narrow — nudging the
    background migration loop to reformat it to COLUMNAR.
    """
    count = 0
    # A couple of full-record reads (drive the row-format latency series).
    for _ in range(2):
        resp = await client.post(
            f"{APP_URL}/api/query", json={"tenant": tenant}, timeout=_HTTP_TIMEOUT
        )
        resp.raise_for_status()
        count += 1
    # Analytical single-column projections over distinct columns.
    for col in _COLUMNS:
        resp = await client.post(
            f"{APP_URL}/api/query",
            json={"tenant": tenant, "columns": [col]},
            timeout=_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        count += 1
    return count


async def main() -> None:
    """Seed every demo tenant and print a short summary of what was done."""
    print(f"Seeding demo data -> {APP_URL}")
    async with httpx.AsyncClient() as client:
        # Verify the server is up before doing anything.
        try:
            health = await client.get(f"{APP_URL}/health", timeout=5.0)
            health.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR: server not reachable at {APP_URL}/health: {exc}")
            sys.exit(1)

        total_rows = 0
        total_queries = 0
        now = time.time()
        for tenant in TENANTS:
            old = await _ingest(client, tenant, _OLD_TS, _OLD_ROWS)
            recent = await _ingest(client, tenant, now, _RECENT_ROWS)
            queries = await _query_spread(client, tenant)
            total_rows += old + recent
            total_queries += queries
            print(
                f"  {tenant}: ingested {old} OLD-ts + {recent} recent rows, "
                f"ran {queries} queries"
            )

    print(
        f"Done: {total_rows} rows across {len(TENANTS)} tenants, "
        f"{total_queries} queries issued."
    )
    print(
        "The background migration loop will reformat the cold scan-heavy "
        "partitions to COLUMNAR within ~30s; watch the dashboard."
    )


if __name__ == "__main__":
    asyncio.run(main())
