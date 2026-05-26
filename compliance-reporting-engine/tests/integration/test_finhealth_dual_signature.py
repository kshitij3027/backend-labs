"""Integration test: FinHealth dual-signature flow against the running ``app`` container.

This test ONLY runs inside the ``tester`` Docker container (where
``BASE_URL`` points at ``http://app:8000`` and ``DATABASE_URL`` points
at the same Postgres the app is wired to). Outside that environment
it skips.

Flow under test:

  1. Seed ~500 events into Postgres tagged with SOX + HIPAA so the
     FinHealth composite has something to aggregate.
  2. POST /reports/generate for FINHEALTH/JSON over a 30-day window.
  3. Poll GET /reports/{id} until ``state == "COMPLETED"``.
  4. GET /reports/{id}/verify and assert BOTH ``verified=True`` AND
     ``secondary_verified=True``, plus both signature hex strings are
     populated (non-null).
  5. Tamper the DB row's ``signature_secondary_hex`` directly, GET
     /verify again, assert ``secondary_verified=False`` while the
     primary ``verified=True`` (we didn't touch the primary signature
     or the underlying log events, so the primary should still pass).
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import update

from src.logs.repository import insert_log_events
from src.logs.seeder import generate_synthetic_logs
from src.persistence.db import init_db, make_engine, make_session_factory
from src.persistence.models import Report


# These tests only make sense against the running ``app`` container.
BASE_URL = os.environ.get("BASE_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not BASE_URL or not DATABASE_URL,
    reason=(
        "integration test requires BASE_URL + DATABASE_URL — run via "
        "docker compose --profile test run --rm tester."
    ),
)


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """An ``AsyncClient`` aimed at the running ``app`` container."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as ac:
        yield ac


@pytest_asyncio.fixture
async def seeded_finhealth_window() -> AsyncIterator[tuple[datetime, datetime]]:
    """Seed ~500 SOX+HIPAA-tagged events into the live DB; return the window.

    We seed across both SOX and HIPAA so the FinHealth composite query
    (which matches events tagged with EITHER framework) has plenty to
    aggregate. The seed is distinct from other integration tests so
    parallel runs don't collide.
    """
    period_end = datetime.now(timezone.utc).replace(microsecond=0)
    period_start = period_end - timedelta(days=30)

    engine = make_engine(DATABASE_URL)
    session_factory = make_session_factory(engine)
    try:
        await init_db(engine)
        events = generate_synthetic_logs(
            500,
            frameworks=["SOX", "HIPAA"],
            seed=7117,
            period_start=period_start,
            period_end=period_end,
        )
        async with session_factory() as session:
            await insert_log_events(session, events)
            await session.commit()
        yield period_start, period_end
    finally:
        await engine.dispose()


async def _wait_for_state(
    client: httpx.AsyncClient,
    report_id: str,
    *,
    target: str = "COMPLETED",
    timeout_s: float = 60.0,
) -> dict:
    """Poll GET /reports/{id} until ``state == target`` or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    last_body: dict = {}
    while asyncio.get_event_loop().time() < deadline:
        r = await client.get(f"/reports/{report_id}")
        assert r.status_code == 200, r.text
        body = r.json()
        last_body = body
        if body["state"] == target:
            return body
        if body["state"] == "FAILED":
            pytest.fail(f"report failed: {body}")
        await asyncio.sleep(0.5)
    pytest.fail(
        f"report {report_id} did not reach {target} within {timeout_s}s; "
        f"last seen: {last_body}"
    )


async def _set_signature_secondary_hex(
    report_id: UUID,
    new_value: str,
) -> None:
    """Tamper with ``signature_secondary_hex`` directly via SQLAlchemy.

    Mirrors the DB-mutation pattern used by other integration tests:
    open a short-lived async engine against the same Postgres, update
    the row, dispose. We don't want the running app's connection pool
    to cache the pre-tamper value, so we open a brand-new engine each
    time rather than reusing one.
    """
    engine = make_engine(DATABASE_URL)
    session_factory = make_session_factory(engine)
    try:
        async with session_factory() as session:
            await session.execute(
                update(Report)
                .where(Report.id == report_id)
                .values(signature_secondary_hex=new_value)
            )
            await session.commit()
    finally:
        await engine.dispose()


async def test_finhealth_dual_signature_flow(client, seeded_finhealth_window) -> None:
    """Generate a FinHealth report, verify both signatures, then tamper the secondary."""
    period_start, period_end = seeded_finhealth_window

    # --- 1. POST /reports/generate ---
    create_body = {
        "framework": "FINHEALTH",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "export_format": "JSON",
        "title": "Integration Test FinHealth Dual-Sign",
    }
    r = await client.post("/reports/generate", json=create_body)
    assert r.status_code == 202, r.text
    create_resp = r.json()
    report_id = create_resp["report_id"]
    assert create_resp["state"] == "PENDING"

    # --- 2. Poll until COMPLETED ---
    final = await _wait_for_state(client, report_id, target="COMPLETED")
    assert final["framework"] == "FINHEALTH"
    assert final["export_format"] == "JSON"
    # Both signatures should be populated on a FinHealth row.
    assert final["signature_hex"] is not None
    assert final["signature_secondary_hex"] is not None
    # Hex strings are 64 chars for SHA-256 — sanity-check the shape.
    assert len(final["signature_hex"]) == 64
    assert len(final["signature_secondary_hex"]) == 64
    assert final["signature_hex"] != final["signature_secondary_hex"]

    # --- 3. GET /reports/{id}/verify (untampered) ---
    v = await client.get(f"/reports/{report_id}/verify")
    assert v.status_code == 200, v.text
    body = v.json()
    assert body["verified"] is True
    assert body["secondary_verified"] is True
    assert body["signature_hex"] == final["signature_hex"]
    assert body["signature_secondary_hex"] == final["signature_secondary_hex"]

    # --- 4. Tamper the secondary signature directly in the DB ---
    # Flip the first hex digit so the stored signature no longer matches
    # the recomputed one. We keep the same length so the column constraint
    # (String(128)) isn't a confounder for the test result.
    tampered = ("0" if final["signature_secondary_hex"][0] != "0" else "f") + (
        final["signature_secondary_hex"][1:]
    )
    assert tampered != final["signature_secondary_hex"]
    await _set_signature_secondary_hex(UUID(report_id), tampered)

    # --- 5. GET /verify (after tamper) ---
    v2 = await client.get(f"/reports/{report_id}/verify")
    assert v2.status_code == 200, v2.text
    body2 = v2.json()
    # Primary stays good — we didn't touch its hex or the underlying events.
    assert body2["verified"] is True
    assert body2["signature_hex"] == final["signature_hex"]
    # Secondary now mismatches the recomputed digest.
    assert body2["secondary_verified"] is False
    assert body2["signature_secondary_hex"] == tampered
