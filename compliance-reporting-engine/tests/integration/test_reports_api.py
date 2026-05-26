"""Integration test: full reports API flow against the running ``app`` container.

This test ONLY runs inside the ``tester`` Docker container (where
``BASE_URL`` points at ``http://app:8000`` and ``DATABASE_URL`` points
at the same Postgres the app is wired to). Outside that environment
it skips, so a casual ``pytest tests/`` on the host doesn't try to
reach a phantom backend.

Flow under test:

  1. Seed ~200 SOX-tagged log events directly into Postgres via the
     same seeder + repository the production code uses (in-process —
     this is the simplest way to get rows in front of the running app).
  2. POST /reports/generate for SOX/JSON over a 30-day window.
  3. Poll GET /reports/{id} until ``state == "COMPLETED"`` (timeout
     30 s). The coordinator runs in the app container, so the test
     can only observe progress via the API.
  4. GET /reports/{id}/download — assert the response is non-empty,
     ``Content-Type: application/json``, and parses back to a dict
     whose ``framework`` field matches.
  5. GET /reports/{id}/verify — assert ``verified=True``.

The seeder seeds across the SAME 30-day window the report queries,
so the report has rows to aggregate even on a freshly-up DB.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from src.logs.repository import insert_log_events
from src.logs.seeder import generate_synthetic_logs
from src.persistence.db import init_db, make_engine, make_session_factory


# These tests only make sense against the running ``app`` container.
# When BASE_URL isn't set (e.g. host ``pytest``), skip the module.
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
async def seeded_window() -> AsyncIterator[tuple[datetime, datetime]]:
    """Seed ~200 SOX-tagged events into the live DB; return the window.

    The seeder is deterministic (``seed=4242``) so the integration
    test reads the same population every run. We deliberately use a
    seed that's distinct from any other test/script so we don't
    collide with rows seeded by a parallel run.
    """
    period_end = datetime.now(timezone.utc).replace(microsecond=0)
    period_start = period_end - timedelta(days=30)

    engine = make_engine(DATABASE_URL)
    session_factory = make_session_factory(engine)
    try:
        await init_db(engine)
        events = generate_synthetic_logs(
            200,
            frameworks=["SOX"],
            seed=4242,
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
    timeout_s: float = 30.0,
) -> dict:
    """Poll GET /reports/{id} until state == target or timeout."""
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


async def test_full_report_flow_sox_json(client, seeded_window) -> None:
    """Generate -> poll -> download -> verify for a SOX/JSON report."""
    period_start, period_end = seeded_window

    # --- 1. POST /reports/generate ---
    create_body = {
        "framework": "SOX",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "export_format": "JSON",
        "title": "Integration Test SOX",
    }
    r = await client.post("/reports/generate", json=create_body)
    assert r.status_code == 202, r.text
    create_resp = r.json()
    report_id = create_resp["report_id"]
    assert create_resp["state"] == "PENDING"

    # --- 2. Poll until COMPLETED ---
    final = await _wait_for_state(client, report_id, target="COMPLETED")
    assert final["framework"] == "SOX"
    assert final["export_format"] == "JSON"
    assert final["signature_hex"] is not None
    assert final["download_url"] == f"/reports/{report_id}/download"

    # --- 3. GET /reports/{id}/download ---
    dl = await client.get(f"/reports/{report_id}/download")
    assert dl.status_code == 200, dl.text
    assert dl.headers.get("content-type", "").startswith("application/json")
    # Non-empty, and the JSON body has the framework we asked for.
    body_bytes = dl.content
    assert len(body_bytes) > 0
    parsed = json.loads(body_bytes.decode("utf-8"))
    assert parsed["framework"] == "SOX"
    assert "summary" in parsed
    assert "data" in parsed

    # --- 4. GET /reports/{id}/verify ---
    v = await client.get(f"/reports/{report_id}/verify")
    assert v.status_code == 200, v.text
    verify_body = v.json()
    assert verify_body["verified"] is True
    assert verify_body["signature_hex"] == final["signature_hex"]
    # No secondary signature for SOX (FinHealth-only).
    assert verify_body["signature_secondary_hex"] is None
    assert verify_body["secondary_verified"] is None
