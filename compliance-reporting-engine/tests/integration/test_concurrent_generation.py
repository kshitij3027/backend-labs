"""Integration test: 5 concurrent /reports/generate requests reach COMPLETED.

Verifies the "5 concurrent report generations complete without errors"
acceptance criterion from project_requirements.md §5. Only runs inside
the ``tester`` Docker container where ``BASE_URL`` points at the live
``app`` service and ``DATABASE_URL`` points at the same Postgres the
app is wired to.

Flow:
  1. Seed ~500 events across all configured frameworks so each report
     has data to aggregate.
  2. POST 5 concurrent /reports/generate (mix of frameworks × formats)
     via ``asyncio.gather`` against a single ``httpx.AsyncClient``.
  3. Poll each ``/reports/{id}`` until COMPLETED or FAILED, bounded by a
     90s budget so a stuck coordinator surfaces as a test failure rather
     than a CI timeout.
  4. Assert every report reached COMPLETED and that no FAILED states
     leaked through (a single FAILED is enough to fail the test).
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from src.logs.repository import insert_log_events
from src.logs.seeder import generate_synthetic_logs
from src.persistence.db import init_db, make_engine, make_session_factory


BASE_URL = os.environ.get("BASE_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not BASE_URL or not DATABASE_URL,
    reason=(
        "integration test requires BASE_URL + DATABASE_URL — run via "
        "docker compose --profile test run --rm tester."
    ),
)


# 5 concurrent generate requests — one per framework. Mix the formats so
# the test also exercises each exporter under contention; PDF tends to
# be the slowest, so include it.
_REQUESTS: list[tuple[str, str]] = [
    ("SOX", "JSON"),
    ("HIPAA", "CSV"),
    ("PCI_DSS", "XML"),
    ("GDPR", "PDF"),
    ("FINHEALTH", "JSON"),
]


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """``AsyncClient`` aimed at the running ``app`` container."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as ac:
        yield ac


@pytest_asyncio.fixture
async def seeded_window() -> AsyncIterator[tuple[datetime, datetime]]:
    """Seed ~500 events across SOX+HIPAA+PCI_DSS+GDPR; return the window.

    FinHealth's repository query matches any SOX or HIPAA tag so the
    same seed population covers all five concurrent reports.
    """
    period_end = datetime.now(timezone.utc).replace(microsecond=0)
    period_start = period_end - timedelta(days=30)

    engine = make_engine(DATABASE_URL)
    session_factory = make_session_factory(engine)
    try:
        await init_db(engine)
        events = generate_synthetic_logs(
            500,
            frameworks=["SOX", "HIPAA", "PCI_DSS", "GDPR"],
            seed=9999,
            period_start=period_start,
            period_end=period_end,
        )
        async with session_factory() as session:
            await insert_log_events(session, events)
            await session.commit()
        yield period_start, period_end
    finally:
        await engine.dispose()


async def _post_generate(
    client: httpx.AsyncClient,
    framework: str,
    fmt: str,
    period_start: datetime,
    period_end: datetime,
) -> str:
    """POST one generate request and return its report_id."""
    body = {
        "framework": framework,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "export_format": fmt,
        "title": f"Concurrent {framework} ({fmt})",
    }
    r = await client.post("/reports/generate", json=body)
    assert r.status_code == 202, r.text
    return r.json()["report_id"]


async def _wait_terminal(
    client: httpx.AsyncClient,
    report_id: str,
    *,
    timeout_s: float = 90.0,
) -> dict:
    """Poll until state in ``{COMPLETED, FAILED}`` or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    last: dict = {}
    while asyncio.get_event_loop().time() < deadline:
        r = await client.get(f"/reports/{report_id}")
        assert r.status_code == 200, r.text
        body = r.json()
        last = body
        if body["state"] in ("COMPLETED", "FAILED"):
            return body
        await asyncio.sleep(0.5)
    pytest.fail(
        f"report {report_id} did not reach terminal state within {timeout_s}s; "
        f"last seen: {last}"
    )


async def test_five_concurrent_reports_all_complete(client, seeded_window) -> None:
    """5 concurrent generate requests all reach COMPLETED."""
    period_start, period_end = seeded_window

    # Fire 5 POSTs concurrently so they all hit the coordinator's
    # semaphore at roughly the same instant.
    report_ids = await asyncio.gather(
        *[
            _post_generate(client, fw, fmt, period_start, period_end)
            for fw, fmt in _REQUESTS
        ]
    )
    assert len(report_ids) == 5
    assert len(set(report_ids)) == 5, "expected 5 distinct report_ids"

    # Then wait for all 5 to settle in parallel.
    finals = await asyncio.gather(
        *[_wait_terminal(client, rid) for rid in report_ids]
    )

    states = [f["state"] for f in finals]
    failed = [f for f in finals if f["state"] == "FAILED"]
    assert not failed, f"one or more reports failed: {failed}"
    assert all(s == "COMPLETED" for s in states), f"states: {states}"

    # Sanity: every COMPLETED row should have a signature.
    for f in finals:
        assert f["signature_hex"], f"missing signature on {f['report_id']}"
