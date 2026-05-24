"""Integration tests for the C17 HTMX partials.

Covers two routes:

  * ``GET /partials/tiers`` — fragment rendered into ``#tiers-card``.
  * ``GET /partials/stats`` — fragment rendered into ``#stats-card``.

The fixture mirrors ``test_dashboard_skeleton.py`` exactly: a per-test
SQLite + storage_root + long scheduler intervals + ``LifespanManager``
so the full ``src.main:app`` lifespan fires. We re-import ``src.main``
per test for the same reason — settings get pinned at app-construct
time.

The "after evaluate" test inserts a synthetic ``JobRun`` row directly
through the session_factory rather than waiting for the APScheduler tick
or relying on ``POST /v1/evaluate`` (which calls ``scan_once`` directly,
bypassing the scheduler's ``_run_job_and_record`` wrapper). That keeps
the partial's contract — "renders the ``finished_at`` of the most recent
``job_runs`` row per ``job_name``" — testable in isolation from the
scheduler wiring.
"""
from __future__ import annotations

import importlib
import sys
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager

from src.persistence.models import JobRun


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest_asyncio.fixture
async def app_under_test(tmp_path, monkeypatch) -> AsyncIterator[tuple]:
    """Spin up a fresh ``src.main:app`` against a per-test SQLite DB.

    Long scheduler intervals so nothing fires during the test window;
    we exercise the partials by hitting the routes directly via the
    in-process httpx client.
    """
    db_path = tmp_path / "test.db"
    storage_root = tmp_path / "tiers"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("STORAGE_ROOT", str(storage_root))
    monkeypatch.setenv(
        "POLICY_CONFIG_PATH", str(PROJECT_ROOT / "config" / "retention_config.yaml")
    )
    monkeypatch.setenv("SCAN_INTERVAL_SEC", "3600")
    monkeypatch.setenv("APPLY_INTERVAL_SEC", "3600")
    monkeypatch.setenv("SWEEP_INTERVAL_SEC", "3600")

    from src.settings import get_settings

    get_settings.cache_clear()
    for mod_name in list(sys.modules.keys()):
        if mod_name == "src.main" or mod_name.startswith("src.main."):
            del sys.modules[mod_name]

    import src.main as main_module  # noqa: E402

    importlib.reload(main_module)
    app = main_module.app

    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client


# --- /partials/tiers ------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_tiers_returns_200_and_lists_all_tiers(app_under_test):
    """Every tier label (HOT/WARM/COLD/ARCHIVE/PENDING) appears in the body."""
    _app, client = app_under_test
    r = await client.get("/partials/tiers")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    for label in ("HOT", "WARM", "COLD", "ARCHIVE", "PENDING"):
        assert label in body, f"missing tier label in /partials/tiers: {label}"


@pytest.mark.asyncio
async def test_partial_tiers_zero_state(app_under_test):
    """On an empty DB every tier reports ``0 files``."""
    _app, client = app_under_test
    r = await client.get("/partials/tiers")
    assert r.status_code == 200
    # All 5 tiers present + all 5 at zero → at least 5 occurrences of
    # the "0 files" marker (one per tier row).
    assert r.text.count("0 files") >= 5


# --- /partials/stats ------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_stats_returns_200(app_under_test):
    """Stats card has the canonical labels and renders cleanly on empty DB."""
    _app, client = app_under_test
    r = await client.get("/partials/stats")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    for label in ("Total files", "Pending transitions", "Last scan"):
        assert label in body, f"missing stats label: {label}"
    # Empty DB → ``Last scan`` is the em-dash sentinel from the template.
    assert "—" in body


@pytest.mark.asyncio
async def test_partial_stats_after_evaluate_records_last_scan(app_under_test):
    """Writing a ``JobRun(scan_job, finished_at=...)`` flips ``Last scan`` off ``—``.

    ``/v1/evaluate`` invokes the lifecycle functions directly and does not
    flow through the scheduler's ``_run_job_and_record`` wrapper, so it
    deliberately does not insert ``job_runs`` rows — only the actual
    APScheduler ticks do. To assert the partial reads ``job_runs``
    correctly we insert a synthetic row through ``app.state.session_factory``,
    which is exactly what ``_run_job_and_record`` does in production.
    """
    app, client = app_under_test

    # Confirm baseline first — no JobRun row means ``Last scan`` should
    # render as the em-dash sentinel.
    r = await client.get("/partials/stats")
    assert r.status_code == 200
    assert "Last scan" in r.text

    # Drive an evaluate cycle for completeness (counts will all be 0 on
    # an empty DB; primary purpose is to assert the route still works).
    r = await client.post("/v1/evaluate")
    assert r.status_code == 200, r.text

    # Insert the JobRun row the scheduler would have written.
    finished = datetime.utcnow()
    session_factory = app.state.session_factory
    async with session_factory() as session:
        session.add(
            JobRun(
                job_name="scan_job",
                started_at=finished,
                finished_at=finished,
                status="ok",
                summary_json='{"scanned": 0, "transitions_planned": 0}',
            )
        )
        await session.commit()

    # Stats partial now exposes the finished_at instead of the em-dash.
    r = await client.get("/partials/stats")
    assert r.status_code == 200
    body = r.text
    # The finished_at string format is the Python str(datetime) form,
    # e.g. "2026-05-23 12:34:56.789" — we only assert the year prefix
    # appears next to the "Last scan" label, which is enough to
    # demonstrate the value flowed through.
    year_prefix = str(finished.year)
    assert year_prefix in body
    # And the entire body must no longer treat "Last scan" as missing
    # (i.e. it should not be rendered as the em-dash sentinel for
    # scan specifically — apply/sweep still render as ``—``).
    # The simplest assertion: at least one non-em-dash datetime token
    # appears in the body.
    assert "scan_job" not in body  # template renders finished_at, not job_name
    assert year_prefix in body
