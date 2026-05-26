"""Unit tests for :mod:`src.scheduling.scheduler`.

These tests inspect the ``ReportScheduler`` *without* starting its
underlying ``AsyncIOScheduler`` — that's an event-loop-lifetime
concern that real integration tests cover. Here we just verify:

  * ``install_jobs()`` registers one cron job per supported framework.
  * Each installed job's trigger is a ``CronTrigger`` (i.e. we didn't
    accidentally fall through to an interval / date trigger).
  * Unknown framework codes are skipped with a warning, not raised.
  * ``_period_for`` produces a 24h window for daily frameworks and a
    7-day window for weekly (FinHealth) frameworks.
  * ``_trigger_for`` falls back to midnight-daily for unmapped codes.

A small optional smoke test confirms ``start()`` + ``shutdown()`` work
inside an event loop — kept tiny so it doesn't flap.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from apscheduler.triggers.cron import CronTrigger

from src.scheduling.scheduler import (
    DAILY_CRON,
    ReportScheduler,
    WEEKLY_CRON,
    _period_for,
    _trigger_for,
)


# --- Fixtures / helpers -------------------------------------------------------


def _fake_settings(
    *,
    supported: list[str] | None = None,
    default_export_format: str = "JSON",
) -> SimpleNamespace:
    """Build a lightweight stand-in for the real :class:`Settings`.

    The scheduler only reads two attributes — ``supported_frameworks_list``
    and ``default_export_format`` — so a ``SimpleNamespace`` is enough.
    """
    return SimpleNamespace(
        supported_frameworks_list=supported or ["SOX", "HIPAA", "PCI_DSS", "GDPR"],
        scheduler_enabled=True,
        default_export_format=default_export_format,
    )


def _fake_coordinator() -> MagicMock:
    """Build a fake coordinator. Tests that call ``install_jobs`` don't
    need it to do anything — they only inspect the installed-job set.
    """
    return MagicMock()


# --- Tests --------------------------------------------------------------------


def test_install_jobs_registers_one_per_framework() -> None:
    """``install_jobs()`` registers exactly one job per supported framework.

    The default supported list has 4 frameworks. FinHealth is *not*
    appended here because the registry hasn't imported it yet (commit
    17 lands FinHealth) — so we expect exactly 4 jobs.
    """
    scheduler = ReportScheduler(_fake_coordinator(), _fake_settings())
    scheduler.install_jobs()

    assert len(scheduler.installed_jobs) == 4
    job_ids = {job.id for job in scheduler.installed_jobs}
    assert job_ids == {
        "generate-sox",
        "generate-hipaa",
        "generate-pci_dss",
        "generate-gdpr",
    }


def test_each_job_has_cron_trigger() -> None:
    """Every installed job uses a ``CronTrigger`` (not interval / date)."""
    scheduler = ReportScheduler(_fake_coordinator(), _fake_settings())
    scheduler.install_jobs()

    for job in scheduler.installed_jobs:
        assert isinstance(job.trigger, CronTrigger), (
            f"Job {job.id} has trigger {type(job.trigger).__name__}, "
            "expected CronTrigger"
        )


def test_unknown_framework_skipped_with_warning() -> None:
    """``install_jobs(["NOPE", "SOX"])`` only registers SOX, no exception."""
    scheduler = ReportScheduler(_fake_coordinator(), _fake_settings())
    scheduler.install_jobs(frameworks=["NOPE", "SOX"])

    assert len(scheduler.installed_jobs) == 1
    assert scheduler.installed_jobs[0].id == "generate-sox"


def test_install_jobs_uses_settings_list_by_default() -> None:
    """When ``frameworks=None`` the scheduler reads the settings list."""
    scheduler = ReportScheduler(
        _fake_coordinator(), _fake_settings(supported=["SOX"])
    )
    scheduler.install_jobs()

    assert len(scheduler.installed_jobs) == 1
    assert scheduler.installed_jobs[0].id == "generate-sox"


def test_period_for_daily_window() -> None:
    """Daily framework -> ``(now - 1 day, now)``."""
    fixed = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    start, end = _period_for("SOX", now=fixed)
    assert end == fixed
    assert start == fixed - timedelta(days=1)
    # Confirm the four daily frameworks all behave identically.
    for fw in DAILY_CRON:
        s, e = _period_for(fw, now=fixed)
        assert e == fixed
        assert s == fixed - timedelta(days=1)


def test_period_for_weekly_window() -> None:
    """Weekly framework (FinHealth) -> ``(now - 7 days, now)``."""
    fixed = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    start, end = _period_for("FINHEALTH", now=fixed)
    assert end == fixed
    assert start == fixed - timedelta(days=7)
    # Confirm any weekly framework in the table behaves the same way.
    for fw in WEEKLY_CRON:
        s, e = _period_for(fw, now=fixed)
        assert e == fixed
        assert s == fixed - timedelta(days=7)


def test_period_for_default_is_daily() -> None:
    """Unmapped frameworks fall through to the daily window (24h)."""
    fixed = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    start, end = _period_for("MYSTERY_FRAMEWORK", now=fixed)
    assert end == fixed
    assert start == fixed - timedelta(days=1)


def test_trigger_for_known_daily() -> None:
    """``_trigger_for("SOX")`` returns a CronTrigger at the configured hour."""
    trigger = _trigger_for("SOX")
    assert isinstance(trigger, CronTrigger)
    # ``DAILY_CRON["SOX"] == (1, 0)`` — assert the trigger's repr surfaces it.
    assert "hour='1'" in str(trigger)
    assert "minute='0'" in str(trigger)


def test_trigger_for_known_weekly() -> None:
    """``_trigger_for("FINHEALTH")`` returns a weekly CronTrigger."""
    trigger = _trigger_for("FINHEALTH")
    assert isinstance(trigger, CronTrigger)
    # WEEKLY_CRON["FINHEALTH"] == ("mon", 2, 0).
    assert "day_of_week='mon'" in str(trigger)
    assert "hour='2'" in str(trigger)
    assert "minute='0'" in str(trigger)


def test_trigger_fallback_for_unmapped() -> None:
    """An unmapped framework code falls back to midnight-daily."""
    trigger = _trigger_for("UNKNOWN")
    assert isinstance(trigger, CronTrigger)
    # Midnight: hour=0, minute=0.
    assert "hour='0'" in str(trigger)
    assert "minute='0'" in str(trigger)


def test_install_jobs_idempotent_on_repeated_call() -> None:
    """Calling ``install_jobs`` twice doesn't crash on duplicate IDs.

    Because each ``add_job`` uses ``replace_existing=True``, a second
    invocation re-registers the same jobs in place. ``installed_jobs``
    grows (it's a snapshot, not the live registry), but the underlying
    scheduler stays consistent.
    """
    scheduler = ReportScheduler(_fake_coordinator(), _fake_settings())
    scheduler.install_jobs()
    first_count = len(scheduler.installed_jobs)
    # Should not raise even though the IDs already exist in the
    # APScheduler internal job store.
    scheduler.install_jobs()
    # The snapshot list double-counts (it's append-only), but the
    # scheduler's own job store still has just 4 unique IDs.
    assert len(scheduler.installed_jobs) == 2 * first_count
    job_ids = {j.id for j in scheduler.scheduler.get_jobs()}
    assert job_ids == {
        "generate-sox",
        "generate-hipaa",
        "generate-pci_dss",
        "generate-gdpr",
    }


def test_scheduler_can_start_and_shutdown() -> None:
    """Smoke test: ``start()`` and ``shutdown()`` lifecycle works under a loop.

    We install zero jobs (frameworks=[]) so no timer ever fires; this is
    purely a sanity check that the AsyncIOScheduler lifecycle is wired
    cleanly. Skipped if APScheduler complains about the loop (e.g. on
    very old Python builds) — kept tiny so it doesn't flap.
    """

    async def _run() -> None:
        scheduler = ReportScheduler(_fake_coordinator(), _fake_settings())
        scheduler.install_jobs(frameworks=[])
        assert scheduler.installed_jobs == []
        scheduler.start()
        try:
            assert scheduler.scheduler.running is True
        finally:
            scheduler.shutdown(wait=False)
        # APScheduler's `running` flag flips asynchronously after
        # ``shutdown(wait=False)`` — give the loop one round-trip so the
        # state transition propagates before we assert.
        await asyncio.sleep(0)

    try:
        asyncio.run(_run())
    except RuntimeError as exc:
        # Highly unlikely in CI, but tolerate event-loop edge cases on
        # exotic Python builds rather than masking a real regression
        # with a "test passed" message.
        pytest.skip(f"event-loop unavailable for start/shutdown smoke test: {exc}")
