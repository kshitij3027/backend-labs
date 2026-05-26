"""APScheduler wrapper that fires one periodic generate job per framework.

Jobs are installed at startup if ``SCHEDULER_ENABLED=true``. Each job:

  1. Computes the report period (last 24 hours for daily frameworks,
     last 7 days for weekly frameworks like FinHealth).
  2. Inserts a fresh ``Report`` row with ``state="PENDING"``.
  3. Schedules ``coordinator.generate(report_id)`` as a fire-and-forget
     asyncio task — the coordinator's semaphore handles backpressure
     if jobs overlap.

We use :class:`apscheduler.schedulers.asyncio.AsyncIOScheduler` so jobs
run on the same event loop as the FastAPI app; no thread bridging is
needed for the async coordinator.

The scheduler is intentionally *not* started inside ``__init__`` — call
:meth:`ReportScheduler.start` after :meth:`install_jobs` so the
FastAPI lifespan can compose the lifecycle explicitly and reject
misconfigurations before any timer fires.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from ..logging_config import get_logger
from ..persistence.models import Report

logger = get_logger("scheduling.scheduler")


# Per-framework cadence configuration.
#
# ``DAILY_CRON`` values are ``(hour, minute)`` pairs — the job fires
# every day at that wall-clock time. ``WEEKLY_CRON`` values are
# ``(day_of_week, hour, minute)`` triples — the job fires once per week
# on that weekday.
#
# FinHealth gets a weekly cadence per the plan; the four primary
# frameworks (SOX, HIPAA, PCI_DSS, GDPR) get daily cadences staggered
# by 15 minutes so they don't all hammer the DB at the same instant.
DAILY_CRON: dict[str, tuple[int, int]] = {
    "SOX": (1, 0),
    "HIPAA": (1, 15),
    "PCI_DSS": (1, 30),
    "GDPR": (1, 45),
}
WEEKLY_CRON: dict[str, tuple[str, int, int]] = {
    "FINHEALTH": ("mon", 2, 0),  # Mondays at 02:00 UTC
}


def _trigger_for(framework: str) -> CronTrigger:
    """Pick a ``CronTrigger`` based on the framework's configured cadence.

    Falls back to midnight-daily for any future framework not in the
    daily / weekly tables — that way a freshly-added rule module won't
    silently never run; it just runs at midnight until the operator
    adds an explicit cadence entry.
    """
    if framework in DAILY_CRON:
        hour, minute = DAILY_CRON[framework]
        return CronTrigger(hour=hour, minute=minute)
    if framework in WEEKLY_CRON:
        day_of_week, hour, minute = WEEKLY_CRON[framework]
        return CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute)
    # Fallback: midnight daily.
    return CronTrigger(hour=0, minute=0)


def _period_for(
    framework: str, *, now: datetime | None = None
) -> tuple[datetime, datetime]:
    """Compute the ``(period_start, period_end)`` window for a scheduled job.

    Daily frameworks cover the previous 24 hours (yesterday). Weekly
    frameworks (FinHealth today) cover the previous 7 days. ``now`` is
    accepted as an argument purely so tests can pin time without having
    to monkey-patch ``datetime.now``.
    """
    now = now or datetime.now(timezone.utc)
    if framework in WEEKLY_CRON:
        return now - timedelta(days=7), now
    return now - timedelta(days=1), now


class ReportScheduler:
    """Owns an AsyncIOScheduler with one cron job per registered framework.

    The scheduler is intentionally NOT started in :meth:`__init__` —
    call :meth:`start` after :meth:`install_jobs` so the FastAPI
    lifespan can compose the lifecycle explicitly and reject
    misconfigurations before any timer fires.

    Each fired job:

      * Opens its own session via the coordinator's session factory
        (decoupled from any request-scoped session).
      * Inserts a fresh ``Report`` row with ``state="PENDING"`` and the
        computed period window.
      * Hands off to ``coordinator.generate(report_id)`` via
        ``asyncio.create_task(...)`` — fire-and-forget; the
        coordinator's semaphore handles backpressure if scheduled
        runs overlap (e.g. an unusually slow generate still in flight
        when the next cron tick lands).
    """

    def __init__(self, coordinator, settings) -> None:
        self.coordinator = coordinator
        self.settings = settings
        self.scheduler = AsyncIOScheduler()
        # Snapshot of installed jobs for easier inspection / testing.
        # Populated by :meth:`install_jobs`. Kept as a plain list (not a
        # dict) so the iteration order matches the install order — handy
        # for log readability.
        self.installed_jobs: list = []

    def install_jobs(self, frameworks: Optional[list[str]] = None) -> None:
        """Register one cron job per framework.

        Args:
            frameworks: Optional override. By default the scheduler
                installs jobs for every framework in
                ``settings.supported_frameworks_list``. FinHealth is
                appended automatically if it's registered in
                ``FRAMEWORK_REGISTRY`` (regardless of whether the
                operator listed it in ``SUPPORTED_FRAMEWORKS``), since
                it has its own weekly cadence and a dedicated dashboard
                widget.

        Unknown framework codes are skipped with a warning rather than
        raising — that way a typo in ``SUPPORTED_FRAMEWORKS`` doesn't
        wedge the whole scheduler.
        """
        # Import here to avoid the circular FRAMEWORK_REGISTRY <->
        # persistence path at module-load time. The registry is
        # populated as a side effect of importing ``src.frameworks``,
        # which (transitively) imports persistence — keeping this
        # import local sidesteps the cycle.
        from ..frameworks import FRAMEWORK_REGISTRY

        if frameworks is None:
            frameworks = list(self.settings.supported_frameworks_list)
        # Tack FinHealth on if it's registered but not already in the
        # operator-supplied list. FinHealth has its own weekly cadence
        # and lives outside the daily-frameworks cluster, so it
        # wouldn't typically show up in ``SUPPORTED_FRAMEWORKS``.
        if "FINHEALTH" in FRAMEWORK_REGISTRY and "FINHEALTH" not in frameworks:
            frameworks.append("FINHEALTH")

        for framework in frameworks:
            if framework not in FRAMEWORK_REGISTRY:
                logger.warning(
                    "scheduler_skipped_unknown_framework",
                    framework=framework,
                )
                continue
            trigger = _trigger_for(framework)
            job = self.scheduler.add_job(
                self._run_one,
                trigger=trigger,
                args=[framework],
                id=f"generate-{framework.lower()}",
                replace_existing=True,
            )
            self.installed_jobs.append(job)
            logger.info(
                "scheduler_job_installed",
                framework=framework,
                trigger=str(trigger),
                job_id=job.id,
            )

    async def _run_one(self, framework: str) -> None:
        """Insert a Report row then dispatch the coordinator's generate task.

        Runs in two distinct phases so the row commit doesn't hold a
        session open across the (potentially long) generation work:

          1. Open a session, insert a ``PENDING`` Report row, commit.
          2. Schedule ``coordinator.generate(report_id)`` as a
             fire-and-forget task. The coordinator opens its *own*
             session and handles transitions / persistence from there.
        """
        period_start, period_end = _period_for(framework)
        async with self.coordinator.session_factory() as session:
            async with session.begin():
                report = Report(
                    framework=framework,
                    period_start=period_start,
                    period_end=period_end,
                    export_format=self.settings.default_export_format,
                    state="PENDING",
                    title=f"Scheduled {framework} report",
                    description=(
                        f"Auto-generated {framework} report for "
                        f"{period_start.date()} -> {period_end.date()}"
                    ),
                )
                session.add(report)
                await session.flush()
                report_id = report.id
        # Fire-and-forget: the coordinator's semaphore handles
        # concurrency. We *don't* await the task here because cron jobs
        # are expected to return promptly so the next tick can land
        # cleanly even if a slow generate is still running.
        asyncio.create_task(self.coordinator.generate(report_id))
        logger.info(
            "scheduler_job_fired",
            framework=framework,
            report_id=str(report_id),
        )

    def start(self) -> None:
        """Start the underlying ``AsyncIOScheduler``.

        Must be called from inside a running event loop (the FastAPI
        lifespan satisfies this). ``AsyncIOScheduler`` grabs the
        current loop at start time and dispatches job coroutines onto
        it from then on.
        """
        self.scheduler.start()
        logger.info("scheduler_started", jobs=len(self.installed_jobs))

    def shutdown(self, *, wait: bool = False) -> None:
        """Stop the scheduler if it's running.

        ``wait=False`` is the right default for the lifespan shutdown
        path — we don't want graceful shutdown to block on an in-flight
        generate that the coordinator is also tearing down.
        """
        if self.scheduler.running:
            self.scheduler.shutdown(wait=wait)
            logger.info("scheduler_shutdown", wait=wait)
