"""Manage long-running ExperimentEngine.run(...) tasks.

The REST layer must not block on the multi-second engine.run() call —
endpoints return immediately with a run_id, while the actual lifecycle
runs as an asyncio task tracked here. On completion the manager persists
the updated run + recovery report via the supplied sessionmaker.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from ..injection.injector import FailureInjector
from ..models.experiments import ExperimentDefinition, ExperimentRun, RunStatus
from .experiment_engine import ExperimentEngine, RunOutcome

logger = logging.getLogger(__name__)


class RunManager:
    """Tracks in-flight experiment runs."""

    def __init__(
        self,
        engine: ExperimentEngine,
        injector: FailureInjector,
        sessionmaker: async_sessionmaker[AsyncSession],
    ) -> None:
        self._engine = engine
        self._injector = injector
        self._sessionmaker = sessionmaker
        self._tasks: dict[str, asyncio.Task] = {}
        self._runs: dict[str, ExperimentRun] = {}
        self._dry_run = False

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def set_dry_run(self, value: bool) -> bool:
        self._dry_run = bool(value)
        logger.info("dry-run mode set to %s", self._dry_run)
        return self._dry_run

    def active_run_ids(self) -> list[str]:
        return [rid for rid, t in self._tasks.items() if not t.done()]

    def get_run(self, run_id: str) -> Optional[ExperimentRun]:
        return self._runs.get(run_id)

    async def start(self, definition: ExperimentDefinition) -> ExperimentRun:
        # Pre-create the run with PENDING status and persist immediately so
        # GET /runs/{run_id} works even before the engine task starts.
        run = ExperimentRun(experiment_id=definition.id, status=RunStatus.PENDING)
        self._runs[run.run_id] = run

        async with self._sessionmaker() as session:
            from ..persistence.repo import ExperimentRunRepo
            await ExperimentRunRepo(session).upsert(run)

        if self._dry_run:
            logger.info("DRY RUN — not actually executing %s", run.run_id)
            run.status = RunStatus.COMPLETED
            run.started_at = datetime.now(timezone.utc)
            run.ended_at = run.started_at
            async with self._sessionmaker() as session:
                from ..persistence.repo import ExperimentRunRepo
                await ExperimentRunRepo(session).upsert(run)
            return run

        task = asyncio.create_task(self._run_and_persist(definition, run))
        self._tasks[run.run_id] = task
        return run

    async def _run_and_persist(
        self, definition: ExperimentDefinition, run: ExperimentRun
    ) -> RunOutcome:
        try:
            outcome = await self._engine.run(definition)
            # Engine produced a fresh ExperimentRun object — copy its fields
            # back into ours so the caller's reference is current.
            for f in (
                "status", "started_at", "ended_at", "baseline_metrics",
                "post_metrics", "scenario_id", "recovery_report_id",
                "error_message",
            ):
                setattr(run, f, getattr(outcome.run, f))

            async with self._sessionmaker() as session:
                from ..persistence.repo import (
                    ExperimentRunRepo, RecoveryReportRepo,
                )
                if outcome.report is not None:
                    await RecoveryReportRepo(session).create(outcome.report)
                await ExperimentRunRepo(session).upsert(run)
            return outcome
        except asyncio.CancelledError:
            run.status = RunStatus.ABORTED
            run.ended_at = datetime.now(timezone.utc)
            async with self._sessionmaker() as session:
                from ..persistence.repo import ExperimentRunRepo
                await ExperimentRunRepo(session).upsert(run)
            # Best-effort rollback if a scenario is still active.
            try:
                if run.scenario_id is not None:
                    await self._injector.rollback(run.scenario_id)
            except Exception:  # noqa: BLE001
                logger.exception("rollback after abort failed for %s", run.run_id)
            raise
        finally:
            # Leave the task in the dict so we can introspect status; the
            # asyncio.Task.done() property gates active_run_ids().
            pass

    async def abort_run(self, run_id: str) -> bool:
        task = self._tasks.get(run_id)
        if task is None or task.done():
            return False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return True

    async def abort_all(self) -> int:
        ids = self.active_run_ids()
        for rid in ids:
            await self.abort_run(rid)
        # Final blanket rollback across any still-active scenarios.
        try:
            await self._injector.rollback_all()
        except Exception:  # noqa: BLE001
            logger.exception("global rollback_all failed")
        return len(ids)
