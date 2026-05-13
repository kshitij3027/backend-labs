"""Async repository facade over the persistence schema.

Each repo accepts an :class:`AsyncSession` and converts Pydantic <-> SQLAlchemy.
The Pydantic domain models in ``src/models/`` remain the public surface; the
ORM rows declared in :mod:`.schema` are an implementation detail of this
package.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..models.experiments import ExperimentDefinition, ExperimentRun, RunStatus
from ..models.validation import RecoveryReport
from .schema import (
    Base,
    ExperimentDefinitionRow,
    ExperimentRunRow,
    RecoveryReportRow,
)


# ----- Engine / session helpers -----------------------------------------


def make_engine(url: str) -> AsyncEngine:
    """Build an async SQLAlchemy engine.

    SQLite URLs may use ``:memory:`` for tests (e.g.
    ``sqlite+aiosqlite:///:memory:``).
    """
    return create_async_engine(url, future=True)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build an ``async_sessionmaker`` bound to ``engine``.

    ``expire_on_commit=False`` keeps ORM rows usable after ``commit()`` so
    the repos can read attributes off them when building Pydantic models.
    """
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_all_tables(engine: AsyncEngine) -> None:
    """Create every table declared on :class:`Base.metadata`.

    Idempotent --- safe to call on every app start.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ----- Definition repo --------------------------------------------------


class ExperimentDefinitionRepo:
    """Async CRUD + list repository for :class:`ExperimentDefinition`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _to_pydantic(row: ExperimentDefinitionRow) -> ExperimentDefinition:
        return ExperimentDefinition.model_validate(
            {
                "id": row.id,
                "name": row.name,
                "description": row.description,
                "type": row.type,
                "target": row.target,
                "parameters": dict(row.parameters or {}),
                "duration": row.duration,
                "severity": row.severity,
                "hypothesis": row.hypothesis,
                "created_at": row.created_at,
            }
        )

    @staticmethod
    def _to_row(model: ExperimentDefinition) -> ExperimentDefinitionRow:
        return ExperimentDefinitionRow(
            id=model.id,
            name=model.name,
            description=model.description,
            type=model.type.value if hasattr(model.type, "value") else str(model.type),
            target=model.target,
            parameters=dict(model.parameters),
            duration=model.duration,
            severity=model.severity,
            hypothesis=(
                model.hypothesis.model_dump() if model.hypothesis is not None else None
            ),
            created_at=model.created_at,
        )

    async def create(self, model: ExperimentDefinition) -> ExperimentDefinition:
        row = self._to_row(model)
        self._session.add(row)
        await self._session.commit()
        return model

    async def get(self, id_: str) -> Optional[ExperimentDefinition]:
        row = await self._session.get(ExperimentDefinitionRow, id_)
        return self._to_pydantic(row) if row is not None else None

    async def list_all(
        self,
        *,
        target: str | None = None,
        type_: str | None = None,
        limit: int = 100,
    ) -> list[ExperimentDefinition]:
        stmt = (
            select(ExperimentDefinitionRow)
            .order_by(desc(ExperimentDefinitionRow.created_at))
            .limit(limit)
        )
        if target:
            stmt = stmt.where(ExperimentDefinitionRow.target == target)
        if type_:
            stmt = stmt.where(ExperimentDefinitionRow.type == type_)
        rows = (await self._session.execute(stmt)).scalars().all()
        return [self._to_pydantic(r) for r in rows]

    async def delete(self, id_: str) -> bool:
        row = await self._session.get(ExperimentDefinitionRow, id_)
        if row is None:
            return False
        await self._session.delete(row)
        await self._session.commit()
        return True


# ----- Run repo ---------------------------------------------------------


class ExperimentRunRepo:
    """Async CRUD + list repository for :class:`ExperimentRun`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _to_pydantic(row: ExperimentRunRow) -> ExperimentRun:
        return ExperimentRun.model_validate(
            {
                "run_id": row.run_id,
                "experiment_id": row.experiment_id,
                "status": row.status,
                "started_at": row.started_at,
                "ended_at": row.ended_at,
                "baseline_metrics": row.baseline_metrics,
                "post_metrics": row.post_metrics,
                "scenario_id": row.scenario_id,
                "recovery_report_id": row.recovery_report_id,
                "error_message": row.error_message,
            }
        )

    @staticmethod
    def _to_row(model: ExperimentRun) -> ExperimentRunRow:
        return ExperimentRunRow(
            run_id=model.run_id,
            experiment_id=model.experiment_id,
            status=(
                model.status.value if hasattr(model.status, "value") else str(model.status)
            ),
            started_at=model.started_at,
            ended_at=model.ended_at,
            baseline_metrics=(
                model.baseline_metrics.model_dump(mode="json")
                if model.baseline_metrics
                else None
            ),
            post_metrics=(
                model.post_metrics.model_dump(mode="json")
                if model.post_metrics
                else None
            ),
            scenario_id=model.scenario_id,
            recovery_report_id=model.recovery_report_id,
            error_message=model.error_message,
        )

    async def create(self, model: ExperimentRun) -> ExperimentRun:
        row = self._to_row(model)
        self._session.add(row)
        await self._session.commit()
        return model

    async def get(self, run_id: str) -> Optional[ExperimentRun]:
        row = await self._session.get(ExperimentRunRow, run_id)
        return self._to_pydantic(row) if row is not None else None

    async def update_status(
        self,
        run_id: str,
        status: RunStatus,
        error_message: str | None = None,
    ) -> bool:
        row = await self._session.get(ExperimentRunRow, run_id)
        if row is None:
            return False
        row.status = status.value if hasattr(status, "value") else str(status)
        if error_message is not None:
            row.error_message = error_message
        await self._session.commit()
        return True

    async def upsert(self, model: ExperimentRun) -> ExperimentRun:
        """Insert or replace the run row by primary key (``run_id``)."""
        existing = await self._session.get(ExperimentRunRow, model.run_id)
        if existing is None:
            self._session.add(self._to_row(model))
        else:
            new_row = self._to_row(model)
            # Copy fields onto the persistent instance to keep the identity stable.
            for col in (
                "experiment_id",
                "status",
                "started_at",
                "ended_at",
                "baseline_metrics",
                "post_metrics",
                "scenario_id",
                "recovery_report_id",
                "error_message",
            ):
                setattr(existing, col, getattr(new_row, col))
        await self._session.commit()
        return model

    async def list_by_experiment(
        self, experiment_id: str, limit: int = 50
    ) -> list[ExperimentRun]:
        stmt = (
            select(ExperimentRunRow)
            .where(ExperimentRunRow.experiment_id == experiment_id)
            .order_by(desc(ExperimentRunRow.started_at))
            .limit(limit)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [self._to_pydantic(r) for r in rows]


# ----- Report repo ------------------------------------------------------


class RecoveryReportRepo:
    """Async CRUD + list repository for :class:`RecoveryReport`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _to_pydantic(row: RecoveryReportRow) -> RecoveryReport:
        return RecoveryReport.model_validate(
            {
                "report_id": row.report_id,
                "scenario_id": row.scenario_id,
                "overall_success": row.overall_success,
                "validation_duration": row.validation_duration,
                "test_results": list(row.test_results or []),
                "summary": dict(row.summary or {}),
                "created_at": row.created_at,
            }
        )

    @staticmethod
    def _to_row(model: RecoveryReport) -> RecoveryReportRow:
        return RecoveryReportRow(
            report_id=model.report_id,
            scenario_id=model.scenario_id,
            overall_success=model.overall_success,
            validation_duration=model.validation_duration,
            test_results=[t.model_dump(mode="json") for t in model.test_results],
            summary=model.summary.model_dump(mode="json"),
            created_at=model.created_at,
        )

    async def create(self, model: RecoveryReport) -> RecoveryReport:
        row = self._to_row(model)
        self._session.add(row)
        await self._session.commit()
        return model

    async def get(self, report_id: str) -> Optional[RecoveryReport]:
        row = await self._session.get(RecoveryReportRow, report_id)
        return self._to_pydantic(row) if row is not None else None

    async def list_by_scenario(
        self, scenario_id: str, limit: int = 50
    ) -> list[RecoveryReport]:
        stmt = (
            select(RecoveryReportRow)
            .where(RecoveryReportRow.scenario_id == scenario_id)
            .order_by(desc(RecoveryReportRow.created_at))
            .limit(limit)
        )
        rows = (await self._session.execute(stmt)).scalars().all()
        return [self._to_pydantic(r) for r in rows]
