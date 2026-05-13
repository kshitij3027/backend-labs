"""SQLAlchemy 2.0 async models for the chaos framework.

Three tables:

* ``experiment_definitions`` --- the user-supplied scenarios (template).
* ``experiment_runs``        --- one entry per actual execution of a definition.
* ``recovery_reports``       --- one entry per validator output.

All nested/structured fields (parameters, hypothesis, metrics, test_results, summary)
are stored as JSON text columns so we don't fight Pydantic's shape changes
during the learning project's iteration.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Declarative base for all chaos-framework ORM models."""

    pass


class ExperimentDefinitionRow(Base):
    """One row per user-authored ``ExperimentDefinition``."""

    __tablename__ = "experiment_definitions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    description: Mapped[str] = mapped_column(String, nullable=False, default="")
    type: Mapped[str] = mapped_column(String, nullable=False)
    target: Mapped[str] = mapped_column(String, nullable=False, index=True)
    parameters: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    duration: Mapped[int] = mapped_column(Integer, nullable=False)
    severity: Mapped[int] = mapped_column(Integer, nullable=False)
    hypothesis: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    runs: Mapped[list["ExperimentRunRow"]] = relationship(
        back_populates="definition", cascade="all, delete-orphan"
    )


class ExperimentRunRow(Base):
    """One row per concrete execution of a definition."""

    __tablename__ = "experiment_runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    experiment_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("experiment_definitions.id", ondelete="CASCADE"),
        index=True,
    )
    status: Mapped[str] = mapped_column(String, nullable=False, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    baseline_metrics: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    post_metrics: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    scenario_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    recovery_report_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("recovery_reports.report_id", ondelete="SET NULL"),
        nullable=True,
    )
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)

    definition: Mapped[ExperimentDefinitionRow] = relationship(back_populates="runs")


class RecoveryReportRow(Base):
    """One row per ``RecoveryReport`` produced by the validator."""

    __tablename__ = "recovery_reports"

    report_id: Mapped[str] = mapped_column(String, primary_key=True)
    scenario_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    overall_success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    validation_duration: Mapped[float] = mapped_column(Float, nullable=False)
    test_results: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    summary: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
