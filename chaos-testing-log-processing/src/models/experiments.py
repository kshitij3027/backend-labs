"""Experiment-definition and run-status domain models.

An :class:`ExperimentDefinition` is the user-authored intent ("inject 200ms
of latency on log-consumer for 5 minutes, expect recovery within 30s").
An :class:`ExperimentRun` is one concrete execution of a definition — minted
by the engine, with its own lifecycle and baseline/post metric snapshots.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from .metrics import SystemMetrics
from .scenarios import FailureType

__all__ = [
    "ExperimentDefinition",
    "ExperimentRun",
    "FailureType",
    "Hypothesis",
    "RunStatus",
]


class Hypothesis(BaseModel):
    """Chaos-engineering hypothesis attached to an experiment.

    Encodes the "If X then Y within Zs" statement plus the recovery budget
    and the invariants the framework is expected to preserve. Stored alongside
    the run report so retrospectives can compare prediction vs. observation.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    statement: str = Field(
        min_length=1,
        description='Full sentence, e.g. "If 200ms latency is injected on log-consumer then p95 RTT returns to baseline within 30s".',
    )
    recovery_time_budget_s: int = Field(
        default=30,
        ge=1,
        description="Seconds the system has to return to steady state.",
    )
    expected_invariants: list[str] = Field(
        default_factory=list,
        description="Plain-English invariants that must hold (e.g. 'no data loss').",
    )


class ExperimentDefinition(BaseModel):
    """A user-authored chaos experiment — the request payload to POST /experiments."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    id: str = Field(
        default_factory=lambda: uuid4().hex,
        description="Engine-assigned uuid4 hex.",
    )
    name: str = Field(min_length=1, description="Human-readable experiment name.")
    description: str = Field(default="", description="Optional free-form description.")
    type: FailureType = Field(description="Fault family to inject.")
    target: str = Field(
        min_length=1,
        description="Target container name (must be in safety allowlist).",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Per-type kwargs (e.g. {'latency_ms': 200}).",
    )
    duration: int = Field(
        default=300,
        ge=1,
        le=3600,
        description="Seconds to hold the fault active (1..3600).",
    )
    severity: int = Field(
        default=2,
        ge=1,
        le=5,
        description="Blast-radius hint (1=mild, 5=disaster-recovery class).",
    )
    hypothesis: Hypothesis | None = Field(
        default=None,
        description="Optional hypothesis encoded as 'If X then Y within Zs'.",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC instant the definition was created.",
    )


class RunStatus(str, Enum):
    """Lifecycle states an ``ExperimentRun`` walks through.

    Matches the lifecycle in plan.md §2 / C11:
    PENDING -> RUNNING -> INJECTING -> OBSERVING -> ROLLING_BACK -> VALIDATING -> COMPLETED.
    FAILED / ABORTED are terminal-error states.
    """

    PENDING = "pending"
    RUNNING = "running"
    INJECTING = "injecting"
    OBSERVING = "observing"
    ROLLING_BACK = "rolling_back"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


class ExperimentRun(BaseModel):
    """One concrete execution of an :class:`ExperimentDefinition`.

    Persisted to SQLite (lands in C12) and pushed live over the WebSocket
    channel (lands in C14). Baseline + post snapshots are captured by the
    engine before injection and after validation respectively.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    run_id: str = Field(
        default_factory=lambda: uuid4().hex,
        description="Engine-assigned uuid4 hex; stable across the run.",
    )
    experiment_id: str = Field(
        min_length=1,
        description="Foreign key into ExperimentDefinition.id.",
    )
    status: RunStatus = Field(
        default=RunStatus.PENDING,
        description="Lifecycle state — mutated by the engine only.",
    )
    started_at: datetime | None = Field(
        default=None,
        description="UTC instant the engine left PENDING; None until then.",
    )
    ended_at: datetime | None = Field(
        default=None,
        description="UTC instant the run reached a terminal status.",
    )
    baseline_metrics: SystemMetrics | None = Field(
        default=None,
        description="Snapshot taken right before injection.",
    )
    post_metrics: SystemMetrics | None = Field(
        default=None,
        description="Snapshot taken after rollback + validation.",
    )
    scenario_id: str | None = Field(
        default=None,
        description="The FailureScenario.id minted by the injector for this run.",
    )
    recovery_report_id: str | None = Field(
        default=None,
        description="The RecoveryReport.report_id produced by the validator.",
    )
    error_message: str | None = Field(
        default=None,
        description="Populated on FAILED/ABORTED to explain the verdict.",
    )
