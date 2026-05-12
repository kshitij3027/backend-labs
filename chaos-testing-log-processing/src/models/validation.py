"""Recovery-validation domain models.

The :class:`RecoveryValidator` (lands in C10) runs a fixed-order suite of
:class:`RecoveryTest` instances after every chaos scenario and emits a
single :class:`RecoveryReport`. The shape of that report is the public
contract surfaced to API consumers and stored in SQLite.

JSON example (matches ``project_requirements.md`` §8 "Sample Output —
recovery validation report")::

    {
      "scenario_id": "8b1c5a2f4e3d4b6e9a7c0d8f1e2a3b4c",
      "overall_success": true,
      "validation_duration": 12.4,
      "test_results": [
        {
          "name": "HealthProbeTest",
          "status": "completed",
          "duration": 1.2,
          "details": {"endpoint": "/health", "code": 200},
          "error_message": null
        }
      ],
      "summary": {
        "total_tests": 4,
        "passed_tests": 4,
        "failed_tests": 0,
        "timeout_tests": 0
      }
    }
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class RecoveryTestStatus(str, Enum):
    """Lifecycle/result state for a single recovery test."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


class RecoveryTest(BaseModel):
    """Spec for one recovery test (HealthProbe, LatencyBaseline, DataLoss...).

    Behaviour-free at this commit; the executable counterpart lives in
    ``src/validation/tests.py`` and consumes one of these as configuration.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    name: str = Field(min_length=1, description="Test name (also report key).")
    description: str = Field(default="", description="Human-readable rationale.")
    required_for_success: bool = Field(
        default=True,
        description=(
            "If True, a non-completed status flips overall_success to False. "
            "Optional probes (e.g. nice-to-haves) can be set False."
        ),
    )
    timeout_s: float = Field(
        default=30.0,
        gt=0.0,
        description="Hard timeout for this test; exceeding it -> TIMEOUT status.",
    )


class TestResult(BaseModel):
    """Outcome of running a single :class:`RecoveryTest`."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    name: str = Field(min_length=1, description="Mirrors RecoveryTest.name.")
    status: RecoveryTestStatus = Field(description="Final status of this run.")
    duration: float = Field(
        ge=0.0,
        description="Wall-clock seconds the test took (including retries).",
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form structured payload (probe code, observed latency, ...).",
    )
    error_message: str | None = Field(
        default=None,
        description="Stringified exception on FAILED/TIMEOUT; None otherwise.",
    )


class RecoverySummary(BaseModel):
    """Aggregate counters over all :class:`TestResult` entries in a report."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    total_tests: int = Field(ge=0, description="Count of test_results entries.")
    passed_tests: int = Field(ge=0, description="Count with status == completed.")
    failed_tests: int = Field(ge=0, description="Count with status == failed.")
    timeout_tests: int = Field(ge=0, description="Count with status == timeout.")


class RecoveryReport(BaseModel):
    """The verdict the :class:`RecoveryValidator` produces per scenario.

    Persisted to SQLite (C12) and pushed to the dashboard report panel (C17).
    The shape is the public contract documented in ``project_requirements.md``
    §8.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    report_id: str = Field(
        default_factory=lambda: uuid4().hex,
        description="Engine-assigned uuid4 hex; stable, referenced by ExperimentRun.",
    )
    scenario_id: str = Field(
        min_length=1,
        description="The FailureScenario.id this report belongs to.",
    )
    overall_success: bool = Field(
        description="True iff every required_for_success test reached COMPLETED.",
    )
    validation_duration: float = Field(
        ge=0.0,
        description="Wall-clock seconds the full validator suite took.",
    )
    test_results: list[TestResult] = Field(
        description="Per-test outcomes in the order they ran.",
    )
    summary: RecoverySummary = Field(description="Aggregate counters.")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC instant the report was finalized.",
    )
