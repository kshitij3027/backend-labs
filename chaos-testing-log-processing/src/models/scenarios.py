"""Failure scenario domain models.

A :class:`FailureScenario` describes a single in-flight fault that the
``FailureInjector`` is asked to materialize against a target container. The
shape mirrors the experiment-definition payload accepted by the REST API and
is intentionally behavior-free at this commit: validation only.

JSON example (matches ``project_requirements.md`` §8 "Sample Output —
experiment definition"; the scenario is the runtime materialization of that
definition once the engine has minted a fresh id and status)::

    {
      "id": "8b1c5a2f4e3d4b6e9a7c0d8f1e2a3b4c",
      "type": "latency_injection",
      "target": "log-collector-service",
      "parameters": {"latency_ms": 200},
      "duration": 300,
      "severity": 2,
      "status": "pending",
      "created_at": "2026-05-12T18:30:00+00:00"
    }
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class FailureType(str, Enum):
    """The five fault families supported by the framework.

    The string values use lowercase ``snake_case`` so they match the JSON
    payloads consumers send to the REST API (``project_requirements.md`` §8).
    """

    NETWORK_PARTITION = "network_partition"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    COMPONENT_FAILURE = "component_failure"
    LATENCY_INJECTION = "latency_injection"
    PACKET_LOSS = "packet_loss"


class ScenarioStatus(str, Enum):
    """Lifecycle states a scenario passes through.

    ``PENDING`` is the default at construction time. ``ACTIVE`` means the
    injection has been applied and the rollback finalizer is registered.
    ``COMPLETED``/``FAILED``/``ABORTED`` are terminal.
    """

    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


class FailureScenario(BaseModel):
    """A single concrete fault the injector will apply (or has applied).

    The shape is deliberately a thin wrapper around the experiment-definition
    payload from `project_requirements.md` §8 with a few engine-controlled
    fields (``id``, ``status``, ``created_at``) added.
    """

    model_config = ConfigDict(
        extra="forbid",
        use_enum_values=False,
        validate_assignment=True,
    )

    id: str = Field(
        default_factory=lambda: uuid4().hex,
        description="Engine-assigned uuid4 hex; stable across rollback/validate.",
    )
    type: FailureType = Field(description="Fault family — routes to per-type injector.")
    target: str = Field(
        min_length=1,
        description="Target container name. Must be in the safety allowlist.",
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form per-type kwargs (e.g. {'latency_ms': 200, 'jitter_ms': 50}).",
    )
    duration: int = Field(
        ge=1,
        le=3600,
        description="How long the fault is held active, in seconds (1..3600).",
    )
    severity: int = Field(
        ge=1,
        le=5,
        description="Blast-radius hint (1=mild, 5=disaster-recovery class).",
    )
    status: ScenarioStatus = Field(
        default=ScenarioStatus.PENDING,
        description="Lifecycle state — mutated by the engine, never by callers.",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC instant the scenario was minted.",
    )
