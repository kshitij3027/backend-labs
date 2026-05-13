"""HTTP request/response schemas distinct from the persisted domain models."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field

from ..models.experiments import Hypothesis, RunStatus
from ..models.scenarios import FailureType


class CreateExperimentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    description: str = ""
    type: FailureType
    target: str = Field(min_length=1)
    parameters: dict[str, Any] = Field(default_factory=dict)
    duration: int = Field(ge=1, le=3600)
    severity: int = Field(ge=1, le=5)
    hypothesis: Optional[Hypothesis] = None


class StartRunResponse(BaseModel):
    run_id: str
    experiment_id: str
    status: RunStatus
    started_at: Optional[datetime] = None
    dry_run: bool = False


class AbortRunResponse(BaseModel):
    aborted: bool
    run_id: str


class AbortAllResponse(BaseModel):
    aborted_count: int


class DryRunResponse(BaseModel):
    dry_run: bool


class TargetInfo(BaseModel):
    name: str
    id: str
    image: Optional[str] = None
    status: Optional[str] = None
    labels: dict[str, str] = Field(default_factory=dict)
