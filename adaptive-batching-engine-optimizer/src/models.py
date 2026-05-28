from enum import Enum

from pydantic import BaseModel, Field


class OptimizerState(str, Enum):
    """Explicit operating states of the control loop (spec Feature Area B)."""

    LEARNING = "learning"  # gathering baseline (B, throughput) samples
    OPTIMIZING = "optimizing"  # actively climbing the throughput gradient
    STABLE = "stable"  # settled near the optimum, gradient ~ 0
    EMERGENCY = "emergency"  # safe-default fallback under resource stress


class MetricSnapshot(BaseModel):
    """A single point-in-time observation of system + processing metrics."""

    timestamp: float
    batch_size: int
    throughput: float  # records processed per second
    latency_ms: float  # per-batch processing latency in milliseconds
    cpu_percent: float
    memory_percent: float
    memory_available_mb: float
    queue_depth: int = 0


class OptimizerStatus(BaseModel):
    """Current optimizer state exposed via the API and WebSocket stream."""

    state: OptimizerState
    batch_size: int
    last_gradient: float
    smoothing_alpha: float
    min_batch_size: int
    max_batch_size: int
    constraint_active: bool
    reason: str = ""


class LoadConfig(BaseModel):
    """Request body for the load-simulation endpoint."""

    messages_per_second: float = Field(ge=0)
    burst_probability: float = Field(default=0.0, ge=0, le=1)
    payload_size_bytes: int = Field(default=256, ge=0)


class OptimizerConfigUpdate(BaseModel):
    """Partial optimizer reconfiguration; all fields optional (patch semantics)."""

    smoothing_alpha: float | None = None
    min_batch_size: int | None = None
    max_batch_size: int | None = None
    optimization_interval: float | None = None
    cpu_constraint_threshold: float | None = None
    memory_constraint_threshold: float | None = None
    latency_constraint_threshold: float | None = None
    batch_increase_factor: float | None = None
    batch_decrease_factor: float | None = None
    weight_throughput: float | None = None
    weight_latency: float | None = None


class DecisionRecord(BaseModel):
    """One entry in the optimizer's decision history (for dashboard display)."""

    timestamp: float
    old_batch_size: int
    new_batch_size: int
    gradient: float
    state: OptimizerState
    reason: str
