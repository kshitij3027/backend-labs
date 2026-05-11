from pydantic import BaseModel, Field

from src.state import Priority


class IngestRequest(BaseModel):
    message: str = Field(min_length=1, max_length=8192)
    priority: Priority = Priority.NORMAL


class IngestResponse(BaseModel):
    accepted: bool
    verdict: str
    priority: Priority


class BackpressureBlock(BaseModel):
    pressure_level: str
    throttle_rate: float
    queue_size: int
    pressure_score: float


class ProcessorBlock(BaseModel):
    processed_count: int
    dropped_count: int
    error_count: int


class CircuitBreakerBlock(BaseModel):
    state: str
    failure_count: int


class SystemStatus(BaseModel):
    backpressure: BackpressureBlock
    processor: ProcessorBlock
    circuit_breaker: CircuitBreakerBlock


class LoadTestStartRequest(BaseModel):
    profile: str = "full"
    rps: int = Field(default=200, ge=1, le=100_000)
    duration_seconds: int = Field(default=60, ge=1, le=3600)
    spike_multiplier: float = Field(default=10.0, ge=1.0, le=100.0)


class LoadTestStatusResponse(BaseModel):
    state: str
    profile: str
    current_phase: str
    elapsed_s: float
    emitted: int
    accepted: int
    throttled: int
    dropped: int
    rejected: int


class AdminConfigUpdate(BaseModel):
    ewma_alpha: float | None = Field(default=None, ge=0.0, le=1.0)
    up_normal_to_pressure: float | None = Field(default=None, ge=0.0, le=1.0)
    up_pressure_to_overload: float | None = Field(default=None, ge=0.0, le=1.0)
    up_overload_to_emergency: float | None = Field(default=None, ge=0.0, le=1.0)
    down_overload_to_pressure: float | None = Field(default=None, ge=0.0, le=1.0)
    down_pressure_to_normal: float | None = Field(default=None, ge=0.0, le=1.0)
    down_recovery_to_normal: float | None = Field(default=None, ge=0.0, le=1.0)
    min_dwell_seconds: float | None = Field(default=None, ge=0.0, le=300.0)
    processing_latency_seconds: float | None = Field(default=None, ge=0.0, le=10.0)
    sampling_interval: float | None = Field(default=None, ge=0.05, le=60.0)
    aimd_beta: float | None = Field(default=None, ge=0.05, le=0.99)
    max_queue_size: int | None = Field(default=None, ge=1, le=1_000_000)


class AdminConfigResponse(BaseModel):
    updated_fields: list[str]
    current: dict
