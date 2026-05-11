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
