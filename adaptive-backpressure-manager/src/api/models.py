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
