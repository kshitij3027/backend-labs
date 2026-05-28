from dataclasses import asdict, dataclass
from typing import Literal

StageName = Literal["parse", "validate", "transform", "write"]


@dataclass(slots=True, frozen=True)
class MetricSample:
    stage: StageName
    ts: float
    cpu_pct: float
    mem_mb: float
    io_read_bytes: int
    io_write_bytes: int
    queue_depth: int
    latency_ms: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class StageEvent:
    stage: StageName
    started_ns: int
    duration_ns: int
    cpu_delta_pct: float
    rss_delta_kb: int
    record_count: int

    def to_dict(self) -> dict:
        return asdict(self)
