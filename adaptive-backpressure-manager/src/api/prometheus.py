from typing import Any, Dict

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

from src.state import Priority


class Metrics:
    """Holds Prometheus collectors and JSON-snapshot helpers."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.pressure_score = Gauge("abpm_pressure_score", "Current fused pressure score", registry=self.registry)
        self.throttle_rate = Gauge("abpm_throttle_rate", "Current AIMD throttle rate (limit/initial)", registry=self.registry)
        self.queue_size = Gauge("abpm_queue_size", "Queue size by priority", ["priority"], registry=self.registry)
        self.aimd_limit = Gauge("abpm_aimd_limit", "Current AIMD limit", registry=self.registry)
        self.pressure_level = Gauge("abpm_pressure_level", "Pressure level (0=normal,1=pressure,2=overload,3=recovery)", registry=self.registry)
        self.processed_total = Counter(
            "abpm_processed_total", "Total processed messages", ["priority"], registry=self.registry
        )
        self.dropped_total = Counter(
            "abpm_dropped_total", "Total dropped messages", ["priority"], registry=self.registry
        )
        self.errored_total = Counter(
            "abpm_errored_total", "Total errored messages", ["priority"], registry=self.registry
        )

    def text(self) -> bytes:
        return generate_latest(self.registry)

    def json_snapshot(self, components: Any) -> Dict[str, Any]:
        from src.state import PressureLevel
        c = components
        level_map = {
            PressureLevel.NORMAL: 0,
            PressureLevel.PRESSURE: 1,
            PressureLevel.OVERLOAD: 2,
            PressureLevel.RECOVERY: 3,
        }
        return {
            "pressure_score": c.fuser.last_score,
            "pressure_level": c.manager.level.value,
            "pressure_level_code": level_map.get(c.manager.level, 0),
            "throttle_rate": c.aimd.throttle_rate,
            "aimd_limit": c.aimd.limit,
            "queue_sizes": {p.value: c.queues.qsize(p) for p in Priority},
            "queue_total": c.queues.total_qsize(),
            "processed_per_priority": {p.value: c.workers.processed_per_priority.get(p, 0) for p in Priority},
            "errored_per_priority": {p.value: c.workers.errored_per_priority.get(p, 0) for p in Priority},
            "admission_counters": c.admission.counters,
            "breaker_state": c.breaker.state.value,
            "breaker_failures": c.breaker.failure_count,
        }
