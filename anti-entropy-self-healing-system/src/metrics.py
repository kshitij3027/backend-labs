import time
from dataclasses import dataclass, field
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# Prometheus metrics
prom_comparisons = Counter("ae_comparisons_total", "Total pairwise comparisons")
prom_inconsistencies = Counter("ae_inconsistencies_total", "Total inconsistencies detected")
prom_repairs = Counter("ae_repairs_total", "Total repairs completed", ["status"])  # status=completed|failed
prom_scan_duration = Histogram("ae_scan_duration_seconds", "Scan cycle duration")
prom_repair_duration = Histogram("ae_repair_duration_seconds", "Repair cycle duration")


@dataclass
class ScanRecord:
    timestamp: float
    inconsistencies: int
    repairs_completed: int
    repairs_failed: int
    duration: float


class ConsistencyMetrics:
    def __init__(self):
        self.comparisons: int = 0
        self.inconsistencies_detected: int = 0
        self.repairs_completed: int = 0
        self.repairs_failed: int = 0
        self.last_scan_time: float = 0.0
        self.avg_repair_time: float = 0.0
        self._repair_times: list[float] = []
        self.scan_history: list[ScanRecord] = []

    def record_comparison(self, count: int = 1):
        self.comparisons += count
        prom_comparisons.inc(count)

    def record_inconsistency(self, count: int = 1):
        self.inconsistencies_detected += count
        prom_inconsistencies.inc(count)

    def record_repair(self, completed: int, failed: int, duration: float = 0.0):
        self.repairs_completed += completed
        self.repairs_failed += failed
        prom_repairs.labels(status="completed").inc(completed)
        prom_repairs.labels(status="failed").inc(failed)
        if duration > 0:
            prom_repair_duration.observe(duration)
        if completed > 0 and duration > 0:
            self._repair_times.append(duration)
            self.avg_repair_time = sum(self._repair_times) / len(self._repair_times)

    def record_scan(self, inconsistencies: int, repairs_completed: int, repairs_failed: int, duration: float):
        self.last_scan_time = time.time()
        prom_scan_duration.observe(duration)
        self.scan_history.append(ScanRecord(
            timestamp=self.last_scan_time,
            inconsistencies=inconsistencies,
            repairs_completed=repairs_completed,
            repairs_failed=repairs_failed,
            duration=duration,
        ))

    def to_dict(self) -> dict:
        return {
            "comparisons": self.comparisons,
            "inconsistencies_detected": self.inconsistencies_detected,
            "repairs_completed": self.repairs_completed,
            "repairs_failed": self.repairs_failed,
            "last_scan_time": self.last_scan_time,
            "avg_repair_time": round(self.avg_repair_time, 4),
            "scan_history": [
                {
                    "timestamp": r.timestamp,
                    "inconsistencies": r.inconsistencies,
                    "repairs_completed": r.repairs_completed,
                    "repairs_failed": r.repairs_failed,
                    "duration": round(r.duration, 4),
                }
                for r in self.scan_history[-10:]  # Last 10 scans
            ],
        }
