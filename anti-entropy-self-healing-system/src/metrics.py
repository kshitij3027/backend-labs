import time
from dataclasses import dataclass, field


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

    def record_inconsistency(self, count: int = 1):
        self.inconsistencies_detected += count

    def record_repair(self, completed: int, failed: int, duration: float = 0.0):
        self.repairs_completed += completed
        self.repairs_failed += failed
        if completed > 0 and duration > 0:
            self._repair_times.append(duration)
            self.avg_repair_time = sum(self._repair_times) / len(self._repair_times)

    def record_scan(self, inconsistencies: int, repairs_completed: int, repairs_failed: int, duration: float):
        self.last_scan_time = time.time()
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
