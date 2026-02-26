"""Processing metrics for the log pipeline."""
import time
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class ProcessingMetrics:
    """Track metrics during log processing."""
    total_lines: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    format_distribution: Dict[str, int] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0

    def record_success(self, format_name: str) -> None:
        """Record a successfully parsed line."""
        self.successful += 1
        self.total_lines += 1
        self.format_distribution[format_name] = (
            self.format_distribution.get(format_name, 0) + 1
        )

    def record_failure(self) -> None:
        """Record a failed parse."""
        self.failed += 1
        self.total_lines += 1

    def record_skip(self) -> None:
        """Record a skipped line (empty, whitespace)."""
        self.skipped += 1
        self.total_lines += 1

    def finish(self) -> None:
        """Mark processing as complete."""
        self.end_time = time.time()

    @property
    def elapsed_seconds(self) -> float:
        """Get elapsed time in seconds."""
        end = self.end_time if self.end_time > 0 else time.time()
        return max(end - self.start_time, 0.001)

    @property
    def throughput(self) -> float:
        """Lines processed per second."""
        return self.total_lines / self.elapsed_seconds

    @property
    def success_rate(self) -> float:
        """Percentage of successfully parsed lines."""
        non_skipped = self.total_lines - self.skipped
        if non_skipped <= 0:
            return 0.0
        return (self.successful / non_skipped) * 100.0

    def to_dict(self) -> dict:
        """Serialize metrics to dictionary."""
        return {
            "total_lines": self.total_lines,
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "format_distribution": dict(self.format_distribution),
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "throughput_per_second": round(self.throughput, 2),
            "success_rate_percent": round(self.success_rate, 2),
        }
