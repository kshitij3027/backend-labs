"""In-memory metrics collection for the MapReduce coordinator."""

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class MetricsCollector:
    jobs_submitted: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    tasks_completed_by_type: dict = field(default_factory=lambda: defaultdict(int))
    tasks_failed_by_type: dict = field(default_factory=lambda: defaultdict(int))
    job_durations: list = field(default_factory=list)  # list of (job_id, duration_seconds)
    shuffle_volumes: dict = field(default_factory=dict)  # job_id -> bytes

    def record_job_submitted(self):
        self.jobs_submitted += 1

    def record_job_completed(self, job_id: str, duration_seconds: float):
        self.jobs_completed += 1
        self.job_durations.append((job_id, duration_seconds))

    def record_job_failed(self):
        self.jobs_failed += 1

    def record_task_completed(self, task_type: str):
        self.tasks_completed_by_type[task_type] += 1

    def record_task_failed(self, task_type: str):
        self.tasks_failed_by_type[task_type] += 1

    def record_shuffle_volume(self, job_id: str, bytes_count: int):
        self.shuffle_volumes[job_id] = self.shuffle_volumes.get(job_id, 0) + bytes_count

    @property
    def avg_job_duration(self) -> float:
        if not self.job_durations:
            return 0.0
        return sum(d for _, d in self.job_durations) / len(self.job_durations)

    def to_dict(self) -> dict:
        return {
            "jobs_submitted": self.jobs_submitted,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
            "tasks_completed_by_type": dict(self.tasks_completed_by_type),
            "tasks_failed_by_type": dict(self.tasks_failed_by_type),
            "avg_job_duration_seconds": round(self.avg_job_duration, 2),
            "total_shuffle_volume_bytes": sum(self.shuffle_volumes.values()),
        }


# Module-level singleton
metrics = MetricsCollector()
