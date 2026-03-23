from enum import Enum
from pydantic import BaseModel


class JobStatus(str, Enum):
    PENDING = "PENDING"
    MAPPING = "MAPPING"
    SHUFFLING = "SHUFFLING"
    REDUCING = "REDUCING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class AnalysisType(str, Enum):
    WORD_COUNT = "WORD_COUNT"
    PATTERN_FREQUENCY = "PATTERN_FREQUENCY"
    SERVICE_DISTRIBUTION = "SERVICE_DISTRIBUTION"
    SECURITY = "SECURITY"


class JobSubmission(BaseModel):
    analysis_type: AnalysisType
    input_files: list[str]
    num_workers: int | None = None


class JobInfo(BaseModel):
    job_id: str
    status: JobStatus
    analysis_type: AnalysisType
    progress: float = 0.0
    current_phase: str = "pending"
    total_chunks: int = 0
    completed_chunks: int = 0
    error_message: str | None = None
    execution_time: float = 0.0
    records_processed: int = 0
    results: dict | None = None
    created_at: str
    completed_at: str | None = None

    def set_phase(self, status: JobStatus, phase_name: str) -> None:
        self.status = status
        self.current_phase = phase_name

    def update_progress(
        self, completed: int, total: int, records: int = 0
    ) -> None:
        self.completed_chunks = completed
        self.total_chunks = total
        self.progress = completed / total if total > 0 else 0.0
        if records:
            self.records_processed = records

    def set_completed(self, results: dict, execution_time: float) -> None:
        self.status = JobStatus.COMPLETED
        self.current_phase = "completed"
        self.progress = 1.0
        self.results = results
        self.execution_time = execution_time

    def set_failed(self, error: str, execution_time: float = 0.0) -> None:
        self.status = JobStatus.FAILED
        self.current_phase = "failed"
        self.error_message = error
        self.execution_time = execution_time
