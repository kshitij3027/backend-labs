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
