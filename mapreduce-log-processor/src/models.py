from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class JobStatus(str, Enum):
    PENDING = "PENDING"
    MAPPING = "MAPPING"
    SHUFFLE_COMPLETE = "SHUFFLE_COMPLETE"
    REDUCING = "REDUCING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskType(str, Enum):
    MAP = "MAP"
    REDUCE = "REDUCE"


class WorkerStatus(str, Enum):
    ALIVE = "ALIVE"
    DEAD = "DEAD"


class JobCreate(BaseModel):
    input_path: str
    map_fn: str
    reduce_fn: str
    num_mappers: int = 2
    num_reducers: int = 2


class JobResponse(BaseModel):
    id: str
    status: JobStatus
    input_path: str
    map_fn: str
    reduce_fn: str
    num_mappers: int
    num_reducers: int
    created_at: datetime
    updated_at: datetime


class TaskResponse(BaseModel):
    id: str
    job_id: str
    type: TaskType
    status: TaskStatus
    worker_id: str | None
    partition_id: int
    retry_count: int
    created_at: datetime
    updated_at: datetime


class WorkerInfo(BaseModel):
    id: str
    status: WorkerStatus
    last_heartbeat: datetime
    tasks_completed: int


class ResultItem(BaseModel):
    key: str
    value: str


class JobResultResponse(BaseModel):
    job_id: str
    status: JobStatus
    results: list[ResultItem]
