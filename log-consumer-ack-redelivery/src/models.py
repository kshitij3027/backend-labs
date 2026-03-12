"""Domain models for message acknowledgment tracking."""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MessageState(str, Enum):
    """Lifecycle states a message can be in."""

    PENDING = "pending"
    PROCESSING = "processing"
    ACKNOWLEDGED = "acknowledged"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"


class AckRecord(BaseModel):
    """Tracks acknowledgment state for a single message."""

    msg_id: str
    delivery_tag: int
    state: MessageState = MessageState.PENDING
    retry_count: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: Optional[str] = None


class DashboardStats(BaseModel):
    """Aggregated statistics shown on the monitoring dashboard."""

    total_received: int = 0
    total_acked: int = 0
    total_failed: int = 0
    total_retried: int = 0
    total_dead_lettered: int = 0
    pending_count: int = 0
    processing_count: int = 0
    success_rate: float = 0.0
    is_connected: bool = False
