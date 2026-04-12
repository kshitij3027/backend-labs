"""Pydantic models for events, sessions, and analytics."""
from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class SessionState(str, enum.Enum):
    """Session lifecycle states."""
    CREATED = "created"
    ACTIVE = "active"
    IDLE = "idle"
    EXPIRED = "expired"


class SessionType(str, enum.Enum):
    """Classification of session behavior."""
    BROWSING = "browsing"
    SEARCHING = "searching"
    PURCHASING = "purchasing"
    MIXED = "mixed"


class FunnelStage(str, enum.Enum):
    """E-commerce funnel progression stages."""
    NONE = "none"
    VIEWED = "viewed"
    CARTED = "carted"
    PURCHASED = "purchased"


class Event(BaseModel):
    """A single user event."""
    user_id: str
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict = Field(default_factory=dict)
    device_type: str = "desktop"
    page_url: str = ""


class Session(BaseModel):
    """A user session tracking state and metadata."""
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    state: SessionState = SessionState.CREATED
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_event_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    event_count: int = 0
    events: list[Event] = Field(default_factory=list)
    device_type: str = "desktop"
    pages_visited: list[str] = Field(default_factory=list)
    event_types: list[str] = Field(default_factory=list)
    quality_score: float = 0.0
    engagement: str = "bounce"
    anomaly_score: float = 0.0
    session_type: str = "browsing"
    funnel_stage: str = "none"
    merged_from: list[str] = Field(default_factory=list)


class SessionAnalysis(BaseModel):
    """Analysis result for a session."""
    quality_score: float
    engagement: str
    session_id: str
