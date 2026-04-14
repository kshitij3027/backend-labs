"""Pydantic models for the real-time analytics dashboard."""

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    redis_connected: bool = False
