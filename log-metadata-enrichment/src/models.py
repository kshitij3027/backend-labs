"""Pydantic v2 models for the log metadata enrichment pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from pydantic import BaseModel


class EnrichmentRequest(BaseModel):
    """Incoming log enrichment request."""

    log_message: str
    source: str = "unknown"


class EnrichedLog(BaseModel):
    """Enriched log entry with metadata from collectors."""

    message: str
    source: str
    timestamp: str = ""

    # System info (optional)
    hostname: Optional[str] = None
    os_info: Optional[str] = None
    python_version: Optional[str] = None

    # Service context (optional)
    service_name: Optional[str] = None
    environment: Optional[str] = None
    version: Optional[str] = None
    region: Optional[str] = None

    # Performance metrics (optional)
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    disk_percent: Optional[float] = None

    # Environment context (optional)
    env_context: Optional[Dict] = None

    # Enrichment metadata
    enrichment_duration_ms: float = 0.0
    collectors_applied: List[str] = []
    enrichment_errors: List[str] = []

    def __init__(self, **data):
        if "timestamp" not in data or data["timestamp"] == "":
            data["timestamp"] = datetime.now(timezone.utc).isoformat()
        super().__init__(**data)


class EnrichmentStats(BaseModel):
    """Statistics for enrichment pipeline operations."""

    processed_count: int = 0
    error_count: int = 0
    success_rate: float = 0.0
    average_throughput: float = 0.0
    runtime_seconds: float = 0.0
