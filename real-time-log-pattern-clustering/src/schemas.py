"""Pydantic v2 data models for the Real-Time Log Pattern Clustering engine.

These are the typed contracts that flow through the system (source: project
requirements §8):

* :class:`LogEntry` — a single incoming log record (the API/stream input).
* :class:`AlgoResult` / :class:`ClusterAssignment` — per-algorithm cluster
  assignments and the combined per-log verdict.
* :class:`PatternRecord` — a discovered, categorized cluster/pattern.
* :class:`AnomalyAlert` — an anomaly pushed to the dashboard.
* :class:`StatsSnapshot` — aggregate engine statistics for the stats cards.
* :class:`HealthResponse` — the ``/health`` liveness payload.

All models accept population from plain dicts (``from_attributes=True``) so they can
be built directly from parsed JSON / Redis payloads.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class LogEntry(BaseModel):
    """A single log record fed into the clustering engine.

    ``timestamp`` accepts an ISO-8601 string (Pydantic coerces it to ``datetime``).
    Only ``timestamp``/``service``/``level``/``message`` are required; the remaining
    network/behavioral fields are optional and default to ``None`` when absent.
    """

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    service: str
    level: str
    message: str
    source_ip: Optional[str] = None
    endpoint: Optional[str] = None
    response_time_ms: Optional[float] = None
    status_code: Optional[int] = None


class AlgoResult(BaseModel):
    """The outcome of one clustering algorithm for a single log.

    ``cluster_id == -1`` denotes noise / a not-yet-seen pattern. ``confidence`` is
    constrained to ``[0, 1]``.
    """

    model_config = ConfigDict(from_attributes=True)

    algorithm: str  # one of: "kmeans" | "dbscan" | "hdbscan"
    cluster_id: int  # -1 == noise / new pattern
    confidence: float = Field(ge=0.0, le=1.0)
    is_anomaly: bool = False


class ClusterAssignment(BaseModel):
    """The combined cluster verdict for a single log across all algorithms."""

    model_config = ConfigDict(from_attributes=True)

    results: list[AlgoResult]
    is_new_pattern: bool
    is_anomaly: bool
    pattern_type: Optional[str] = None
    masked_message: Optional[str] = None


class PatternRecord(BaseModel):
    """A discovered, categorized pattern (a cluster with metadata)."""

    model_config = ConfigDict(from_attributes=True)

    pattern_id: str
    # one of: "security_pattern" | "performance_pattern" | "error_pattern" | "generic"
    pattern_type: str
    algorithm: str
    representative: str
    count: int
    confidence: float
    first_seen: datetime
    last_seen: datetime


class AnomalyAlert(BaseModel):
    """An anomaly event pushed to the dashboard via WebSocket."""

    model_config = ConfigDict(from_attributes=True)

    timestamp: datetime
    message: str
    service: Optional[str] = None
    algorithms: list[str] = Field(default_factory=list)  # algorithms that flagged it
    score: float


class StatsSnapshot(BaseModel):
    """Aggregate engine statistics powering the dashboard stat cards."""

    model_config = ConfigDict(from_attributes=True)

    total_processed: int
    throughput_per_sec: float
    total_clusters: int
    patterns_discovered: int
    anomalies_detected: int
    algorithms: list[str] = Field(default_factory=list)
    silhouette: Optional[float] = None
    davies_bouldin: Optional[float] = None
    coherence: Optional[float] = None


class HealthResponse(BaseModel):
    """Liveness payload returned by ``GET /health``."""

    model_config = ConfigDict(from_attributes=True)

    status: str
    version: str
    algorithms: list[str] = Field(default_factory=list)
