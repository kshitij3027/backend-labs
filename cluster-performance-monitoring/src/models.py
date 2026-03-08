"""Pydantic models for cluster performance monitoring."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class MetricPoint(BaseModel):
    """A single metric data point collected from a cluster node."""

    timestamp: datetime
    node_id: str
    metric_name: str
    value: float
    labels: dict[str, str] = Field(default_factory=dict)


class NodeInfo(BaseModel):
    """Metadata about a cluster node."""

    node_id: str
    role: str
    host: str
    port: int


class AggregatedMetric(BaseModel):
    """Statistical aggregation over a window of metric points."""

    metric_name: str
    node_id: str
    min: float
    max: float
    avg: float
    p95: float
    p99: float
    count: int


class Alert(BaseModel):
    """A threshold-breach alert for a monitored metric."""

    level: str
    metric_name: str
    node_id: str
    current_value: float
    threshold: float
    message: str
    timestamp: datetime


class ClusterHealth(BaseModel):
    """Overall health summary of the monitored cluster."""

    status: str
    performance_score: float
    active_nodes: int
    total_throughput: float
    avg_cpu_usage: float
    avg_memory_usage: float
    alerts: list[Alert]
    recommendations: list[str]


class PerformanceReport(BaseModel):
    """Comprehensive performance report for the cluster."""

    report_id: str
    generated_at: datetime
    cluster_health: ClusterHealth
    performance_summary: dict
    alerts_summary: dict
    recommendations: list[str]
    node_metrics: dict
