"""Structured logging + Prometheus metrics public surface."""

from .logging import configure_logging, request_id_middleware
from .prom import (
    EXPERIMENTS_TOTAL,
    FAULTS_ACTIVE,
    INJECTION_LATENCY_SECONDS,
    RECOVERY_DURATION_SECONDS,
    RECOVERY_FAILURES_TOTAL,
    EMERGENCY_STOPS_TOTAL,
    render_latest,
)

__all__ = [
    "configure_logging",
    "request_id_middleware",
    "EXPERIMENTS_TOTAL",
    "FAULTS_ACTIVE",
    "INJECTION_LATENCY_SECONDS",
    "RECOVERY_DURATION_SECONDS",
    "RECOVERY_FAILURES_TOTAL",
    "EMERGENCY_STOPS_TOTAL",
    "render_latest",
]
