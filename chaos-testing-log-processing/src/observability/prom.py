"""Prometheus metric definitions + ``/metrics`` renderer.

Defines a private :class:`CollectorRegistry` so the chaos framework's
metrics are isolated from any default-registry traffic the
``prometheus-client`` library accumulates (default-registry use across
test runs is the usual source of "Duplicated time series" errors).

All six metrics referenced in §5 C16 of the project plan live here:

    - ``chaos_experiments_total{verdict}``    — Counter
    - ``chaos_faults_active``                  — Gauge
    - ``chaos_recovery_failures_total{test_name}`` — Counter
    - ``chaos_injection_latency_seconds{type}``    — Histogram
    - ``chaos_recovery_duration_seconds``      — Histogram
    - ``chaos_emergency_stops_total``          — Counter

The histograms ship custom buckets sized for the chaos workloads:
injection latency is sub-second-to-low-seconds; recovery validation runs
a probe suite that can stretch to tens of seconds.
"""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Module-level singletons so metrics survive app reloads.
REGISTRY = CollectorRegistry(auto_describe=True)

EXPERIMENTS_TOTAL = Counter(
    "chaos_experiments_total",
    "Total experiment runs by verdict (completed | failed | aborted)",
    ["verdict"],
    registry=REGISTRY,
)

FAULTS_ACTIVE = Gauge(
    "chaos_faults_active",
    "Number of currently in-flight chaos scenarios",
    registry=REGISTRY,
)

RECOVERY_FAILURES_TOTAL = Counter(
    "chaos_recovery_failures_total",
    "Number of recovery-test failures, by test name",
    ["test_name"],
    registry=REGISTRY,
)

INJECTION_LATENCY_SECONDS = Histogram(
    "chaos_injection_latency_seconds",
    "Wall-clock time from FailureInjector.inject() enter to return",
    ["type"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=REGISTRY,
)

RECOVERY_DURATION_SECONDS = Histogram(
    "chaos_recovery_duration_seconds",
    "Wall-clock duration of the recovery validation suite",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0),
    registry=REGISTRY,
)

EMERGENCY_STOPS_TOTAL = Counter(
    "chaos_emergency_stops_total",
    "Number of supervisor-triggered emergency stops",
    registry=REGISTRY,
)


def render_latest() -> tuple[bytes, str]:
    """Return ``(payload, content_type)`` for the FastAPI ``/metrics`` handler."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
