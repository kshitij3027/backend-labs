"""Log ingestion and metric extraction for the real-time analytics dashboard."""

from __future__ import annotations

import time

import numpy as np

from src.models import LogEntry, MetricPoint


def extract_metrics(logs: list[LogEntry]) -> list[MetricPoint]:
    """Extract MetricPoint entries from raw log entries.

    A single log can produce multiple metrics (e.g. a GET request with
    response_time yields both a ``response_time`` and a ``request_count``
    metric).  Tags only include fields that are not None, and all values
    are cast to strings.
    """
    metrics: list[MetricPoint] = []

    for log in logs:
        base_tags: dict[str, str] = {}
        if log.endpoint is not None:
            base_tags["endpoint"] = str(log.endpoint)
        if log.method is not None:
            base_tags["method"] = str(log.method)
        if log.status_code is not None:
            base_tags["status_code"] = str(log.status_code)

        # --- response_time metric ---
        if log.response_time is not None:
            metrics.append(MetricPoint(
                service=log.service,
                metric_name="response_time",
                value=log.response_time,
                timestamp=log.timestamp,
                tags=dict(base_tags),
            ))

        # --- error_count metric ---
        if log.level == "ERROR":
            error_tags: dict[str, str] = {}
            if log.error_type is not None:
                error_tags["error_type"] = str(log.error_type)
            if log.endpoint is not None:
                error_tags["endpoint"] = str(log.endpoint)
            metrics.append(MetricPoint(
                service=log.service,
                metric_name="error_count",
                value=1.0,
                timestamp=log.timestamp,
                tags=error_tags,
            ))

        # --- request_count metric ---
        if log.method is not None:
            metrics.append(MetricPoint(
                service=log.service,
                metric_name="request_count",
                value=1.0,
                timestamp=log.timestamp,
                tags=dict(base_tags),
            ))

    return metrics


def generate_sample_logs(
    service: str = "web-api",
    count: int = 50,
) -> list[LogEntry]:
    """Generate realistic sample log entries using numpy for randomness.

    Timestamps are spread over the last 5 minutes.  The generated logs
    include a mix of methods, endpoints, status codes, and a configurable
    error rate (~8 %).
    """
    rng = np.random.default_rng()
    now = time.time()

    endpoints = [
        "/api/users",
        "/api/orders",
        "/api/products",
        "/api/health",
        "/api/auth",
    ]
    methods = ["GET", "POST", "PUT", "DELETE"]
    method_weights = np.array([0.6, 0.25, 0.1, 0.05])

    status_choices = [200, 201, 400, 404, 500]
    status_weights = np.array([0.80, 0.05, 0.05, 0.05, 0.05])

    error_types = [
        "TimeoutError",
        "ConnectionError",
        "ValueError",
        "PermissionError",
    ]

    logs: list[LogEntry] = []

    # Pre-generate random values for the whole batch
    timestamps = now - rng.uniform(0, 300, size=count)
    endpoint_indices = rng.integers(0, len(endpoints), size=count)
    method_indices = rng.choice(len(methods), size=count, p=method_weights)
    status_indices = rng.choice(len(status_choices), size=count, p=status_weights)
    response_times = rng.normal(loc=120.0, scale=40.0, size=count)
    response_times = np.clip(response_times, 10.0, None)
    spike_mask = rng.random(size=count) < 0.05
    spike_values = rng.uniform(500.0, 2000.0, size=count)
    response_times = np.where(spike_mask, spike_values, response_times)
    error_mask = rng.random(size=count) < 0.08

    for i in range(count):
        ts = float(timestamps[i])
        endpoint = endpoints[int(endpoint_indices[i])]
        method = methods[int(method_indices[i])]
        status_code = status_choices[int(status_indices[i])]
        rt = round(float(response_times[i]), 1)
        is_error = bool(error_mask[i])

        if is_error:
            level = "ERROR"
            error_type = error_types[rng.integers(0, len(error_types))]
            message = f"{error_type}: {method} {endpoint} failed"
        else:
            level = "INFO"
            error_type = None
            message = f"{method} {endpoint} {status_code} {rt}ms"

        logs.append(LogEntry(
            timestamp=ts,
            service=service,
            level=level,
            message=message,
            response_time=rt,
            method=method,
            endpoint=endpoint,
            status_code=status_code,
            error_type=error_type,
        ))

    return logs
