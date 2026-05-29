"""Pydantic response envelopes for the REST API.

These thin wrappers compose the domain models defined in :mod:`src.models`
into the exact shapes the dashboard and e2e tests consume, so the route
handlers stay declarative.
"""

from __future__ import annotations

from pydantic import BaseModel

from src.models import LoadConfig, MetricSnapshot, OptimizerStatus


class MetricsResponse(BaseModel):
    """Combined payload for the dashboard's metrics view.

    Bundles the latest measured snapshot, the chartable parallel series for the
    recent window, and the current optimizer status into one response so the
    dashboard can render everything from a single poll.
    """

    current: MetricSnapshot | None
    series: dict
    status: OptimizerStatus


class LoadResponse(BaseModel):
    """Acknowledgement returned after retargeting the synthetic traffic."""

    applied: LoadConfig
    current_rate: float
    message: str
