"""Pydantic response envelopes for the cache REST API.

These thin wrappers shape the dicts produced by the metrics aggregator, the L1/
L2 tiers, the pattern engine, and the cache manager into the exact JSON the
dashboard and the e2e/integration tests consume, so the route handlers in
:mod:`src.api.routes_cache` stay declarative.

The fields lean on permissive ``dict`` typing on purpose — the underlying
``Metrics.snapshot()`` / ``L1Cache.stats()`` / ``L2Redis.stats()`` shapes are
already well-defined elsewhere and we don't want to duplicate (or risk drifting
from) their structure here. The one contract C15 must hold (project §8) is that
``performance.overall_hit_rate`` and ``performance.total_requests`` are always
present, which the route assembles explicitly.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class StatsResponse(BaseModel):
    """Aggregate cache statistics for ``GET /cache/stats`` and the dashboard.

    Attributes
    ----------
    performance:
        Overall counters — at least ``overall_hit_rate`` (float),
        ``total_requests`` (int), ``hits`` (int), ``misses`` (int).
    tiers:
        Per-tier breakdown with ``l1``/``l2``/``l3`` (and ``backend``) sub-dicts.
    memory:
        Approximate memory accounting: ``l1_mb`` (L1 value footprint),
        ``cap_mb`` (the configured cross-tier cap), and ``total_mb``
        (best-effort total across tiers).
    timing_ms:
        Cached-vs-uncached latency percentiles/averages in milliseconds.
    degraded:
        Whether the L2 (Redis) tier is currently degraded.
    alert:
        A degradation-alert dict, or ``None`` when healthy.
    """

    performance: dict
    tiers: dict
    memory: dict
    timing_ms: dict = Field(default_factory=dict)
    degraded: bool = False
    alert: dict | None = None


class HotKeysResponse(BaseModel):
    """Ranked hot keys for ``GET /cache/hot``."""

    hot: list = Field(default_factory=list)


class PatternReport(BaseModel):
    """Query-pattern analysis surface for ``GET /patterns`` (project §3 Feature A).

    Assembled from :meth:`PatternEngine.analyze` plus
    :meth:`PatternEngine.recommendations`. Fields lean on permissive ``dict`` /
    ``list`` typing for the same reason as the other envelopes here — the
    underlying engine shapes are already well-defined.

    Attributes
    ----------
    temporal:
        ``{"hour_of_day": {0..23: n}, "day_of_week": {0..6: n}}`` — zero-filled
        histograms over the recent observation window.
    per_source:
        Per-source query counts (``None`` sources rendered as ``"unknown"``).
    total_observations:
        Size of the observation window the analysis was computed over.
    recommendations:
        Ranked warming recommendations, each
        ``{"key", "query", "source", "score", "count", "reason"}``.
    """

    temporal: dict
    per_source: dict = Field(default_factory=dict)
    total_observations: int = 0
    recommendations: list = Field(default_factory=list)


class WarmResponse(BaseModel):
    """Acknowledgement for ``POST /cache/warm`` — how many keys were warmed."""

    warmed: int


class InvalidateResponse(BaseModel):
    """Per-tier removal tally for ``POST /cache/invalidate``."""

    l1: int
    l2: int
    l3: int
