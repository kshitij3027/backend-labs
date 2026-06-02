"""Cache metrics aggregator and degradation-alert detection.

Tracks per-tier hit/miss counts, an overall hit rate, total requests, and
rolling cached-vs-uncached timing samples. Produces a nested ``snapshot()``
dict (the shape consumed by ``GET /cache/stats`` and the dashboard, per
project_requirements.md §8) plus a bounded hit-rate ``series()`` for charting.

A degradation alert fires when the hit rate falls below a threshold under
sufficient load, or when L2 (Redis) has been externally flagged as degraded.

Standard library only (``collections.deque`` for bounded rolling windows).
The cache manager later fills in the ``memory`` block of the snapshot — we
leave it as an empty-dict placeholder here.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Deque, Optional

# Tiers whose hits count toward the cache hit rate.
HIT_TIERS = ("l1", "l2", "l3")
# Tiers representing a full miss (served by the slow source of truth).
MISS_TIERS = ("backend",)

# Cap on retained raw timing samples per bucket — bounds memory while keeping
# percentiles representative of recent traffic.
_TIMING_MAXLEN = 1000


class Metrics:
    """Aggregates cache hit/miss counts, timing, and degradation signals.

    The instance is not internally locked; the cache manager serializes
    ``record_request`` calls (FastAPI request handling is single-threaded per
    event loop for the increment path). Counters are plain ints and deques,
    which are themselves thread-safe for append, so reads via ``snapshot`` are
    consistent enough for a monitoring surface.
    """

    def __init__(
        self,
        *,
        degradation_threshold: float = 0.5,
        history_points: int = 60,
        min_requests_for_alert: int = 20,
    ) -> None:
        self.degradation_threshold = degradation_threshold
        self.history_points = history_points
        self.min_requests_for_alert = min_requests_for_alert

        # Per-tier request counts (hits for HIT_TIERS, misses for MISS_TIERS).
        self._tier_counts: dict[str, int] = {
            tier: 0 for tier in (*HIT_TIERS, *MISS_TIERS)
        }
        self.total_requests: int = 0

        # Rolling raw timing samples (milliseconds) for cached vs uncached.
        self._cached_times: Deque[float] = deque(maxlen=_TIMING_MAXLEN)
        self._uncached_times: Deque[float] = deque(maxlen=_TIMING_MAXLEN)

        # Bounded history of overall hit-rate snapshots for the dashboard chart.
        self._series: Deque[float] = deque(maxlen=history_points)

        # Externally-set flag: True when the L2/Redis tier is degraded.
        self.l2_degraded: bool = False

    # -- recording -------------------------------------------------------

    def record_request(self, tier: str, elapsed_ms: float) -> None:
        """Record one served request.

        ``tier`` is the tier that served the result (``l1``/``l2``/``l3`` for a
        cache hit, ``backend`` for a full miss). ``elapsed_ms`` is the wall time
        for that request in milliseconds. Unknown tiers still bump the total and
        are treated as uncached for timing purposes.
        """
        if tier in self._tier_counts:
            self._tier_counts[tier] += 1
        else:
            # Tolerate unexpected tier labels without losing the request count.
            self._tier_counts[tier] = self._tier_counts.get(tier, 0) + 1
        self.total_requests += 1

        if tier in HIT_TIERS:
            self._cached_times.append(float(elapsed_ms))
        else:
            self._uncached_times.append(float(elapsed_ms))

        # Snapshot the running hit rate after this request for the chart.
        self._series.append(self.overall_hit_rate)

    def mark_l2_degraded(self, degraded: bool) -> None:
        """Set the external L2-degraded flag (driven by the Redis tier)."""
        self.l2_degraded = bool(degraded)

    # -- derived metrics -------------------------------------------------

    @property
    def overall_hit_rate(self) -> float:
        """Fraction of requests served from a cache tier (0.0 when idle)."""
        if self.total_requests == 0:
            return 0.0
        hits = sum(self._tier_counts.get(tier, 0) for tier in HIT_TIERS)
        return hits / self.total_requests

    @property
    def _hits(self) -> int:
        return sum(self._tier_counts.get(tier, 0) for tier in HIT_TIERS)

    @property
    def _misses(self) -> int:
        return sum(self._tier_counts.get(tier, 0) for tier in MISS_TIERS)

    @staticmethod
    def _avg(samples: Deque[float] | list[float]) -> float:
        """Mean of ``samples``, or 0.0 when empty."""
        if not samples:
            return 0.0
        return sum(samples) / len(samples)

    @staticmethod
    def _percentile(samples: Deque[float] | list[float], p: float) -> float:
        """Return the ``p``-th percentile (0..100) of ``samples``.

        Uses nearest-rank on the sorted samples; returns 0.0 when empty. ``p``
        is clamped to the [0, 100] range so callers can pass values like 90.
        """
        if not samples:
            return 0.0
        ordered = sorted(samples)
        if p <= 0:
            return ordered[0]
        if p >= 100:
            return ordered[-1]
        # Nearest-rank: rank = ceil(p/100 * N), 1-indexed.
        rank = math.ceil(p / 100.0 * len(ordered))
        rank = max(1, min(rank, len(ordered)))
        return ordered[rank - 1]

    # -- reporting -------------------------------------------------------

    def degradation_alert(self) -> Optional[dict]:
        """Return an alert dict when cache performance is degraded, else None.

        Fires when either:
          * enough traffic has accumulated (``total_requests`` >=
            ``min_requests_for_alert``) and the overall hit rate is below
            ``degradation_threshold``, or
          * the L2 tier has been externally flagged degraded.
        """
        if self.l2_degraded:
            return {
                "reason": "l2_degraded",
                "hit_rate": self.overall_hit_rate,
                "threshold": self.degradation_threshold,
                "total_requests": self.total_requests,
            }
        if (
            self.total_requests >= self.min_requests_for_alert
            and self.overall_hit_rate < self.degradation_threshold
        ):
            return {
                "reason": "low_hit_rate",
                "hit_rate": self.overall_hit_rate,
                "threshold": self.degradation_threshold,
                "total_requests": self.total_requests,
            }
        return None

    def snapshot(self) -> dict:
        """Return a nested metrics snapshot for the stats endpoint/dashboard.

        Shape (the ``memory`` block is populated later by the cache manager)::

            {
              "performance": {overall_hit_rate, total_requests, hits, misses},
              "tiers": {
                "l1": {"hits": n}, "l2": {"hits": n}, "l3": {"hits": n},
                "backend": {"misses": n},
              },
              "timing_ms": {
                cached_p50, cached_p90, cached_avg,
                uncached_p50, uncached_p90, uncached_avg,
              },
              "memory": {},          # placeholder, filled by cache manager
              "degraded": bool,
              "alert": <degradation_alert() or None>,
            }
        """
        return {
            "performance": {
                "overall_hit_rate": self.overall_hit_rate,
                "total_requests": self.total_requests,
                "hits": self._hits,
                "misses": self._misses,
            },
            "tiers": {
                "l1": {"hits": self._tier_counts.get("l1", 0)},
                "l2": {"hits": self._tier_counts.get("l2", 0)},
                "l3": {"hits": self._tier_counts.get("l3", 0)},
                "backend": {"misses": self._tier_counts.get("backend", 0)},
            },
            "timing_ms": {
                "cached_p50": self._percentile(self._cached_times, 50),
                "cached_p90": self._percentile(self._cached_times, 90),
                "cached_avg": self._avg(self._cached_times),
                "uncached_p50": self._percentile(self._uncached_times, 50),
                "uncached_p90": self._percentile(self._uncached_times, 90),
                "uncached_avg": self._avg(self._uncached_times),
            },
            "memory": {},
            "degraded": self.l2_degraded,
            "alert": self.degradation_alert(),
        }

    def series(self, n: Optional[int] = None) -> dict:
        """Return the recent hit-rate history for charting.

        ``{"hit_rate": [...]}`` containing the last ``n`` snapshots (all
        retained points when ``n`` is None). The deque is already capped at
        ``history_points``.
        """
        points = list(self._series)
        if n is not None:
            points = points[-n:]
        return {"hit_rate": points}
