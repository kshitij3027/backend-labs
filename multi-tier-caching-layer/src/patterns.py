"""Heuristic query-pattern engine (Feature Area A — no ML libraries).

This module learns *which* queries are worth pre-warming using a transparent
**frecency-with-cost** heuristic rather than scikit-learn. Internet research on
cache-warming (and the project plan) confirms that frequency x recency x cost
scoring rivals a trained model for this workload while keeping the image small,
the behaviour explainable, and the tests deterministic.

What it satisfies (project_requirements.md §3 Feature Area A):

* **Record every query** with timestamp, normalized form, response time, and
  hour-of-day (we also keep day-of-week and the originating source).
* **Temporal analysis** — hour-of-day and day-of-week histograms surface
  Monday-morning spikes / end-of-day reporting.
* **User/team analysis** — a per-``source`` counter ranks who queries most.
* **Ranked warming recommendations** — every tracked key is scored by
  ``frequency x recency x cost`` and returned newest-/hottest-first.

Design notes
------------
* **stdlib only**: ``dataclasses``, ``collections.deque``, ``math`` (unused
  beyond clarity), ``time``, ``datetime``. No numpy / scikit-learn.
* **Two state structures.** A bounded ``deque(maxlen=history_size)`` of raw
  :class:`QueryObservation` drives the temporal/per-source histograms (it is a
  sliding window over the most recent traffic). A separate per-key aggregate
  dict holds the long-lived counters used for scoring; it is itself capped at
  ``history_size`` entries and evicts the least-recently-seen key when full.
* **Injectable clock.** ``timer`` defaults to ``time.time`` but tests pass a
  mutable fake clock so recency decay is exercised without real sleeps. We use
  ``time.time`` (wall clock) rather than ``monotonic`` because hour-of-day /
  day-of-week derivation needs a real epoch timestamp.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Deque, Optional


@dataclass(frozen=True)
class QueryObservation:
    """A single recorded query event.

    Frozen so observations are immutable once appended to the history window.

    Attributes
    ----------
    key:
        Canonical cache key for the query (see :mod:`src.keys`).
    query:
        The normalized query string.
    source:
        Originating user/team/service, or ``None`` if unattributed.
    response_time_ms:
        How long the underlying query took to serve, in milliseconds.
    ts:
        Unix epoch seconds at which the query was observed.
    hour_of_day:
        UTC hour-of-day (0-23) derived from ``ts``.
    day_of_week:
        UTC day-of-week derived from ``ts`` (Monday=0 .. Sunday=6).
    """

    key: str
    query: str
    source: Optional[str]
    response_time_ms: float
    ts: float
    hour_of_day: int
    day_of_week: int


class PatternEngine:
    """Heuristic frecency-with-cost engine for cache-warming recommendations.

    The engine is the single source of truth for "what should we warm next?".
    :class:`~src.cache_manager.CacheManager` (C11) calls :meth:`record_query`
    on every served query; the warmer (C14) and the ``/patterns`` and
    ``/cache/hot`` endpoints consume :meth:`analyze`, :meth:`recommendations`,
    and :meth:`hot_keys`.
    """

    def __init__(
        self,
        *,
        history_size: int = 5000,
        freq_weight: float = 1.0,
        recency_weight: float = 1.0,
        cost_weight: float = 1.0,
        recency_half_life_seconds: float = 3600.0,
        timer: Callable[[], float] = time.time,
    ) -> None:
        self._history_size = history_size
        self._freq_weight = freq_weight
        self._recency_weight = recency_weight
        self._cost_weight = cost_weight
        # Guard against a non-positive half-life (would divide by zero / blow up).
        self._half_life = recency_half_life_seconds if recency_half_life_seconds > 0 else 1.0
        self._timer = timer

        # Sliding window of raw events for temporal / per-source analysis.
        self._observations: Deque[QueryObservation] = deque(maxlen=history_size)
        # Long-lived per-key aggregates used for scoring.
        # {key: {"query", "source", "count", "last_seen", "cost_ema_ms"}}
        self._aggregates: dict[str, dict] = {}

    # -- recording -------------------------------------------------------

    def record_query(
        self,
        key: str,
        query: str,
        source: Optional[str],
        response_time_ms: float,
        *,
        ts: Optional[float] = None,
    ) -> None:
        """Record one served query.

        ``ts`` defaults to ``self._timer()``. ``hour_of_day`` and
        ``day_of_week`` are derived from ``ts`` in **UTC** so the histograms are
        timezone-stable across hosts. The observation is appended to the bounded
        history window and the per-key aggregate is updated:

        * ``count`` increments,
        * ``last_seen`` is set to ``ts`` (used by the recency decay),
        * ``cost_ema_ms`` becomes an exponential moving average of the observed
          response times (so a key's cost tracks recent latency rather than an
          all-time average), seeded on first sight with the raw value.
        """
        if ts is None:
            ts = self._timer()

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        hour_of_day = dt.hour
        day_of_week = dt.weekday()  # Monday=0 .. Sunday=6

        self._observations.append(
            QueryObservation(
                key=key,
                query=query,
                source=source,
                response_time_ms=response_time_ms,
                ts=ts,
                hour_of_day=hour_of_day,
                day_of_week=day_of_week,
            )
        )

        agg = self._aggregates.get(key)
        if agg is None:
            self._maybe_evict()
            self._aggregates[key] = {
                "query": query,
                "source": source,
                "count": 1,
                "last_seen": ts,
                "cost_ema_ms": float(response_time_ms),
            }
        else:
            agg["count"] += 1
            agg["last_seen"] = ts
            # EMA with alpha=0.3 — recent latency weighted more than the tail.
            alpha = 0.3
            agg["cost_ema_ms"] = (
                alpha * float(response_time_ms) + (1.0 - alpha) * agg["cost_ema_ms"]
            )
            # Keep the freshest query text / source for the key.
            agg["query"] = query
            agg["source"] = source

    def _maybe_evict(self) -> None:
        """Evict the least-recently-seen aggregate if the dict is at capacity."""
        if len(self._aggregates) < self._history_size:
            return
        # Find and drop the entry with the oldest last_seen.
        oldest_key = min(
            self._aggregates,
            key=lambda k: self._aggregates[k]["last_seen"],
        )
        del self._aggregates[oldest_key]

    # -- analysis --------------------------------------------------------

    def analyze(self) -> dict:
        """Summarise the recent observation window.

        Returns a dict with **full, zero-filled** hour-of-day (0-23) and
        day-of-week (0-6) histograms — every bucket is always present so
        downstream consumers (dashboard, ``/patterns``) need no defaulting — a
        ``per_source`` counter (only sources actually seen, with ``None``
        rendered as the string ``"unknown"``), and the total observation count.
        """
        hour_hist = {h: 0 for h in range(24)}
        day_hist = {d: 0 for d in range(7)}
        per_source: dict[str, int] = {}

        for obs in self._observations:
            hour_hist[obs.hour_of_day] += 1
            day_hist[obs.day_of_week] += 1
            source = obs.source if obs.source is not None else "unknown"
            per_source[source] = per_source.get(source, 0) + 1

        return {
            "hour_of_day": hour_hist,
            "day_of_week": day_hist,
            "per_source": per_source,
            "total_observations": len(self._observations),
        }

    # -- scoring ---------------------------------------------------------

    def _recency(self, last_seen: float, now: float) -> float:
        """Exponential recency decay in ``(0, 1]`` — 1.0 when just seen."""
        age = max(0.0, now - last_seen)
        return 0.5 ** (age / self._half_life)

    def _score(self, agg: dict, now: float) -> float:
        """Frecency-with-cost score for one aggregate.

        ``score = (count ** freq_weight)
                   * (recency ** recency_weight)
                   * (cost_ema_ms ** cost_weight)``

        where ``recency = 0.5 ** (age / half_life)`` lies in ``(0, 1]``. The
        score is **monotonically increasing** in each factor independently:
        higher ``count`` -> higher score; more recent ``last_seen`` (larger
        recency) -> higher score; higher ``cost_ema_ms`` -> higher score (with
        the other two held equal). Cost is floored at ``1e-9`` so a zero-latency
        observation cannot zero out an otherwise hot key.
        """
        recency = self._recency(agg["last_seen"], now)
        cost = max(agg["cost_ema_ms"], 1e-9)
        return (
            (agg["count"] ** self._freq_weight)
            * (recency ** self._recency_weight)
            * (cost ** self._cost_weight)
        )

    # -- recommendations -------------------------------------------------

    def recommendations(self, top_n: int = 20) -> list[dict]:
        """Return the ``top_n`` keys to warm, ranked by score descending.

        Every tracked key is scored at ``now = self._timer()``. Each item is a
        dict ``{"key", "query", "source", "score", "count", "reason"}`` where
        ``reason`` is a short human-readable breakdown of the three factors.
        """
        now = self._timer()
        scored: list[tuple[float, str, dict]] = []
        for key, agg in self._aggregates.items():
            score = self._score(agg, now)
            scored.append((score, key, agg))

        # Sort by score desc; tie-break on key for stable, deterministic output.
        scored.sort(key=lambda item: (-item[0], item[1]))

        results: list[dict] = []
        for score, key, agg in scored[:top_n]:
            recency = self._recency(agg["last_seen"], now)
            results.append(
                {
                    "key": key,
                    "query": agg["query"],
                    "source": agg["source"],
                    "score": score,
                    "count": agg["count"],
                    "reason": (
                        f"freq={agg['count']} "
                        f"recency={recency:.3f} "
                        f"cost={agg['cost_ema_ms']:.1f}ms"
                    ),
                }
            )
        return results

    def hot_keys(self, top_n: int = 20) -> list[dict]:
        """Return the ``top_n`` hottest keys by the same frecency-with-cost score.

        Same ranking as :meth:`recommendations` but a leaner shape:
        ``{"key", "query", "score", "count"}``.
        """
        now = self._timer()
        scored: list[tuple[float, str, dict]] = []
        for key, agg in self._aggregates.items():
            scored.append((self._score(agg, now), key, agg))

        scored.sort(key=lambda item: (-item[0], item[1]))

        return [
            {
                "key": key,
                "query": agg["query"],
                "score": score,
                "count": agg["count"],
            }
            for score, key, agg in scored[:top_n]
        ]
