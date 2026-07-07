"""Shared contracts for the correlation engine's detectors.

Defines the per-cycle :class:`DetectionContext` handed to every detector, the
:class:`Detector` protocol the engine registers against, a TTL
:class:`DedupeCache` that suppresses re-emission of the same finding, the
shared :data:`FRESHNESS_SECONDS` emission bound, and the tiny scoring/id
helpers every detector shares. Individual detector families
live in sibling modules (``temporal``, ``session`` — C5/C6 add ``cascade``,
``user`` and ``metric``); :class:`src.engine.CorrelationEngine` orchestrates
them all.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from src.aggregation import MetricAggregator
from src.models import Correlation, LogEvent

#: The dedupe cache runs a full expiry sweep only once it grows past this many
#: keys, capping per-call prune work. In practice the key space is tiny (one
#: key per source pair / journey), so the sweep almost never triggers.
_PRUNE_THRESHOLD = 1024

#: Freshness bound on emissions: a detector only emits a relationship whose
#: NEWEST underlying event is at most this many seconds old. Anchoring every
#: emission to current activity keeps per-row detection latency (detected_at
#: minus the newest event's timestamp) <= ~4 s + one detection tick —
#: comfortably inside the pipeline's 5 s real-time contract — and means that
#: once a dedupe TTL lapses, a finding is re-emitted only on genuinely fresh
#: activity, never on stale events still lingering in the sliding window.
FRESHNESS_SECONDS = 4.0


@dataclass
class DetectionContext:
    """Everything one detection cycle exposes to the detectors.

    Attributes:
        now: The cycle's clock (epoch seconds) — simulated in tests, wall time
            in production. Every emitted correlation stamps ``detected_at=now``.
        new_events: Events parsed since the previous detection cycle — the
            "trigger" set detectors scan, so history is not re-detected.
        window_events: Every buffered event still inside the sliding window
            (``settings.window_seconds``), roughly oldest first.
        aggregator: The per-second metric rings (input for the C6 metric
            detector; temporal/session detectors ignore it).
        patterns: The PatternLearner once C7 lands (typed loosely until then).
    """

    now: float
    new_events: list[LogEvent]
    window_events: list[LogEvent]
    aggregator: MetricAggregator | None
    patterns: Any | None = None


class Detector(Protocol):
    """The engine-facing detector contract: a name plus one detect() per cycle."""

    name: str

    def detect(self, ctx: DetectionContext) -> list[Correlation]:
        """Return this cycle's newly detected correlations (possibly empty)."""
        ...


class DedupeCache:
    """TTL set of "already emitted this" keys (dict of key -> expiry seconds).

    Detectors call :meth:`seen` before emitting: the first call for a key
    records it and returns True ("fresh — emit now"); repeat calls within the
    TTL return False ("suppressed"). Expired entries are reclaimed
    opportunistically — a lookup overwrites its own expired entry, and a full
    sweep runs only once the cache outgrows ``_PRUNE_THRESHOLD``, so per-call
    prune work stays capped.
    """

    def __init__(self) -> None:
        self._expiry: dict[str, float] = {}

    def __len__(self) -> int:
        return len(self._expiry)

    def seen(self, key: str, now: float, ttl: float) -> bool:
        """Record ``key`` and return True when it was NOT already live.

        True means "first sighting inside a TTL window — emit"; False means the
        key was recorded less than ``ttl`` seconds ago and must be suppressed.
        Only a True return (re)records the key, so suppressed sightings never
        extend the window.
        """
        expiry = self._expiry.get(key)
        if expiry is not None and expiry > now:
            return False
        self._expiry[key] = now + ttl
        if len(self._expiry) > _PRUNE_THRESHOLD:
            self._prune(now)
        return True

    def _prune(self, now: float) -> None:
        """Drop every expired entry (runs rarely; see class docstring)."""
        for key in [k for k, expiry in self._expiry.items() if expiry <= now]:
            del self._expiry[key]


def clamp01(x: float) -> float:
    """Clamp ``x`` into [0.0, 1.0] — every strength/confidence goes through this."""
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else float(x)


def pair_key(*parts: str) -> str:
    """Canonical dedupe key: parts joined sorted, so any order collides."""
    return "|".join(sorted(str(part) for part in parts))


def new_correlation_id() -> str:
    """A short unique id for an emitted Correlation (uuid4 hex, 12 chars)."""
    return uuid.uuid4().hex[:12]
