"""In-memory, single-process rolling aggregate stats that feed ``GET /api/stats`` (C8).

:class:`StatsAggregator` folds each :meth:`~src.nlp.NLPEngine.analyze` result into a handful
of running tallies — intent / sentiment / entity-type distributions, a rolling trending-keyword
counter (via :class:`src.nlp.keyword.TrendingKeywords`), a bounded newest-first *recent* buffer
and an approximate throughput — and hands them out as one JSON-ready :meth:`snapshot`. It is
the sole data source for the dashboard panels built in C11/C12.

**Scope: process-local and ephemeral by design.** Everything lives in memory in a single
process; there is no persistence and no cross-process sharing (one uvicorn worker owns one
aggregator). A restart resets the numbers — acceptable for a live *rolling* dashboard, and it
keeps the hot analyze path free of any I/O.

**Thread-safety.** The analyze routes are sync ``def`` handlers, so FastAPI runs them in a
threadpool and :meth:`update` is called concurrently from several worker threads. Every public
method (:meth:`update`, :meth:`snapshot`, :meth:`reset`) takes a single non-reentrant
``threading.Lock`` for its whole body, so counter read-modify-writes and the snapshot's
copy-out never interleave (a plain ``counter[k] += 1`` is *not* atomic across threads). The
critical sections are tiny, so lock contention is negligible; correctness is chosen over
micro-optimisation here.

**Robustness.** :meth:`update` never raises on a malformed / partial result dict: a missing or
wrong-typed facet is simply skipped, so one bad line can never take down the stats endpoint.

Unlike the deterministic NLP layer, this module *does* read the wall / monotonic clock — but
only for runtime telemetry (``recent[].ts`` for the UI, and the throughput estimate), never for
anything a test pins to an exact value.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from typing import Any

from src.nlp.keyword import TrendingKeywords

#: Recent-buffer messages are truncated to this many characters — the dashboard's recent feed
#: only needs a preview, and unbounded strings would let one huge log line bloat every snapshot.
_RECENT_MESSAGE_MAXCHARS: int = 200


class StatsAggregator:
    """Thread-safe in-memory rolling aggregates over analyze results; feeds ``GET /api/stats``.

    Construct one per process (see :meth:`src.main.Runtime.build`), call :meth:`update` with each
    :meth:`~src.nlp.NLPEngine.analyze` result dict, and read :meth:`snapshot` from the stats
    endpoint. All three public methods are guarded by one lock and are safe to call concurrently.
    """

    def __init__(self, window: int = 500, trending_top_k: int = 10) -> None:
        """Create an empty aggregator.

        Args:
            window: Max size of the newest-first ``recent`` buffer and of the rolling timestamp
                window used to estimate throughput (a bounded :class:`collections.deque`).
            trending_top_k: How many trending keywords :meth:`snapshot` returns.
        """
        self._window = window
        self._trending_top_k = trending_top_k
        # Distribution tallies — one Counter per facet.
        self._intent_counts: Counter[str] = Counter()
        self._sentiment_counts: Counter[str] = Counter()
        self._entity_type_counts: Counter[str] = Counter()
        # Rolling global keyword frequency (reuses the C6 helper — not reimplemented here).
        self._trending = TrendingKeywords()
        # Bounded newest-at-the-right buffer of compact recent items for the UI feed.
        self._recent: deque[dict[str, Any]] = deque(maxlen=window)
        # Bounded window of monotonic update timestamps used to approximate throughput.
        self._update_monotonic: deque[float] = deque(maxlen=window)
        self._total = 0
        # Non-reentrant: no locked method calls another locked method (see _throughput).
        self._lock = threading.Lock()

    def update(self, result: dict) -> None:
        """Fold one analyze-result dict into the running aggregates. Thread-safe; never raises.

        ``result`` is expected in the :meth:`~src.nlp.NLPEngine.analyze` schema
        (``{message, entities:[{label,...}], intent:{label,...}, sentiment:{label,...},
        keywords:[...]}``) but is treated defensively: a non-dict is ignored outright, and any
        missing / wrong-typed facet is skipped rather than raising, so a malformed line updates
        whatever facets it can (and still counts toward ``total_analyzed``).
        """
        if not isinstance(result, dict):
            # Defensive: analyze() always returns a dict, but garbage in must never raise.
            return

        # Read the clocks outside the lock — they touch no shared state. time.time() is the
        # epoch stamp the UI shows per recent item; time.monotonic() drives the throughput
        # estimate (immune to wall-clock adjustments).
        now_epoch = time.time()
        now_monotonic = time.monotonic()

        intent_label = _facet_label(result.get("intent"))
        sentiment_label = _facet_label(result.get("sentiment"))

        with self._lock:
            self._total += 1

            if intent_label is not None:
                self._intent_counts[intent_label] += 1
            if sentiment_label is not None:
                self._sentiment_counts[sentiment_label] += 1

            entities = result.get("entities")
            if isinstance(entities, list):
                for entity in entities:
                    if isinstance(entity, dict):
                        label = entity.get("label")
                        if isinstance(label, str) and label:
                            self._entity_type_counts[label] += 1

            keywords = result.get("keywords")
            if isinstance(keywords, list):
                # TrendingKeywords.add() calls .strip()/.casefold(), so hand it only strings.
                self._trending.add([kw for kw in keywords if isinstance(kw, str)])

            message = result.get("message")
            if not isinstance(message, str):
                message = ""
            self._recent.append(
                {
                    "message": message[:_RECENT_MESSAGE_MAXCHARS],
                    "intent": intent_label,
                    "sentiment": sentiment_label,
                    "ts": now_epoch,
                }
            )
            self._update_monotonic.append(now_monotonic)

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-ready copy of the current aggregates. Thread-safe.

        Shape (all keys always present)::

            {
                "total_analyzed": int,
                "intent_distribution": {label: count, ...},
                "sentiment_distribution": {label: count, ...},
                "entity_type_distribution": {label: count, ...},
                "trending_keywords": [[keyword, count], ...],   # up to trending_top_k
                "recent": [{message, intent, sentiment, ts}, ...],  # newest first
                "throughput_per_sec": float,                    # approximate, >= 0.0
            }

        Every container is a fresh copy, so the caller can never mutate internal state.
        """
        with self._lock:
            return {
                "total_analyzed": self._total,
                "intent_distribution": dict(self._intent_counts),
                "sentiment_distribution": dict(self._sentiment_counts),
                "entity_type_distribution": dict(self._entity_type_counts),
                "trending_keywords": [
                    [keyword, count]
                    for keyword, count in self._trending.top(self._trending_top_k)
                ],
                # deque appends newest at the right, so reversed() yields newest-first. Each
                # item is shallow-copied so a caller mutating the snapshot can't corrupt state.
                "recent": [dict(item) for item in reversed(self._recent)],
                "throughput_per_sec": self._throughput(),
            }

    def reset(self) -> None:
        """Clear every tally, the recent buffer and the throughput window. Thread-safe."""
        with self._lock:
            self._intent_counts.clear()
            self._sentiment_counts.clear()
            self._entity_type_counts.clear()
            self._trending.reset()
            self._recent.clear()
            self._update_monotonic.clear()
            self._total = 0

    def _throughput(self) -> float:
        """Approximate updates/sec over the rolling window. Caller must hold ``self._lock``.

        With ``n`` timestamps spanning ``span`` seconds there are ``n - 1`` inter-arrival gaps,
        so the mean rate is ``(n - 1) / span``. Degenerate cases (fewer than two samples, or a
        zero/negative span from coarse-clock ties) return ``0.0`` — always a non-negative float.
        """
        samples = self._update_monotonic
        if len(samples) < 2:
            return 0.0
        span = samples[-1] - samples[0]
        if span <= 0.0:
            return 0.0
        return (len(samples) - 1) / span


def _facet_label(facet: Any) -> str | None:
    """Extract a non-empty ``label`` string from an ``{"label": ...}`` facet, else ``None``.

    Used for the ``intent`` and ``sentiment`` facets: tolerates the facet being absent
    (``None``), not a dict, or missing / wrong-typed ``label``, always returning either a
    usable label or ``None`` so callers never have to re-check.
    """
    if isinstance(facet, dict):
        label = facet.get("label")
        if isinstance(label, str) and label:
            return label
    return None
