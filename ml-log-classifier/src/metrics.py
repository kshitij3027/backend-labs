"""Live-metrics aggregation + WebSocket fan-out for the dashboard (Commit 10).

This module is the **single source of truth** for "what has the classifier done
so far" and the plumbing that streams that picture to the React dashboard in real
time. It contains exactly two collaborators and nothing model-specific:

1. :class:`MetricsAggregator` — a thread-safe tally of every classification the
   service performs. ``POST /classify`` (and the batch/stream routes) run in
   FastAPI's worker threadpool, so several threads call :meth:`record`
   concurrently; a single :class:`threading.Lock` makes each update atomic and
   guarantees :meth:`snapshot` never observes a torn state. It tracks the running
   total (the authoritative ``total_classified`` that ``GET /stats`` now reads),
   per-label severity/category/service distributions, a running average
   confidence, a bounded ring buffer of the most-recent predictions, and a sliding
   window of record timestamps used to estimate live throughput. :meth:`snapshot`
   renders all of that as a plain, JSON-serializable dict.

2. :class:`ConnectionManager` — a minimal WebSocket fan-out registry. It accepts
   connections, drops them on disconnect, and :meth:`broadcast`-s a message to all
   live sockets, tolerating sockets that have died mid-flight (one dead client must
   never break the broadcast loop or take the others down with it).

The aggregator is deliberately decoupled from the model: it consumes the result
dict the classifier *already* produced (so metrics never re-run inference) plus
the originating ``raw_log``. The periodic broadcaster task and the
``/ws/metrics`` + ``GET /metrics`` routes that drive these objects live in
:mod:`src.api`.

Out of scope here (later commits): multi-service hierarchical metrics, the
adaptive-retraining feedback signals and A/B serving stats.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    from fastapi import WebSocket


#: How many most-recent predictions the ring buffer keeps for the live feed.
RECENT_MAXLEN = 50

#: How many characters of each raw log we retain in a recent-prediction entry.
#: Keeps the WebSocket snapshot compact even under a flood of long log lines.
RAW_LOG_PREVIEW_LEN = 120

#: Sliding window (seconds) over which record timestamps are retained for
#: throughput estimation. Anything older than this is pruned on every
#: :meth:`MetricsAggregator.record`.
THROUGHPUT_WINDOW_SEC = 60.0

#: The shorter sub-window (seconds) actually used to compute the headline
#: ``throughput_per_sec`` so the figure reacts quickly to bursts/lulls. Must be
#: ``<= THROUGHPUT_WINDOW_SEC``.
THROUGHPUT_RATE_WINDOW_SEC = 10.0


class MetricsAggregator:
    """Thread-safe live tally of classifications, rendered as JSON snapshots.

    Every classified log is reported via :meth:`record`; the dashboard reads the
    aggregate via :meth:`snapshot` (over the WebSocket and ``GET /metrics``) and
    ``GET /stats`` reads :attr:`total_classified` directly. All mutating and
    reading paths take :attr:`_lock`, because classification runs in FastAPI's
    threadpool (many writer threads) while the broadcaster reads snapshots from the
    event-loop thread.

    Attributes:
        total_classified: Authoritative count of logs classified since start. This
            is the single source of truth for ``/stats`` and the snapshot total.
        severity_counts: ``{severity_label: count}``.
        category_counts: ``{category_label: count}``.
        service_counts: ``{service_name: count}`` — only populated for results that
            carry a ``service`` key (the base classifier omits it, so this stays
            empty until the multi-service work lands).
        confidence_sum: Running sum of per-record ``confidence`` (paired with
            :attr:`confidence_count`) to derive the average without storing every
            value.
        confidence_count: Number of confidence values summed into
            :attr:`confidence_sum`.
        recent: Bounded :class:`collections.deque` (``maxlen=RECENT_MAXLEN``) of
            compact recent-prediction dicts for the live feed.
        _timestamps: Sliding window of record times (monotonic-friendly wall
            seconds) used to estimate throughput; pruned to
            :data:`THROUGHPUT_WINDOW_SEC` on every record.
        started_at: Wall-clock time the aggregator was created (for ``uptime_sec``).
        model_status: Mirror of the service's model lifecycle status
            (``"ready"`` / ``"training"`` / ``"untrained"``), updated via
            :meth:`set_status`.
        current_version: Active registry version id, or ``None``; updated via
            :meth:`set_status`.
    """

    def __init__(self) -> None:
        """Create an empty aggregator with status ``"ready"`` and no version."""
        self._lock = threading.Lock()

        self.total_classified: int = 0
        self.severity_counts: Counter = Counter()
        self.category_counts: Counter = Counter()
        self.service_counts: Counter = Counter()

        self.confidence_sum: float = 0.0
        self.confidence_count: int = 0

        self.recent: Deque[Dict[str, Any]] = deque(maxlen=RECENT_MAXLEN)
        self._timestamps: Deque[float] = deque()

        self.started_at: float = time.time()
        self.model_status: str = "ready"
        self.current_version: Optional[str] = None

    # -- recording ---------------------------------------------------------

    def record(self, result: Dict[str, Any], raw_log: Optional[str] = None) -> None:
        """Fold one classification ``result`` into the running tallies.

        Intended to be called **once per classified log** (single classify -> 1
        call; batch -> one call per result; stream -> one call per yielded line),
        replacing the old ``Counter.increment`` so the aggregator owns the
        authoritative total. The ``result`` dict is the one the classifier already
        produced — no inference is re-run here.

        Under :attr:`_lock` this:

        * increments :attr:`total_classified`;
        * bumps the severity / category counts (and the service count *iff* the
          result carries a non-empty ``service``);
        * folds ``confidence`` into the running sum/count for the average;
        * appends a compact entry (truncated ``raw_log``, labels, confidence and a
          wall-clock ``ts``) to the :attr:`recent` ring buffer;
        * records "now" in the throughput window and prunes entries older than
          :data:`THROUGHPUT_WINDOW_SEC`.

        Args:
            result: A classification result dict with at least ``severity`` /
                ``category`` / ``confidence`` (the shape
                :meth:`src.ensemble.LogClassifier.classify` returns); an optional
                ``service`` key is honored when present.
            raw_log: The original log text, truncated to
                :data:`RAW_LOG_PREVIEW_LEN` chars for the recent-prediction feed.
        """
        now = time.time()
        severity = str(result.get("severity", "UNKNOWN"))
        category = str(result.get("category", "UNKNOWN"))
        # ``confidence`` may be missing/None on a malformed result — treat as 0.0.
        confidence = float(result.get("confidence") or 0.0)
        service = result.get("service")

        preview = ""
        if raw_log:
            preview = str(raw_log)[:RAW_LOG_PREVIEW_LEN]

        entry = {
            "raw_log": preview,
            "severity": severity,
            "category": category,
            "confidence": confidence,
            "ts": now,
        }

        with self._lock:
            self.total_classified += 1
            self.severity_counts[severity] += 1
            self.category_counts[category] += 1
            if service:  # only track services that are actually present
                self.service_counts[str(service)] += 1

            self.confidence_sum += confidence
            self.confidence_count += 1

            self.recent.append(entry)

            self._timestamps.append(now)
            self._prune_timestamps(now)

    def set_status(
        self,
        model_status: Optional[str] = None,
        current_version: Optional[str] = None,
    ) -> None:
        """Update the cached model status / version reflected in snapshots.

        Called by the training lifecycle in :mod:`src.api` (``"training"`` when a
        retrain starts, ``"ready"`` + the new version on a successful hot-swap) so
        the dashboard and ``GET /metrics`` agree with ``/stats`` and
        ``/train/status``. Either argument may be ``None`` to leave that field
        unchanged.

        Args:
            model_status: New lifecycle status, or ``None`` to keep the current one.
            current_version: New active version id, or ``None`` to keep the current
                one.
        """
        with self._lock:
            if model_status is not None:
                self.model_status = model_status
            if current_version is not None:
                self.current_version = current_version

    # -- derived ----------------------------------------------------------

    def _prune_timestamps(self, now: float) -> None:
        """Drop throughput-window entries older than :data:`THROUGHPUT_WINDOW_SEC`.

        Must be called while holding :attr:`_lock`. Pops from the left of the deque
        (oldest first) until the front is within the window.
        """
        cutoff = now - THROUGHPUT_WINDOW_SEC
        ts = self._timestamps
        while ts and ts[0] < cutoff:
            ts.popleft()

    def _throughput_locked(self, now: Optional[float] = None) -> float:
        """Compute throughput (records/sec) — caller must hold :attr:`_lock`.

        Counts records whose timestamp falls in the last
        :data:`THROUGHPUT_RATE_WINDOW_SEC` seconds and divides by that window width,
        giving a quick-reacting short-window rate. Returns ``0.0`` when the window is
        empty.
        """
        if now is None:
            now = time.time()
        cutoff = now - THROUGHPUT_RATE_WINDOW_SEC
        recent_count = 0
        # Scan from the right (newest) and stop once we fall out of the window.
        for ts in reversed(self._timestamps):
            if ts >= cutoff:
                recent_count += 1
            else:
                break
        if THROUGHPUT_RATE_WINDOW_SEC <= 0:
            return 0.0
        return round(recent_count / THROUGHPUT_RATE_WINDOW_SEC, 4)

    def throughput_per_sec(self) -> float:
        """Estimate the current classification throughput in records/second.

        Uses the last :data:`THROUGHPUT_RATE_WINDOW_SEC` seconds of records divided
        by that window length (e.g. 30 records in the last 10s -> ``3.0``/s). This is
        a short, fast-reacting window over the longer
        :data:`THROUGHPUT_WINDOW_SEC` retention window. Thread-safe.
        """
        with self._lock:
            return self._throughput_locked()

    def _avg_confidence_locked(self) -> float:
        """Mean per-record confidence — caller must hold :attr:`_lock` (0.0 if none)."""
        if self.confidence_count == 0:
            return 0.0
        return round(self.confidence_sum / self.confidence_count, 4)

    def snapshot(self) -> Dict[str, Any]:
        """Render a JSON-serializable point-in-time view of all metrics.

        Taken atomically under :attr:`_lock` so the broadcaster (event-loop thread)
        never reads a tally mid-update. Every value is a native Python type
        (``int`` / ``float`` / ``str`` / ``dict`` / ``list``), so it serializes
        directly via ``ws.send_json`` / a FastAPI JSON response with no remapping.

        Returns:
            A dict with these keys::

                {
                  "total_classified": <int>,
                  "severity_distribution": {<label>: <int>, ...},
                  "category_distribution": {<label>: <int>, ...},
                  "service_distribution": {<service>: <int>, ...},  # often empty
                  "avg_confidence": <float>,            # rounded to 4dp
                  "throughput_per_sec": <float>,        # rounded to 4dp
                  "recent_predictions": [               # newest LAST, <= RECENT_MAXLEN
                     {"raw_log": <str>, "severity": <str>,
                      "category": <str>, "confidence": <float>, "ts": <float>},
                     ...
                  ],
                  "model_status": <str>,
                  "current_version": <str | None>,
                  "uptime_sec": <float>,                # rounded to 3dp
                }
        """
        now = time.time()
        with self._lock:
            return {
                "total_classified": int(self.total_classified),
                "severity_distribution": dict(self.severity_counts),
                "category_distribution": dict(self.category_counts),
                "service_distribution": dict(self.service_counts),
                "avg_confidence": self._avg_confidence_locked(),
                "throughput_per_sec": self._throughput_locked(now),
                "recent_predictions": list(self.recent),
                "model_status": self.model_status,
                "current_version": self.current_version,
                "uptime_sec": round(now - self.started_at, 3),
            }


class ConnectionManager:
    """Tracks live ``/ws/metrics`` WebSocket clients and fans snapshots out to them.

    Kept intentionally tiny: the periodic broadcaster (in :mod:`src.api`) is the
    *only* coroutine that calls :meth:`broadcast`, so there is no concurrent-send
    contention on a given socket. :meth:`broadcast` iterates a **copy** of the
    client list and removes any socket whose send fails, so a client that vanished
    between snapshots can never break the loop for the others.

    Attributes:
        active: The currently-connected WebSocket clients.
    """

    def __init__(self) -> None:
        """Start with no connected clients."""
        self.active: List["WebSocket"] = []

    async def connect(self, websocket: "WebSocket") -> None:
        """Accept the handshake and register ``websocket`` as an active client.

        Args:
            websocket: The incoming :class:`fastapi.WebSocket` to accept and track.
        """
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: "WebSocket") -> None:
        """Deregister ``websocket`` if present (idempotent; never raises).

        Args:
            websocket: The socket to forget. A double-disconnect (e.g. both the
                endpoint's ``WebSocketDisconnect`` handler and a failed broadcast)
                is harmless.
        """
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        """Send ``message`` as JSON to every live client, pruning dead sockets.

        Iterates a snapshot copy of :attr:`active` (so removals during the loop are
        safe) and calls ``ws.send_json(message)`` on each. Any socket that raises is
        collected and removed afterwards rather than aborting the broadcast — one
        dead client must not starve the rest.

        Args:
            message: A JSON-serializable dict (typically
                :meth:`MetricsAggregator.snapshot`).
        """
        dead: List["WebSocket"] = []
        for websocket in list(self.active):
            try:
                await websocket.send_json(message)
            except Exception:  # noqa: BLE001 - any send failure means the socket is gone
                dead.append(websocket)
        for websocket in dead:
            self.disconnect(websocket)
