"""Single sliding-window instance combining buffer, stats, and min/max.

A :class:`SlidingWindow` owns:

* a bounded FIFO buffer of :class:`Event` objects,
* an :class:`IncrementalStats` aggregate kept perfectly in sync with the
  buffer's contents, and
* a :class:`MonotonicMinMax` also kept in sync for O(1) min/max queries.

The window expires events in two ways:

1. **Time-based** — any event whose ``timestamp`` is older than
   ``now - window_size`` is considered stale and removed.
2. **Size-based** — if the buffer would exceed ``max_size``, the oldest
   event is evicted to make room. This is a safety net against runaway
   memory growth during ingest bursts.

Both forms of expiry update ``_stats`` and ``_minmax`` so that snapshots
always reflect exactly the events currently in the buffer.

Commit 7 adds :meth:`state_dict` / :meth:`load_state` for the Redis
checkpoint loop (see :mod:`src.checkpoint`). These capture the event
buffer (not the derived stats) and rebuild the aggregates via ``add``
on restore — simpler and safer than round-tripping the stats objects.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from src.models import Event, WindowResult
from src.stats import IncrementalStats, MonotonicMinMax


class SlidingWindow:
    """A single sliding window over one metric stream.

    Parameters
    ----------
    name:
        Display/identifier name (e.g. ``"response_time_1m"``).
    resolution:
        Human-readable resolution label (``"1s"``, ``"1m"``, ``"5m"``, ...).
    window_size:
        Time-width of the window in seconds. Events older than
        ``now - window_size`` are expired.
    slide_interval:
        Recomputation cadence in seconds. Stored for the window manager;
        :class:`SlidingWindow` itself does not schedule anything.
    max_size:
        Hard cap on how many events may be buffered simultaneously.
        Protects memory during bursty ingest.
    """

    def __init__(
        self,
        name: str,
        resolution: str,
        window_size: float,
        slide_interval: float,
        max_size: int,
    ) -> None:
        self.name = name
        self.resolution = resolution
        self.window_size = window_size
        self.slide_interval = slide_interval
        self.max_size = max_size
        # maxlen would silently drop oldest entries without letting us
        # update the stats/minmax — so we manage eviction manually in add().
        self._buffer: deque[Event] = deque()
        self._stats = IncrementalStats()
        self._minmax = MonotonicMinMax()

    def add(self, event: Event) -> None:
        """Ingest a single event, evicting stale entries as needed.

        The ordering here matters:

        1. Evict time-expired events first so we don't count them toward
           the ``max_size`` cap.
        2. Evict one more event if we are still at ``max_size`` (the new
           event would push us over the cap).
        3. Append the new event and update aggregates.
        4. Expire the monotonic deques against the current cutoff. We do
           this last so that in the common case where the new event is
           itself the running min or max, the deques already contain it
           before any stale monotonic entries are dropped.
        """
        cutoff = event.timestamp - self.window_size

        # 1. Time-based expiry from the head of the buffer.
        while self._buffer and self._buffer[0].timestamp < cutoff:
            expired = self._buffer.popleft()
            self._stats.remove(expired.value)

        # 2. Size-cap eviction: drop the oldest event to make room. Note
        #    that its timestamp may still be within the time window, so
        #    the time-based ``cutoff`` alone is not enough to prune the
        #    monotonic deques — we derive an effective cutoff below.
        if len(self._buffer) >= self.max_size:
            expired = self._buffer.popleft()
            self._stats.remove(expired.value)

        # 3. Append + incremental update.
        self._buffer.append(event)
        self._stats.add(event.value)
        self._minmax.add(event.timestamp, event.value)

        # 4. Re-sync the monotonic deques. We prune against the timestamp
        #    of the oldest still-buffered event, which is >= the time-based
        #    cutoff and also guarantees that any entry evicted via the
        #    size-cap path is dropped from the min/max deques as well.
        self._minmax.expire_before(self._buffer[0].timestamp)

    def snapshot(self, now: float) -> WindowResult:
        """Compute a :class:`WindowResult` as of wall-clock time ``now``.

        This performs *lazy* time-based expiry, meaning the caller can
        request a snapshot long after the last :meth:`add` and still get
        a correct result: events that have since aged out are flushed
        before the stats are read.
        """
        cutoff = now - self.window_size

        # Drain any events that have aged out since the last mutation.
        while self._buffer and self._buffer[0].timestamp < cutoff:
            expired = self._buffer.popleft()
            self._stats.remove(expired.value)
        self._minmax.expire_before(cutoff)

        # When the window is empty, surface zeros for min/max (rather than
        # whatever residue happened to be in the deques) so consumers can
        # treat "count == 0" as an unambiguous signal.
        has_events = self._stats.count > 0
        return WindowResult(
            window_name=self.name,
            resolution=self.resolution,
            window_start=cutoff,
            window_end=now,
            count=self._stats.count,
            sum=self._stats.total,
            average=self._stats.mean,
            min=self._minmax.min if has_events else 0.0,
            max=self._minmax.max if has_events else 0.0,
            std_dev=self._stats.std_dev,
        )

    def size(self) -> int:
        """Number of events currently buffered."""
        return len(self._buffer)

    def state_dict(self) -> dict[str, Any]:
        """Serialize the window's current state as a JSON-safe dict.

        Captures:
          - configuration (name, resolution, window_size, slide_interval, max_size)
          - the full event buffer as a list of dicts with
            ``event_id``, ``timestamp``, ``value``, ``metric``, ``metadata``.

        We re-derive stats from the events on restore rather than
        serializing the :class:`IncrementalStats` / :class:`MonotonicMinMax`
        directly — that keeps the snapshot much simpler and sidesteps any
        risk of inconsistency between the buffer and its aggregates.
        """
        return {
            "name": self.name,
            "resolution": self.resolution,
            "window_size": self.window_size,
            "slide_interval": self.slide_interval,
            "max_size": self.max_size,
            "events": [
                {
                    "event_id": event.event_id,
                    "timestamp": event.timestamp,
                    "value": event.value,
                    "metric": event.metric,
                    "metadata": dict(event.metadata),
                }
                for event in self._buffer
            ],
        }

    def load_state(self, state: dict[str, Any]) -> None:
        """Rehydrate the window from a previously-serialized state dict.

        Clears any existing buffer/stats and replays each stored event
        through :meth:`add` so that :class:`IncrementalStats` and
        :class:`MonotonicMinMax` are rebuilt fresh. Events whose
        timestamps have since expired (older than ``window_size`` from
        now) are still replayed and will then be lazy-expired on the
        next :meth:`snapshot` — the buffer may briefly contain stale
        entries until the next snapshot runs.
        """
        self._buffer.clear()
        self._stats = IncrementalStats()
        self._minmax = MonotonicMinMax()

        for raw in state.get("events", []):
            event = Event(
                event_id=str(raw.get("event_id", "")),
                timestamp=float(raw.get("timestamp", 0.0)),
                value=float(raw.get("value", 0.0)),
                metric=str(raw.get("metric", "")),
                metadata=dict(raw.get("metadata", {})),
            )
            self.add(event)
